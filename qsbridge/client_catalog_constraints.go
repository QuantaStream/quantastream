package qsbridge

import "sort"

// ClientCatalogConstraintType identifies a table constraint kind.
type ClientCatalogConstraintType string

const (
	// ClientCatalogConstraintPrimaryKey identifies primary-key metadata.
	ClientCatalogConstraintPrimaryKey ClientCatalogConstraintType = "PRIMARY KEY"
	// ClientCatalogConstraintForeignKey identifies relationship-backed foreign-key metadata.
	ClientCatalogConstraintForeignKey ClientCatalogConstraintType = "FOREIGN KEY"
)

// ClientCatalogConstraint describes one adapter-visible table constraint column.
type ClientCatalogConstraint struct {
	Schema           string
	Table            string
	ConstraintName   string
	ConstraintType   ClientCatalogConstraintType
	ColumnName       string
	OrdinalPosition  int
	ReferencedSchema string
	ReferencedTable  string
	ReferencedColumn string
}

// ClientCatalogConstraintExchange is client-facing constraint metadata for one table.
type ClientCatalogConstraintExchange struct {
	Connection   ConnectionContext
	Schema       string
	Table        string
	Pattern      string
	Constraints  []ClientCatalogConstraint
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogConstraints returns primary-key and relationship-backed constraint metadata.
func (s PlanningService) ListClientCatalogConstraints(connection ConnectionContext, catalog Catalog, schema string, table string, pattern string) ClientCatalogConstraintExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogConstraintExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Table:       table,
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogConstraintResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "constraint metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogConstraintResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "constraint metadata requires a table name"),
		})
		exchange.Result = exchange.catalogConstraintResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	definition, diagnostics := catalog.Table(schema, table)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Constraints = filterClientCatalogConstraints(tableCatalogConstraints(definition), pattern)
	}
	exchange.Result = exchange.catalogConstraintResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether constraint metadata can be returned.
func (e ClientCatalogConstraintExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts constraint diagnostics into protocol-facing errors.
func (e ClientCatalogConstraintExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking constraint error, if any.
func (e ClientCatalogConstraintExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogConstraintExchange) catalogConstraintResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogConstraintResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogConstraintRows(),
		Final: true,
	})
}

func catalogConstraintResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Constraint_schema", Type: DataTypeString},
		{Name: "Table_name", Type: DataTypeString},
		{Name: "Constraint_name", Type: DataTypeString},
		{Name: "Constraint_type", Type: DataTypeString},
		{Name: "Column_name", Type: DataTypeString},
		{Name: "Ordinal_position", Type: DataTypeInt},
		{Name: "Referenced_table_schema", Type: DataTypeString, Nullable: true},
		{Name: "Referenced_table_name", Type: DataTypeString, Nullable: true},
		{Name: "Referenced_column_name", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCatalogConstraintExchange) catalogConstraintRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Constraints))
	for _, constraint := range e.Constraints {
		rows = append(rows, ResultRow{
			metadataStringCell(constraint.Schema),
			metadataStringCell(constraint.Table),
			metadataStringCell(constraint.ConstraintName),
			metadataStringCell(string(constraint.ConstraintType)),
			metadataStringCell(constraint.ColumnName),
			metadataIntCell(constraint.OrdinalPosition),
			metadataStringCell(constraint.ReferencedSchema),
			metadataStringCell(constraint.ReferencedTable),
			metadataStringCell(constraint.ReferencedColumn),
		})
	}
	return rows
}

func tableCatalogConstraints(table TableDefinition) []ClientCatalogConstraint {
	constraints := make([]ClientCatalogConstraint, 0)
	primarySeq := 1
	for _, field := range table.Fields {
		if !field.PrimaryKey {
			continue
		}
		constraints = append(constraints, ClientCatalogConstraint{
			Schema:          table.Schema,
			Table:           table.Name,
			ConstraintName:  "PRIMARY",
			ConstraintType:  ClientCatalogConstraintPrimaryKey,
			ColumnName:      field.Name,
			OrdinalPosition: primarySeq,
		})
		primarySeq++
	}
	for _, relationship := range table.Relationships {
		if relationship.FromField == "" {
			continue
		}
		constraints = append(constraints, ClientCatalogConstraint{
			Schema:           table.Schema,
			Table:            table.Name,
			ConstraintName:   relationship.Name,
			ConstraintType:   ClientCatalogConstraintForeignKey,
			ColumnName:       relationship.FromField,
			OrdinalPosition:  1,
			ReferencedSchema: table.Schema,
			ReferencedTable:  relationship.ToTable,
			ReferencedColumn: relationship.ToField,
		})
	}
	sort.SliceStable(constraints, func(i, j int) bool {
		if constraints[i].ConstraintName != constraints[j].ConstraintName {
			return constraints[i].ConstraintName < constraints[j].ConstraintName
		}
		if constraints[i].OrdinalPosition != constraints[j].OrdinalPosition {
			return constraints[i].OrdinalPosition < constraints[j].OrdinalPosition
		}
		return constraints[i].ColumnName < constraints[j].ColumnName
	})
	return constraints
}

func filterClientCatalogConstraints(constraints []ClientCatalogConstraint, pattern string) []ClientCatalogConstraint {
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloneClientCatalogConstraints(constraints)
	}
	filtered := make([]ClientCatalogConstraint, 0, len(constraints))
	for _, constraint := range constraints {
		if catalogFieldPatternMatch(pattern, constraint.ConstraintName) || catalogFieldPatternMatch(pattern, constraint.ColumnName) {
			filtered = append(filtered, constraint)
		}
	}
	return filtered
}

func cloneClientCatalogConstraints(constraints []ClientCatalogConstraint) []ClientCatalogConstraint {
	if len(constraints) == 0 {
		return nil
	}
	return append([]ClientCatalogConstraint(nil), constraints...)
}
