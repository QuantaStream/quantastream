package qsbridge

import "sort"

// ClientCatalogIndex describes one adapter-visible key/index entry.
type ClientCatalogIndex struct {
	Schema           string
	Table            string
	KeyName          string
	ColumnName       string
	Sequence         int
	Unique           bool
	IndexType        string
	RelationshipName string
	ReferencedTable  string
	ReferencedColumn string
}

// ClientCatalogIndexExchange is client-facing key/index metadata for one table.
type ClientCatalogIndexExchange struct {
	Connection   ConnectionContext
	Schema       string
	Table        string
	Indexes      []ClientCatalogIndex
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogIndexes returns key/index metadata for adapters.
func (s PlanningService) ListClientCatalogIndexes(connection ConnectionContext, catalog Catalog, schema string, table string) ClientCatalogIndexExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogIndexExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Table:       table,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogIndexResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "index metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogIndexResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "index metadata requires a table name"),
		})
		exchange.Result = exchange.catalogIndexResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	definition, diagnostics := catalog.Table(schema, table)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Indexes = tableCatalogIndexes(definition)
	}
	exchange.Result = exchange.catalogIndexResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether index metadata can be returned.
func (e ClientCatalogIndexExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts index metadata diagnostics into protocol-facing errors.
func (e ClientCatalogIndexExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking index metadata error, if any.
func (e ClientCatalogIndexExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogIndexExchange) catalogIndexResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogIndexResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogIndexRows(),
		Final: true,
	})
}

func catalogIndexResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Table", Type: DataTypeString},
		{Name: "Key_name", Type: DataTypeString},
		{Name: "Seq_in_index", Type: DataTypeInt},
		{Name: "Column_name", Type: DataTypeString},
		{Name: "Non_unique", Type: DataTypeBool},
		{Name: "Index_type", Type: DataTypeString, Nullable: true},
		{Name: "Relationship", Type: DataTypeString, Nullable: true},
		{Name: "Referenced_table", Type: DataTypeString, Nullable: true},
		{Name: "Referenced_column", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCatalogIndexExchange) catalogIndexRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Indexes))
	for _, index := range e.Indexes {
		rows = append(rows, ResultRow{
			metadataStringCell(index.Schema),
			metadataStringCell(index.Table),
			metadataStringCell(index.KeyName),
			metadataIntCell(index.Sequence),
			metadataStringCell(index.ColumnName),
			metadataBoolCell(!index.Unique),
			metadataStringCell(index.IndexType),
			metadataStringCell(index.RelationshipName),
			metadataStringCell(index.ReferencedTable),
			metadataStringCell(index.ReferencedColumn),
		})
	}
	return rows
}

func tableCatalogIndexes(table TableDefinition) []ClientCatalogIndex {
	indexes := make([]ClientCatalogIndex, 0)
	primarySeq := 1
	for _, field := range table.Fields {
		if field.PrimaryKey {
			indexes = append(indexes, ClientCatalogIndex{
				Schema:     table.Schema,
				Table:      table.Name,
				KeyName:    "PRIMARY",
				ColumnName: field.Name,
				Sequence:   primarySeq,
				Unique:     true,
				IndexType:  string(field.Index),
			})
			primarySeq++
			continue
		}
		if field.Index != "" {
			indexes = append(indexes, ClientCatalogIndex{
				Schema:     table.Schema,
				Table:      table.Name,
				KeyName:    "idx_" + field.Name,
				ColumnName: field.Name,
				Sequence:   1,
				IndexType:  string(field.Index),
			})
		}
	}
	for _, relationship := range table.Relationships {
		if relationship.FromField == "" {
			continue
		}
		indexes = append(indexes, ClientCatalogIndex{
			Schema:           table.Schema,
			Table:            table.Name,
			KeyName:          relationship.Name,
			ColumnName:       relationship.FromField,
			Sequence:         1,
			IndexType:        string(relationship.Encoding.Kind),
			RelationshipName: relationship.Name,
			ReferencedTable:  relationship.ToTable,
			ReferencedColumn: relationship.ToField,
		})
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		if indexes[i].KeyName != indexes[j].KeyName {
			return indexes[i].KeyName < indexes[j].KeyName
		}
		if indexes[i].Sequence != indexes[j].Sequence {
			return indexes[i].Sequence < indexes[j].Sequence
		}
		return indexes[i].ColumnName < indexes[j].ColumnName
	})
	return indexes
}

func metadataIntCell(value int) ResultCell {
	return ResultCell{Kind: ValueInt, Value: value}
}
