package qsruntime

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) createTemporaryTableRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	table, diagnostics := r.temporaryTableDefinition(request)
	if diagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: diagnostics,
			Statement:   qsbridge.StatementResult{Status: "CREATE TEMPORARY TABLE failed"},
		}
	}
	if _, exists := r.Session.TemporaryTables[temporaryTableSessionKey(table.Schema, table.Name)]; exists {
		if request.Bound.Prepared.Query.Mutation.IfNotExists {
			return ExecutionResult{
				Statement: qsbridge.StatementResult{Status: "Temporary table " + table.Name + " already exists"},
			}
		}
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "temporary table already exists: "+qualifiedRuntimeTableName(table.Schema, table.Name)),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TEMPORARY TABLE failed"},
		}
	}
	return ExecutionResult{
		Statement: qsbridge.StatementResult{
			Status: "Temporary table " + table.Name + " created",
			SessionActions: []qsbridge.SessionAction{{
				Kind:  qsbridge.SessionActionCreateTemporaryTable,
				Name:  table.Name,
				Value: table.Schema,
				Table: table,
			}},
		},
	}
}

func (r SQLRuntime) dropTemporaryTableRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	mutation := request.Bound.Prepared.Query.Mutation
	target := mutation.Target
	schemaName := strings.TrimSpace(target.Schema)
	if schemaName == "" {
		schemaName = r.Session.EffectiveSchema(r.DefaultSchema)
	}
	tableName := strings.TrimSpace(target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(target.ID))
	}
	if tableName == "" {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "DROP TEMPORARY TABLE target is empty"),
			},
			Statement: qsbridge.StatementResult{Status: "DROP TEMPORARY TABLE failed"},
		}
	}
	if _, exists := r.Session.TemporaryTables[temporaryTableSessionKey(schemaName, tableName)]; !exists {
		if mutation.IfExists {
			return ExecutionResult{
				Statement: qsbridge.StatementResult{Status: "Temporary table " + tableName + " did not exist"},
			}
		}
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogTableNotFound, qsbridge.PhaseExecute, "temporary table not found: "+qualifiedRuntimeTableName(schemaName, tableName)),
			},
			Statement: qsbridge.StatementResult{Status: "DROP TEMPORARY TABLE failed"},
		}
	}
	return ExecutionResult{
		Statement: qsbridge.StatementResult{
			Status: "Temporary table " + tableName + " dropped",
			SessionActions: []qsbridge.SessionAction{{
				Kind:  qsbridge.SessionActionDropTemporaryTable,
				Name:  tableName,
				Value: schemaName,
			}},
		},
	}
}

func (r SQLRuntime) temporaryTableDefinition(request qsbridge.ExecutionRequest) (qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	mutation := request.Bound.Prepared.Query.Mutation
	target := mutation.Target
	schemaName := strings.TrimSpace(target.Schema)
	if schemaName == "" {
		schemaName = r.Session.EffectiveSchema(r.DefaultSchema)
	}
	tableName := strings.TrimSpace(target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(target.ID))
	}
	if tableName == "" {
		return qsbridge.TableDefinition{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TEMPORARY TABLE target is empty"),
		}
	}
	if len(mutation.Columns) == 0 {
		return qsbridge.TableDefinition{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TEMPORARY TABLE requires at least one column"),
		}
	}
	fields := make([]qsbridge.FieldDefinition, 0, len(mutation.Columns))
	for _, column := range mutation.Columns {
		fieldName := strings.TrimSpace(column.Name)
		if fieldName == "" {
			return qsbridge.TableDefinition{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TEMPORARY TABLE column name is empty"),
			}
		}
		fields = append(fields, qsbridge.FieldDefinition{
			Name:         fieldName,
			PhysicalName: strings.TrimSpace(column.PhysicalName),
			Type:         column.Type,
			Index:        column.Index,
			Nullable:     column.Nullable,
			PrimaryKey:   column.PrimaryKey,
			Encoding:     column.Encoding,
			Dictionary:   column.Dictionary,
		})
	}
	return qsbridge.TableDefinition{
		Schema: schemaName,
		Name:   tableName,
		Fields: fields,
	}, nil
}

func temporaryTableSessionKey(schema string, name string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
}

func qualifiedRuntimeTableName(schema string, name string) string {
	if strings.TrimSpace(schema) == "" {
		return name
	}
	return strings.TrimSpace(schema) + "." + name
}
