package qsruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) createTemporaryTableRuntimeResult(ctx context.Context, request qsbridge.ExecutionRequest) ExecutionResult {
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
	sourceSQL := strings.TrimSpace(request.Bound.Prepared.Query.Mutation.SourceSQL)
	var rows []qsbridge.TemporaryTableRow
	if sourceSQL != "" {
		var sourceDiagnostics qsbridge.DiagnosticSet
		rows, sourceDiagnostics = r.createTemporaryTableAsSelectRows(ctx, table, sourceSQL, request.Options)
		if sourceDiagnostics.BlocksNative() {
			return ExecutionResult{
				Diagnostics: sourceDiagnostics,
				Statement:   qsbridge.StatementResult{Status: "CREATE TEMPORARY TABLE failed"},
			}
		}
	}
	actions := []qsbridge.SessionAction{{
		Kind:  qsbridge.SessionActionCreateTemporaryTable,
		Name:  table.Name,
		Value: table.Schema,
		Table: table,
	}}
	if len(rows) > 0 {
		actions = append(actions, qsbridge.SessionAction{
			Kind:  qsbridge.SessionActionInsertTemporaryRows,
			Name:  table.Name,
			Value: table.Schema,
			Rows:  rows,
		})
	}
	status := "Temporary table " + table.Name + " created"
	if sourceSQL != "" {
		status = fmt.Sprintf("Temporary table %s created from SELECT", table.Name)
	}
	return ExecutionResult{
		Count: uint64(len(rows)),
		Statement: qsbridge.StatementResult{
			AffectedRows:   uint64(len(rows)),
			Status:         status,
			SessionActions: actions,
		},
	}
}

func (r SQLRuntime) createTemporaryTableAsSelectRows(ctx context.Context, table qsbridge.TableDefinition, sourceSQL string, options qsbridge.ExecutionOptions) ([]qsbridge.TemporaryTableRow, qsbridge.DiagnosticSet) {
	sourceResult, err := r.ExecuteSQL(ctx, sourceSQL, options)
	if err != nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TEMPORARY TABLE AS SELECT failed: "+err.Error()),
		}
	}
	if sourceResult.Diagnostics.BlocksNative() || sourceResult.Runtime.Diagnostics.BlocksNative() {
		diagnostics := append(qsbridge.DiagnosticSet(nil), sourceResult.Diagnostics...)
		diagnostics = append(diagnostics, sourceResult.Runtime.Diagnostics...)
		return nil, diagnostics
	}
	if len(sourceResult.Request.ResultColumns) != len(table.Fields) {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TEMPORARY TABLE AS SELECT result shape changed during execution"),
		}
	}
	chunk, diagnostics := sourceResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]qsbridge.TemporaryTableRow, 0, len(chunk.Rows))
	for rowIndex, row := range chunk.Rows {
		if len(row) != len(table.Fields) {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("CREATE TEMPORARY TABLE AS SELECT row %d has %d values for %d columns", rowIndex+1, len(row), len(table.Fields))),
			}
		}
		values := make(qsbridge.ResultRow, len(row))
		for columnIndex, cell := range row {
			coerced, cellDiagnostics := temporaryTableCoerceCell(cell, table.Fields[columnIndex])
			if cellDiagnostics.BlocksNative() {
				return nil, cellDiagnostics
			}
			values[columnIndex] = coerced
		}
		rows = append(rows, qsbridge.TemporaryTableRow{Values: values})
	}
	return rows, nil
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
