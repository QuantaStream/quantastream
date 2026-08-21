package qsruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) insertSelectRuntimeResult(ctx context.Context, request qsbridge.ExecutionRequest) ExecutionResult {
	mutation := cloneMutationShape(request.Bound.Prepared.Query.Mutation)
	sourceSQL := strings.TrimSpace(mutation.SourceSQL)
	if sourceSQL == "" {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "INSERT SELECT requires a SELECT statement"),
			},
			Statement: qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	columns, diagnostics := r.insertSelectTargetColumns(mutation)
	if diagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: diagnostics,
			Statement:   qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	sourceResult, err := r.ExecuteSQL(ctx, sourceSQL, request.Options)
	if err != nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "INSERT SELECT source failed: "+err.Error()),
			},
			Statement: qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	sourceDiagnostics := append(qsbridge.DiagnosticSet(nil), sourceResult.Diagnostics...)
	sourceDiagnostics = append(sourceDiagnostics, sourceResult.Runtime.Diagnostics...)
	if sourceDiagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: sourceDiagnostics,
			Statement:   qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	if len(sourceResult.Request.ResultColumns) != len(columns) {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "INSERT SELECT result shape changed during execution"),
			},
			Statement: qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	rowSet := sourceResult.Runtime.RowSet
	if sourceOrder := projectionOrder(sourceResult.Request.Bound.Prepared.Query.Projection); len(sourceOrder) > 0 {
		rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, sourceOrder)
	}
	rowSet = temporaryTableOrderVisibleProjectedRowSetByResultColumns(rowSet, sourceResult.Request.ResultColumns)
	chunk, diagnostics := rowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: diagnostics,
			Statement:   qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	rows, diagnostics := createTableAsSelectMutationRows(columns, chunk.Rows)
	if diagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: diagnostics,
			Statement:   qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	mutation.SourceSQL = ""
	mutation.Columns = columns
	mutation.Rows = rows
	insertRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	insertRequest.Options = request.Options
	insertRequest.Result = qsbridge.ResultShape{Kind: qsbridge.ResultStatement}
	insertRequest.Statement = qsbridge.StatementResult{Status: fmt.Sprintf("Records: %d", len(rows))}
	insertRequest.Mutation = mutation
	insertResult, err := r.ExecutePrepared(ctx, insertRequest)
	if err != nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "INSERT SELECT failed: "+err.Error()),
			},
			Statement: qsbridge.StatementResult{Status: "INSERT SELECT failed"},
		}
	}
	if insertResult.Diagnostics.BlocksNative() {
		insertResult.Statement.Status = "INSERT SELECT failed"
		return insertResult
	}
	insertResult.Count = uint64(len(rows))
	insertResult.Statement.AffectedRows = uint64(len(rows))
	insertResult.Statement.Status = fmt.Sprintf("Records: %d", len(rows))
	return insertResult
}

func (r SQLRuntime) insertSelectTargetColumns(mutation qsbridge.MutationShape) ([]qsbridge.FieldRef, qsbridge.DiagnosticSet) {
	if len(mutation.Columns) > 0 {
		return insertSelectCloneFieldRefs(mutation.Columns), nil
	}
	tableName := strings.TrimSpace(mutation.Target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(mutation.Target.ID))
	}
	schemaName := strings.TrimSpace(mutation.Target.Schema)
	if schemaName == "" {
		schemaName = r.Session.EffectiveSchema(r.DefaultSchema)
	}
	table, diagnostics := r.planningCatalog().Table(schemaName, tableName)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	target := mutation.Target
	if strings.TrimSpace(target.Schema) == "" {
		target.Schema = schemaName
	}
	if strings.TrimSpace(target.Table) == "" {
		target.Table = tableName
	}
	columns := make([]qsbridge.FieldRef, 0, len(table.Fields))
	for _, field := range table.Fields {
		columns = append(columns, field.Ref(target, qsbridge.FieldRoleMutationTarget))
	}
	return columns, nil
}

func insertSelectCloneFieldRefs(fields []qsbridge.FieldRef) []qsbridge.FieldRef {
	if len(fields) == 0 {
		return nil
	}
	cloned := make([]qsbridge.FieldRef, len(fields))
	copy(cloned, fields)
	return cloned
}
