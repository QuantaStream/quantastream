package qsruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) createDurableTableAsSelectRuntimeResult(ctx context.Context, request qsbridge.ExecutionRequest) ExecutionResult {
	mutation := cloneMutationShape(request.Bound.Prepared.Query.Mutation)
	sourceSQL := strings.TrimSpace(mutation.SourceSQL)
	if sourceSQL == "" {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT requires a SELECT statement"),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	if result, blocked := r.rejectStorageMutation(ctx, "create_table_as_select", "CREATE TABLE AS SELECT failed"); blocked {
		return result
	}
	sourceResult, err := r.ExecuteSQL(ctx, sourceSQL, request.Options)
	if err != nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT failed: "+err.Error()),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	sourceDiagnostics := append(qsbridge.DiagnosticSet(nil), sourceResult.Diagnostics...)
	sourceDiagnostics = append(sourceDiagnostics, sourceResult.Runtime.Diagnostics...)
	if sourceDiagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: sourceDiagnostics,
			Statement:   qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	if len(sourceResult.Request.ResultColumns) != len(mutation.Columns) {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT result shape changed during execution"),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	rowSet := temporaryTableOrderVisibleProjectedRowSetByResultColumns(sourceResult.Runtime.RowSet, sourceResult.Request.ResultColumns)
	chunk, chunkDiagnostics := rowSet.ToResultChunk(0, true)
	if chunkDiagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: chunkDiagnostics,
			Statement:   qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	mutation.Columns = createTableAsSelectColumnsWithRuntimeDefaults(mutation.Columns)
	rows, rowDiagnostics := createTableAsSelectMutationRows(mutation.Columns, chunk.Rows)
	if rowDiagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: rowDiagnostics,
			Statement:   qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	createRequest := request
	createRequest.Bound.Prepared.Query.Mutation = mutation
	createIntermediate, lowerDiagnostics := r.Lowerer.LowerExecutionRequest(createRequest)
	if lowerDiagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: lowerDiagnostics,
			Statement:   qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	createRuntimeRequest := NewSQLExecutionRequest(createIntermediate, createRequest)
	createResult, err := r.ExecutePrepared(ctx, createRuntimeRequest)
	if err != nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT failed: "+err.Error()),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	if createResult.Diagnostics.BlocksNative() {
		createResult.Statement.Status = "CREATE TABLE AS SELECT failed"
		return createResult
	}
	if len(rows) == 0 {
		createResult.Count = 0
		createResult.Statement.AffectedRows = 0
		createResult.Statement.Status = fmt.Sprintf("Table %s created from SELECT", createTableAsSelectTargetName(mutation))
		return createResult
	}
	insertRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	insertRequest.Options = request.Options
	insertRequest.Result = qsbridge.ResultShape{Kind: qsbridge.ResultStatement}
	insertRequest.Statement = qsbridge.StatementResult{Status: fmt.Sprintf("Records: %d", len(rows))}
	insertRequest.Mutation = qsbridge.MutationShape{
		Kind:    qsbridge.MutationInsert,
		Target:  mutation.Target,
		Columns: mutation.Columns,
		Rows:    rows,
	}
	insertResult, err := r.ExecutePrepared(ctx, insertRequest)
	if err != nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT failed: "+err.Error()),
			},
			Statement: qsbridge.StatementResult{Status: "CREATE TABLE AS SELECT failed"},
		}
	}
	if insertResult.Diagnostics.BlocksNative() {
		insertResult.Statement.Status = "CREATE TABLE AS SELECT failed"
		return insertResult
	}
	insertResult.Count = uint64(len(rows))
	insertResult.Statement.AffectedRows = uint64(len(rows))
	insertResult.Statement.Status = fmt.Sprintf("Table %s created from SELECT", createTableAsSelectTargetName(mutation))
	return insertResult
}

func createTableAsSelectColumnsWithRuntimeDefaults(columns []qsbridge.FieldRef) []qsbridge.FieldRef {
	adjusted := append([]qsbridge.FieldRef(nil), columns...)
	for i := range adjusted {
		if adjusted[i].Type != qsbridge.DataTypeFloat {
			continue
		}
		if adjusted[i].Encoding.Kind == "" && adjusted[i].Encoding.LegacyName == "" {
			adjusted[i].Encoding = qsbridge.LegacyEncodingProfile("FloatScaleBSI", qsbridge.LegacyEncodingOptions{Scale: 6})
			continue
		}
		if adjusted[i].Encoding.Scale <= 0 {
			adjusted[i].Encoding.Scale = 6
		}
	}
	return adjusted
}

func createTableAsSelectMutationRows(columns []qsbridge.FieldRef, resultRows []qsbridge.ResultRow) ([]qsbridge.MutationRow, qsbridge.DiagnosticSet) {
	rows := make([]qsbridge.MutationRow, 0, len(resultRows))
	for rowIndex, resultRow := range resultRows {
		if len(resultRow) != len(columns) {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("CREATE TABLE AS SELECT row %d has %d values for %d columns", rowIndex+1, len(resultRow), len(columns))),
			}
		}
		values := make([]qsbridge.Expr, len(resultRow))
		for columnIndex, cell := range resultRow {
			kind := cell.Kind
			if kind == qsbridge.ValueUnknown {
				kind = createTableAsSelectLiteralKind(columns[columnIndex].Type)
			}
			if cell.Value == nil {
				kind = qsbridge.ValueNull
			}
			values[columnIndex] = qsbridge.Literal(kind, cell.Value)
		}
		rows = append(rows, qsbridge.MutationRow{Values: values})
	}
	return rows, nil
}

func createTableAsSelectLiteralKind(dataType qsbridge.DataType) qsbridge.ValueKind {
	switch dataType {
	case qsbridge.DataTypeBool:
		return qsbridge.ValueBool
	case qsbridge.DataTypeFloat:
		return qsbridge.ValueFloat
	case qsbridge.DataTypeInt:
		return qsbridge.ValueInt
	case qsbridge.DataTypeTime:
		return qsbridge.ValueTime
	case qsbridge.DataTypeString:
		return qsbridge.ValueString
	default:
		return qsbridge.ValueString
	}
}

func createTableAsSelectTargetName(mutation qsbridge.MutationShape) string {
	if name := strings.TrimSpace(mutation.Target.Table); name != "" {
		return name
	}
	return strings.TrimSpace(string(mutation.Target.ID))
}
