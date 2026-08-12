package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// BitmapQueryResult is qsbridge's native bitmap-query result shape.
type BitmapQueryResult = qsbridge.QuantaBitmapQueryResult

// BitmapQueryResultAdapter maps bitmap-query results to qsruntime execution results.
type BitmapQueryResultAdapter struct{}

// ToExecutionResult converts a bitmap query result into a runtime execution result.
func (a BitmapQueryResultAdapter) ToExecutionResult(result BitmapQueryResult) ExecutionResult {
	rownums := append([]qsbridge.QuantaRownum(nil), result.Rownums...)
	diagnostics := qsbridge.DiagnosticSet(nil)
	if !result.Success && result.ErrorMessage != "" {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			result.ErrorMessage,
		))
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: rownums,
		},
		Diagnostics: diagnostics,
		Count:       result.Count,
		Probes:      append([]qsbridge.ProjectionProbe(nil), result.Probes...),
	}
}
