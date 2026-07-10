package qsbridge

// QuantaExecutionResult is the native bitmap/runtime execution result envelope.
//
// This is lower-level than the client-facing ExecutionResult in result.go. It
// carries projected rowsets, diagnostics, probes, counts, and statement metadata
// between native executor kernels and runtime adapters without implying any
// particular wire protocol or SQL client response shape.
type QuantaExecutionResult struct {
	RowSet      QuantaProjectedRowSet
	Diagnostics DiagnosticSet
	Probes      []ProjectionProbe
	Count       uint64
	Statement   StatementResult
}

// CandidateCount reports how many row candidates are present in the result row set.
func (r QuantaExecutionResult) CandidateCount() int {
	return r.RowSet.CandidateCount()
}
