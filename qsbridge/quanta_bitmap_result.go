package qsbridge

// QuantaBitmapQueryResult is the native bitmap-query result shape.
//
// It carries candidate rownums and count/error metadata produced by bitmap
// predicate evaluation before later runtime layers adapt it into projected
// rows, statements, or protocol-visible results.
type QuantaBitmapQueryResult struct {
	Success      bool
	ErrorMessage string
	Count        uint64
	Rownums      []QuantaRownum
}

// CandidateCount reports how many row candidates are materialized in the result.
func (r QuantaBitmapQueryResult) CandidateCount() int {
	return len(r.Rownums)
}

// Clone returns a copy that does not alias rownum slices.
func (r QuantaBitmapQueryResult) Clone() QuantaBitmapQueryResult {
	r.Rownums = append([]QuantaRownum(nil), r.Rownums...)
	return r
}
