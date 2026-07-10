package qsfixture

import "github.com/QuantaStream/quantastream/qsbridge"

// NewRelationshipVectorIndex creates a deterministic relationship-vector fixture reader.
func NewRelationshipVectorIndex(vectors map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum) qsbridge.InMemoryRelationshipVectorIndex {
	return qsbridge.InMemoryRelationshipVectorIndex{Vectors: cloneRelationshipVectorMap(vectors)}
}

func cloneRelationshipVectorMap(vectors map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum) map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum {
	cloned := make(map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum, len(vectors))
	for name, vector := range vectors {
		cloned[name] = make(map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum, len(vector))
		for source, targets := range vector {
			cloned[name][source] = append([]qsbridge.QuantaRownum(nil), targets...)
		}
	}
	return cloned
}
