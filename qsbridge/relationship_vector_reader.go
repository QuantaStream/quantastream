package qsbridge

import "context"

// RelationshipVectorReader reads relationship-vector mappings for filter-domain normalization.
type RelationshipVectorReader interface {
	ReadRelatedCandidates(context.Context, FilterDomainRelationshipVectorRequest) (QuantaCandidateSet, DiagnosticSet, error)
}

// RelationshipVectorResultReader reads relationship-vector candidates with optional timing metadata.
type RelationshipVectorResultReader interface {
	ReadRelatedCandidateResult(context.Context, FilterDomainRelationshipVectorRequest) (FilterDomainRelationshipVectorResult, DiagnosticSet, error)
}

// InMemoryRelationshipVectorIndex is a deterministic test/vector reader.
type InMemoryRelationshipVectorIndex struct {
	Vectors map[string]map[QuantaRownum][]QuantaRownum
}

// ReadRelatedCandidates maps source rownums to target rownums in source-candidate order.
func (i InMemoryRelationshipVectorIndex) ReadRelatedCandidates(_ context.Context, request FilterDomainRelationshipVectorRequest) (QuantaCandidateSet, DiagnosticSet, error) {
	vector, ok := i.Vectors[request.LeafName()]
	if !ok {
		return QuantaCandidateSet{}, DiagnosticSet{
			ErrorDiagnostic(
				DiagnosticUnsupportedSQL,
				PhaseExecute,
				"relationship-vector reader has no vector mapping for "+request.LeafName(),
			),
		}, nil
	}
	targetRows := make([]QuantaRownum, 0, len(request.SourceCandidates.Rownums))
	seen := map[QuantaRownum]bool{}
	for _, source := range request.SourceCandidates.Rownums {
		for _, target := range vector[source] {
			if seen[target] {
				continue
			}
			seen[target] = true
			targetRows = append(targetRows, target)
		}
	}
	return QuantaCandidateSet{
		Index:   request.TargetDomain,
		Rownums: targetRows,
	}, nil, nil
}
