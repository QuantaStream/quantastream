package qsfixture

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestNewRelationshipVectorIndexCopiesFixtureVectors(t *testing.T) {
	vectors := map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
		"part.p_brand": {
			7: {2, 4},
		},
	}
	reader := NewRelationshipVectorIndex(vectors)
	vectors["part.p_brand"][7][0] = 99

	candidates, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), qsbridge.FilterDomainRelationshipVectorRequest{
		SourceFragment:   qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"},
		SourceCandidates: qsbridge.QuantaCandidateSet{Index: "part", Rownums: []qsbridge.QuantaRownum{7}},
		SourceDomain:     "part",
		TargetDomain:     "lineitem",
		Direction:        qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
	})
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !reflect.DeepEqual(candidates.Rownums, []qsbridge.QuantaRownum{2, 4}) {
		t.Fatalf("rownums = %#v, want copied fixture rownums", candidates.Rownums)
	}
}
