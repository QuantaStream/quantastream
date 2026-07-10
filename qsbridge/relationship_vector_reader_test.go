package qsbridge

import (
	"context"
	"reflect"
	"testing"
)

func TestInMemoryRelationshipVectorIndexReadsCandidatesWithDedupe(t *testing.T) {
	reader := InMemoryRelationshipVectorIndex{Vectors: map[string]map[QuantaRownum][]QuantaRownum{
		"part.p_brand": {
			7: {2, 4},
			8: {4, 6},
		},
	}}
	request := testRelationshipVectorReaderRequest(
		"part",
		"lineitem",
		FilterDomainRelationshipVectorDirectionRightToLeft,
		[]QuantaRownum{7, 8},
	)

	candidates, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), request)
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if candidates.Index != "lineitem" {
		t.Fatalf("index = %q, want lineitem", candidates.Index)
	}
	want := []QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(candidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", candidates.Rownums, want)
	}
}

func TestInMemoryRelationshipVectorIndexReportsMissingVector(t *testing.T) {
	reader := InMemoryRelationshipVectorIndex{}
	request := testRelationshipVectorReaderRequest(
		"part",
		"lineitem",
		FilterDomainRelationshipVectorDirectionRightToLeft,
		[]QuantaRownum{7},
	)

	_, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), request)
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want missing vector diagnostic", diagnostics)
	}
}

func testRelationshipVectorReaderRequest(source string, target string, direction FilterDomainRelationshipVectorDirection, sourceRows []QuantaRownum) FilterDomainRelationshipVectorRequest {
	return FilterDomainRelationshipVectorRequest{
		SourceFragment:   QuantaQueryFragment{Index: source, Field: "p_brand"},
		SourceCandidates: QuantaCandidateSet{Index: source, Rownums: sourceRows},
		SourceDomain:     source,
		TargetDomain:     target,
		Direction:        direction,
	}
}
