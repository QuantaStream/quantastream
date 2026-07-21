package qsruntime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestInMemoryRelationshipVectorIndexReadsRightToLeftWithDedupe(t *testing.T) {
	reader := InMemoryRelationshipVectorIndex{Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
		"part.p_brand": {
			7: {2, 4},
			8: {4, 6},
		},
	}}
	request := testFilterDomainVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7, 8},
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
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(candidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", candidates.Rownums, want)
	}
}

func TestInMemoryRelationshipVectorIndexReadsLeftToRight(t *testing.T) {
	reader := InMemoryRelationshipVectorIndex{Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
		"lineitem.l_partkey": {
			2: {7},
			4: {7, 8},
		},
	}}
	request := testFilterDomainVectorRequest(
		"lineitem",
		"part",
		qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
		[]qsbridge.QuantaRownum{2, 4},
	)
	request.SourceFragment = qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_partkey"}

	candidates, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), request)
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if candidates.Index != "part" {
		t.Fatalf("index = %q, want part", candidates.Index)
	}
	want := []qsbridge.QuantaRownum{7, 8}
	if !reflect.DeepEqual(candidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", candidates.Rownums, want)
	}
}

func TestInMemoryRelationshipVectorIndexAllowsEmptySource(t *testing.T) {
	reader := InMemoryRelationshipVectorIndex{Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
		"part.p_brand": {
			7: {2},
		},
	}}
	request := testFilterDomainVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		nil,
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
	if len(candidates.Rownums) != 0 {
		t.Fatalf("rownums = %#v, want empty", candidates.Rownums)
	}
}

func TestLegacyDirectRelationshipVectorReaderRecordsRequestShape(t *testing.T) {
	reader := &LegacyDirectRelationshipVectorReader{}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7, 8},
	)

	candidates, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), request)
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported boundary", diagnostics)
	}
	if !strings.Contains(diagnostics[0].Message, "relationship-vector reader is not wired yet") {
		t.Fatalf("diagnostic message = %q, want relationship-vector boundary", diagnostics[0].Message)
	}
	if !strings.Contains(diagnostics[0].Message, "vector=lineitem.l_partkey") {
		t.Fatalf("diagnostic message = %q, want vector field", diagnostics[0].Message)
	}
	if candidates.Index != "" || len(candidates.Rownums) != 0 {
		t.Fatalf("candidates = %#v, want empty unwired result", candidates)
	}
	if reader.LastRequest.SourceDomain != "part" || reader.LastRequest.TargetDomain != "lineitem" {
		t.Fatalf("recorded domains = %s -> %s, want part -> lineitem", reader.LastRequest.SourceDomain, reader.LastRequest.TargetDomain)
	}
	if reader.LastRequest.Direction != qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft {
		t.Fatalf("recorded direction = %q, want right-to-left", reader.LastRequest.Direction)
	}
	if !reflect.DeepEqual(reader.LastRequest.SourceCandidates.Rownums, []qsbridge.QuantaRownum{7, 8}) {
		t.Fatalf("recorded source candidates = %#v, want [7 8]", reader.LastRequest.SourceCandidates.Rownums)
	}
	if reader.LastRequest.Edge.Left.Table.Table != "lineitem" || reader.LastRequest.Edge.Right.Table.Table != "part" {
		t.Fatalf("recorded edge = %#v, want lineitem -> part", reader.LastRequest.Edge)
	}
	if reader.LastRead.VectorIndex != "lineitem" || reader.LastRead.VectorField != "l_partkey" {
		t.Fatalf("recorded read vector = %s.%s, want lineitem.l_partkey", reader.LastRead.VectorIndex, reader.LastRead.VectorField)
	}
}

func TestNewLegacyDirectRelationshipVectorReadRequestBuildsPartToLineitemRead(t *testing.T) {
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7, 8},
	)

	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if read.SourceDomain != "part" || read.TargetDomain != "lineitem" {
		t.Fatalf("domains = %s -> %s, want part -> lineitem", read.SourceDomain, read.TargetDomain)
	}
	if read.Direction != qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft {
		t.Fatalf("direction = %q, want right-to-left", read.Direction)
	}
	if read.VectorIndex != "lineitem" || read.VectorField != "l_partkey" {
		t.Fatalf("vector = %s.%s, want lineitem.l_partkey", read.VectorIndex, read.VectorField)
	}
	if !reflect.DeepEqual(read.SourceCandidates.Rownums, []qsbridge.QuantaRownum{7, 8}) {
		t.Fatalf("source candidates = %#v, want [7 8]", read.SourceCandidates.Rownums)
	}
}

func TestNewLegacyDirectRelationshipVectorReadRequestBuildsLineitemToPartRead(t *testing.T) {
	request := testPartLineitemVectorRequest(
		"lineitem",
		"part",
		qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
		[]qsbridge.QuantaRownum{2, 4},
	)
	request.SourceFragment = qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_partkey"}

	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if read.SourceDomain != "lineitem" || read.TargetDomain != "part" {
		t.Fatalf("domains = %s -> %s, want lineitem -> part", read.SourceDomain, read.TargetDomain)
	}
	if read.Direction != qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight {
		t.Fatalf("direction = %q, want left-to-right", read.Direction)
	}
	if read.VectorIndex != "lineitem" || read.VectorField != "l_partkey" {
		t.Fatalf("vector = %s.%s, want lineitem.l_partkey", read.VectorIndex, read.VectorField)
	}
	if read.SourceFragment.Index != "lineitem" || read.SourceFragment.Field != "l_partkey" {
		t.Fatalf("source fragment = %#v, want lineitem.l_partkey", read.SourceFragment)
	}
}

func TestLegacyDirectRelationshipVectorReaderDelegatesToBackend(t *testing.T) {
	backend := &fakeLegacyDirectRelationshipVectorBackend{
		Candidates: qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 4}},
	}
	reader := &LegacyDirectRelationshipVectorReader{Backend: backend}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7, 8},
	)

	candidates, diagnostics, err := reader.ReadRelatedCandidates(context.Background(), request)
	if err != nil {
		t.Fatalf("ReadRelatedCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !reflect.DeepEqual(candidates.Rownums, []qsbridge.QuantaRownum{2, 4}) {
		t.Fatalf("candidates = %#v, want [2 4]", candidates.Rownums)
	}
	if backend.LastRead.VectorIndex != "lineitem" || backend.LastRead.VectorField != "l_partkey" {
		t.Fatalf("backend vector = %s.%s, want lineitem.l_partkey", backend.LastRead.VectorIndex, backend.LastRead.VectorField)
	}
	if !reflect.DeepEqual(backend.LastRead.SourceCandidates.Rownums, []qsbridge.QuantaRownum{7, 8}) {
		t.Fatalf("backend source candidates = %#v, want [7 8]", backend.LastRead.SourceCandidates.Rownums)
	}
}

func TestLegacyDirectRelationshipVectorReaderDrivesNormalizerWithBackend(t *testing.T) {
	backend := &fakeLegacyDirectRelationshipVectorBackend{
		Candidates: qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 4}},
	}
	reader := &LegacyDirectRelationshipVectorReader{Backend: backend}
	normalizer := NewReaderBackedFilterDomainNormalizer(
		testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7, 8}},
		}},
		reader,
	)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"},
		},
	})
	plan := FilterDomainNormalizationPlan{
		Operation: FilterDomainNormalizeGroupedFilter,
		Translation: qsbridge.QuantaFilterDomainTranslation{
			Required:      true,
			SourceDomains: []string{"part"},
			TargetDomain:  "lineitem",
			Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
		},
		Requests: []FilterDomainNormalizationRequest{{
			Operation:        FilterDomainNormalizeGroupedFilter,
			SourceDomain:     "part",
			TargetDomain:     "lineitem",
			RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{testLineitemPartVectorEdge()},
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		}},
	}

	result, diagnostics, err := normalizer.NormalizeFilterDomains(context.Background(), request, plan)
	if err != nil {
		t.Fatalf("NormalizeFilterDomains error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(result.Leaves) != 1 {
		t.Fatalf("leaves = %#v, want one normalized leaf", result.Leaves)
	}
	leaf := result.Leaves[0]
	if leaf.SourceDomain != "part" || leaf.TargetDomain != "lineitem" {
		t.Fatalf("leaf domains = %s -> %s, want part -> lineitem", leaf.SourceDomain, leaf.TargetDomain)
	}
	if !reflect.DeepEqual(leaf.CandidateSet.Rownums, []qsbridge.QuantaRownum{2, 4}) {
		t.Fatalf("leaf candidates = %#v, want [2 4]", leaf.CandidateSet.Rownums)
	}
	if backend.LastRead.VectorIndex != "lineitem" || backend.LastRead.VectorField != "l_partkey" {
		t.Fatalf("backend vector = %s.%s, want lineitem.l_partkey", backend.LastRead.VectorIndex, backend.LastRead.VectorField)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendReadsChildToParent(t *testing.T) {
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
			}),
		},
	}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"lineitem",
		"part",
		qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
		[]qsbridge.QuantaRownum{2, 4, 6},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	candidates, diagnostics, err := backend.ReadRelationshipVectorCandidates(context.Background(), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if candidates.Index != "part" {
		t.Fatalf("index = %q, want part", candidates.Index)
	}
	want := []qsbridge.QuantaRownum{7, 8}
	if !reflect.DeepEqual(candidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", candidates.Rownums, want)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendReadsParentToChild(t *testing.T) {
	projection := fakeLegacyDirectRelationshipVectorProjectionReader{
		BSI: testRelationshipVectorBSI(map[uint64]int64{
			2: 7,
			4: 8,
			6: 8,
			9: 10,
		}),
	}
	backend := LegacyDirectBitIndexRelationshipVectorBackend{ProjectionReader: projection}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	candidates, diagnostics, err := backend.ReadRelationshipVectorCandidates(context.Background(), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidates error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if candidates.Index != "lineitem" {
		t.Fatalf("index = %q, want lineitem", candidates.Index)
	}
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(candidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", candidates.Rownums, want)
	}
	if projection.LastRead.SourceDomain != "" {
		t.Fatalf("value receiver should not mutate original projection fixture")
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendCachesProjectionWithinRequest(t *testing.T) {
	calls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
			}),
			Calls: &calls,
		},
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
	}
	firstRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("first read request diagnostics = %#v, want none", diagnostics)
	}
	secondRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("second read request diagnostics = %#v, want none", diagnostics)
	}

	first, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(context.Background(), firstRead)
	if err != nil {
		t.Fatalf("first ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v, want none", diagnostics)
	}
	if first.ProjectionCacheHit {
		t.Fatalf("first projection cache hit = true, want false")
	}
	if calls != 1 {
		t.Fatalf("projection calls after first read = %d, want 1", calls)
	}

	second, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(context.Background(), secondRead)
	if err != nil {
		t.Fatalf("second ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", diagnostics)
	}
	if !second.ProjectionCacheHit {
		t.Fatalf("second projection cache hit = false, want true")
	}
	if calls != 1 {
		t.Fatalf("projection calls after second read = %d, want 1", calls)
	}
	if !reflect.DeepEqual(second.TargetCandidates.Rownums, []qsbridge.QuantaRownum{2}) {
		t.Fatalf("second candidates = %#v, want [2]", second.TargetCandidates.Rownums)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendReusesCandidateSupersetWithinRequest(t *testing.T) {
	calls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
			}),
			Calls: &calls,
		},
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
	}
	firstRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7, 8},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("first read request diagnostics = %#v, want none", diagnostics)
	}
	secondRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("second read request diagnostics = %#v, want none", diagnostics)
	}

	ctx := WithQueryScratchpad(context.Background())
	first, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, firstRead)
	if err != nil {
		t.Fatalf("first ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v, want none", diagnostics)
	}
	if first.CandidateCacheHit || first.CandidateCacheMode != "miss" {
		t.Fatalf("first candidate cache = %t/%q, want miss", first.CandidateCacheHit, first.CandidateCacheMode)
	}
	if !reflect.DeepEqual(first.TargetCandidates.Rownums, []qsbridge.QuantaRownum{2, 4, 6}) {
		t.Fatalf("first candidates = %#v, want [2 4 6]", first.TargetCandidates.Rownums)
	}

	second, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, secondRead)
	if err != nil {
		t.Fatalf("second ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", diagnostics)
	}
	if !second.CandidateCacheHit || second.CandidateCacheMode != "retained_subset" {
		t.Fatalf("second candidate cache = %t/%q, want retained_subset hit", second.CandidateCacheHit, second.CandidateCacheMode)
	}
	if second.BatchEqualElapsed != 0 {
		t.Fatalf("second BatchEqualElapsed = %s, want 0 for cached candidate subset", second.BatchEqualElapsed)
	}
	if !reflect.DeepEqual(second.TargetCandidates.Rownums, []qsbridge.QuantaRownum{4, 6}) {
		t.Fatalf("second candidates = %#v, want [4 6]", second.TargetCandidates.Rownums)
	}
	if calls != 1 {
		t.Fatalf("projection calls = %d, want 1", calls)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesProjectedSourceKeyValues(t *testing.T) {
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				20: 8,
				21: 9,
				22: 9,
			}),
		},
		SourceKeyReader: fakeLegacyDirectRelationshipVectorSourceKeyReader{
			Values: []int64{9},
		},
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7},
	)
	request.Edge.Right.PhysicalName = "p_partkey"
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(context.Background(), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	want := []qsbridge.QuantaRownum{21, 22}
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, want) {
		t.Fatalf("candidates = %#v, want %#v", result.TargetCandidates.Rownums, want)
	}
	if !result.SourceKeyProjectionUsed {
		t.Fatalf("SourceKeyProjectionUsed = false, want true")
	}
	if result.SourceValueCount != 1 {
		t.Fatalf("SourceValueCount = %d, want 1", result.SourceValueCount)
	}
}

func testFilterDomainVectorRequest(source string, target string, direction qsbridge.FilterDomainRelationshipVectorDirection, sourceRows []qsbridge.QuantaRownum) qsbridge.FilterDomainRelationshipVectorRequest {
	return qsbridge.FilterDomainRelationshipVectorRequest{
		SourceFragment:   qsbridge.QuantaQueryFragment{Index: source, Field: "p_brand"},
		SourceCandidates: qsbridge.QuantaCandidateSet{Index: source, Rownums: sourceRows},
		SourceDomain:     source,
		TargetDomain:     target,
		Direction:        direction,
	}
}

func testPartLineitemVectorRequest(source string, target string, direction qsbridge.FilterDomainRelationshipVectorDirection, sourceRows []qsbridge.QuantaRownum) qsbridge.FilterDomainRelationshipVectorRequest {
	request := testFilterDomainVectorRequest(source, target, direction, sourceRows)
	request.Operation = qsbridge.FilterDomainNormalizeGroupedFilter
	request.Strategy = qsbridge.PhysicalStrategyRelationshipVectorNormalization
	request.Edge = testLineitemPartVectorEdge()
	return request
}

func testLineitemPartVectorEdge() qsbridge.RelationshipJoinPlanEdge {
	return qsbridge.RelationshipJoinPlanEdge{
		Left: qsbridge.FieldRef{
			Table:    qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:     "l_partkey",
			Encoding: qsbridge.EncodingProfile{LegacyName: "ParentRelation"},
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}
}

type fakeLegacyDirectRelationshipVectorBackend struct {
	LastRead    LegacyDirectRelationshipVectorReadRequest
	Candidates  qsbridge.QuantaCandidateSet
	Diagnostics qsbridge.DiagnosticSet
	Err         error
}

func (b *fakeLegacyDirectRelationshipVectorBackend) ReadRelationshipVectorCandidates(_ context.Context, request LegacyDirectRelationshipVectorReadRequest) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if b != nil {
		b.LastRead = request
		return b.Candidates, b.Diagnostics, b.Err
	}
	return qsbridge.QuantaCandidateSet{}, nil, nil
}

type fakeLegacyDirectRelationshipVectorProjectionReader struct {
	LastRead    LegacyDirectRelationshipVectorReadRequest
	BSI         *roaring64.BSI
	Diagnostics qsbridge.DiagnosticSet
	Err         error
	Calls       *int
}

func (r fakeLegacyDirectRelationshipVectorProjectionReader) ReadRelationshipVectorProjection(_ context.Context, request LegacyDirectRelationshipVectorReadRequest) (*roaring64.BSI, qsbridge.DiagnosticSet, error) {
	r.LastRead = request
	if r.Calls != nil {
		*r.Calls++
	}
	return r.BSI, r.Diagnostics, r.Err
}

type fakeLegacyDirectRelationshipVectorSourceKeyReader struct {
	Values      []int64
	Diagnostics qsbridge.DiagnosticSet
	Err         error
}

func (r fakeLegacyDirectRelationshipVectorSourceKeyReader) ReadRelationshipVectorSourceKeyValues(_ context.Context, _ LegacyDirectRelationshipVectorReadRequest) ([]int64, qsbridge.DiagnosticSet, error) {
	return append([]int64(nil), r.Values...), r.Diagnostics, r.Err
}

func testRelationshipVectorBSI(values map[uint64]int64) *roaring64.BSI {
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetValue(rownum, value)
	}
	return bsi
}
