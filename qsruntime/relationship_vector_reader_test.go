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

func TestRelationshipVectorReverseArtifactConfigFromEnvNormalizesMode(t *testing.T) {
	t.Setenv(relationshipVectorReverseArtifactEnv, "cache")
	t.Setenv(relationshipVectorReverseArtifactEdgeEnv, "")

	config := RelationshipVectorReverseArtifactConfigFromEnv()

	if config.Mode != RelationshipVectorReverseArtifactProcess {
		t.Fatalf("mode = %q, want process", config.Mode)
	}
	if config.Edge != relationshipVectorReverseArtifactDefaultEdge {
		t.Fatalf("edge = %q, want default edge", config.Edge)
	}
	if manager := NewRelationshipVectorReverseArtifactManager(RelationshipVectorReverseArtifactConfig{Mode: "off"}); manager != nil {
		t.Fatalf("disabled manager = %#v, want nil", manager)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesReverseArtifactQueryMode(t *testing.T) {
	t.Setenv(relationshipVectorReverseArtifactEnv, "query")
	t.Setenv(relationshipVectorReverseArtifactEdgeEnv, "lineitem.l_partkey")
	reverseArtifacts := NewRelationshipVectorReverseArtifactManager(RelationshipVectorReverseArtifactConfigFromEnv())
	t.Cleanup(reverseArtifacts.clear)

	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
				9: 10,
			}),
		},
		ReverseArtifacts: reverseArtifacts,
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	)
	request.Edge.Capabilities = qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(WithQueryScratchpad(context.Background()), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if result.CandidateCacheMode != "reverse_artifact" || result.CandidateMode != "reverse_artifact_query" {
		t.Fatalf("candidate cache/mode = %q/%q, want reverse artifact query", result.CandidateCacheMode, result.CandidateMode)
	}
	if result.BatchEqualElapsed != 0 {
		t.Fatalf("BatchEqualElapsed = %s, want 0 for reverse artifact path", result.BatchEqualElapsed)
	}
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", result.TargetCandidates.Rownums, want)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesPhysicalReverseArtifact(t *testing.T) {
	calls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI:   testRelationshipVectorBSI(map[uint64]int64{2: 7}),
			Calls: &calls,
		},
		ReverseArtifactCandidateReader: fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader{
			Result: LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
				Candidates: qsbridge.QuantaCandidateSet{
					Index:   "lineitem",
					Rownums: []qsbridge.QuantaRownum{2, 4, 6},
				},
				Mode:         "reverse_artifact_server",
				CacheHit:     true,
				Rows:         4,
				Values:       3,
				SourceValues: 2,
				TargetRows:   3,
			},
			OK: true,
		},
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	)
	request.Edge.Capabilities = qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(WithQueryScratchpad(context.Background()), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if result.CandidateCacheMode != "reverse_artifact" || result.CandidateMode != "reverse_artifact_server" {
		t.Fatalf("candidate cache/mode = %q/%q, want physical reverse artifact", result.CandidateCacheMode, result.CandidateMode)
	}
	if !result.CandidateCacheHit {
		t.Fatalf("candidate cache hit = false, want true")
	}
	if calls != 0 {
		t.Fatalf("projection calls = %d, want 0 when physical reverse artifact is available", calls)
	}
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", result.TargetCandidates.Rownums, want)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendSkipsBroadPhysicalReverseArtifact(t *testing.T) {
	projectionCalls := 0
	artifactCalls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
				9: 10,
			}),
			Calls: &projectionCalls,
		},
		ReverseArtifactCandidateReader: fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader{
			Stats: LegacyDirectRelationshipVectorReverseArtifactStats{
				Rows:   4,
				Values: 3,
			},
			StatsOK: true,
			Result: LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
				Candidates: qsbridge.QuantaCandidateSet{
					Index:   "lineitem",
					Rownums: []qsbridge.QuantaRownum{2, 4, 6},
				},
				Mode: "reverse_artifact_server",
			},
			OK:    true,
			Calls: &artifactCalls,
		},
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	)
	request.Edge.Capabilities = qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(WithQueryScratchpad(context.Background()), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if result.CandidateCacheMode == "reverse_artifact" || strings.HasPrefix(result.CandidateMode, "reverse_artifact") {
		t.Fatalf("candidate cache/mode = %q/%q, want non-artifact fallback", result.CandidateCacheMode, result.CandidateMode)
	}
	if artifactCalls != 0 {
		t.Fatalf("artifact calls = %d, want 0 for broad source set", artifactCalls)
	}
	if projectionCalls != 1 {
		t.Fatalf("projection calls = %d, want fallback projection", projectionCalls)
	}
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", result.TargetCandidates.Rownums, want)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesReverseArtifactProcessCache(t *testing.T) {
	t.Setenv(relationshipVectorReverseArtifactEnv, "process")
	t.Setenv(relationshipVectorReverseArtifactEdgeEnv, "lineitem.l_partkey")
	reverseArtifacts := NewRelationshipVectorReverseArtifactManager(RelationshipVectorReverseArtifactConfigFromEnv())
	t.Cleanup(reverseArtifacts.clear)

	calls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
				9: 10,
			}),
			Calls: &calls,
		},
		ProjectionCache:  NewLegacyDirectRelationshipVectorProjectionCache(),
		ReverseArtifacts: reverseArtifacts,
	}
	firstRequest := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	)
	firstRequest.Edge.Capabilities = qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion}
	firstRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(firstRequest)
	if diagnostics.BlocksNative() {
		t.Fatalf("first read request diagnostics = %#v, want none", diagnostics)
	}
	secondRequest := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{7},
	)
	secondRequest.Edge.Capabilities = qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion}
	secondRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(secondRequest)
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
	if first.CandidateCacheMode != "reverse_artifact" || first.CandidateMode != "reverse_artifact_process_build" {
		t.Fatalf("first candidate cache/mode = %q/%q, want reverse artifact process build", first.CandidateCacheMode, first.CandidateMode)
	}
	if calls != 1 {
		t.Fatalf("projection calls after first read = %d, want 1", calls)
	}

	second, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, secondRead)
	if err != nil {
		t.Fatalf("second ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", diagnostics)
	}
	if second.CandidateCacheMode != "reverse_artifact" || second.CandidateMode != "reverse_artifact_process_hit" {
		t.Fatalf("second candidate cache/mode = %q/%q, want reverse artifact process hit", second.CandidateCacheMode, second.CandidateMode)
	}
	if second.ProjectionElapsed != 0 || second.ProjectionCacheHit {
		t.Fatalf("second projection timing/cache = %s/%t, want no FK projection", second.ProjectionElapsed, second.ProjectionCacheHit)
	}
	if calls != 1 {
		t.Fatalf("projection calls after second read = %d, want process artifact hit without projection", calls)
	}
	want := []qsbridge.QuantaRownum{2}
	if !reflect.DeepEqual(second.TargetCandidates.Rownums, want) {
		t.Fatalf("second rownums = %#v, want %#v", second.TargetCandidates.Rownums, want)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendRequiresDeclaredReverseArtifactCapability(t *testing.T) {
	t.Setenv(relationshipVectorReverseArtifactEnv, "process")
	t.Setenv(relationshipVectorReverseArtifactEdgeEnv, "lineitem.l_partkey")
	reverseArtifacts := NewRelationshipVectorReverseArtifactManager(RelationshipVectorReverseArtifactConfigFromEnv())
	t.Cleanup(reverseArtifacts.clear)

	calls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				2: 7,
				4: 8,
				6: 8,
				9: 10,
			}),
			Calls: &calls,
		},
		ReverseArtifacts: reverseArtifacts,
	}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8, 7},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("read request diagnostics = %#v, want none", diagnostics)
	}

	result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(WithQueryScratchpad(context.Background()), read)
	if err != nil {
		t.Fatalf("ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if strings.HasPrefix(result.CandidateMode, "reverse_artifact") || result.CandidateCacheMode == "reverse_artifact" {
		t.Fatalf("candidate cache/mode = %q/%q, want reverse artifact disabled without schema capability", result.CandidateCacheMode, result.CandidateMode)
	}
	if calls != 1 {
		t.Fatalf("projection calls = %d, want fallback relationship vector projection", calls)
	}
	want := []qsbridge.QuantaRownum{2, 4, 6}
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", result.TargetCandidates.Rownums, want)
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

func TestLegacyDirectBitIndexRelationshipVectorBackendPrefersProjectionReaderForParentToChildExpansion(t *testing.T) {
	projectionCalls := 0
	borrowCalls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				20: 8,
				21: 7,
				22: 9,
			}),
			Calls: &projectionCalls,
		},
		SourceKeyReader: fakeLegacyDirectRelationshipVectorSourceKeyReader{
			Values: []int64{7, 8},
		},
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			borrowCalls++
			return nil, nil, nil
		}),
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{100, 101},
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
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, []qsbridge.QuantaRownum{20, 21}) {
		t.Fatalf("candidates = %#v, want [20 21]", result.TargetCandidates.Rownums)
	}
	if result.CandidateMode != "batch_equal" || result.CandidateCacheMode != "miss" {
		t.Fatalf("candidate mode/cache = %q/%q, want batch_equal/miss", result.CandidateMode, result.CandidateCacheMode)
	}
	if projectionCalls != 1 || borrowCalls != 0 {
		t.Fatalf("calls projection/borrow = %d/%d, want 1/0", projectionCalls, borrowCalls)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesDirectBatchEQForBoundedParentToChildExpansion(t *testing.T) {
	projectionCalls := 0
	borrowCalls := 0
	queryCalls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		PreferDirectParentToChildCandidate: true,
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI:   testRelationshipVectorBSI(map[uint64]int64{20: 8}),
			Calls: &projectionCalls,
		},
		SourceKeyReader: fakeLegacyDirectRelationshipVectorSourceKeyReader{
			Values: []int64{7, 8},
		},
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			borrowCalls++
			if len(request.Query.Fragments) != 1 {
				t.Fatalf("fragments = %#v, want one BATCH_EQ fragment", request.Query.Fragments)
			}
			fragment := request.Query.Fragments[0]
			if fragment.Index != "lineitem" || fragment.Field != "l_partkey" || fragment.BSIOp != qsbridge.QuantaBSIOpBatchEQ {
				t.Fatalf("fragment = %#v, want lineitem.l_partkey BATCH_EQ", fragment)
			}
			if len(fragment.Values) != 2 || fragment.Values[0].Int64() != 7 || fragment.Values[1].Int64() != 8 {
				t.Fatalf("fragment values = %#v, want [7 8]", fragment.Values)
			}
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryCalls++
					return BitmapQueryResult{
						Success: true,
						Count:   3,
						Rownums: []qsbridge.QuantaRownum{2, 4, 6},
					}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{100, 101},
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
	if !reflect.DeepEqual(result.TargetCandidates.Rownums, []qsbridge.QuantaRownum{2, 4, 6}) {
		t.Fatalf("candidates = %#v, want [2 4 6]", result.TargetCandidates.Rownums)
	}
	if result.CandidateMode != "direct_batch_eq" || result.CandidateCacheMode != "direct_query" {
		t.Fatalf("candidate mode/cache = %q/%q, want direct_batch_eq/direct_query", result.CandidateMode, result.CandidateCacheMode)
	}
	if !result.SourceKeyProjectionUsed || result.SourceKeyProjectionReason != "projected_source_key" {
		t.Fatalf("source key projection = %t/%q, want projected source key", result.SourceKeyProjectionUsed, result.SourceKeyProjectionReason)
	}
	if result.ProjectionElapsed != 0 || result.ProjectionCacheHit {
		t.Fatalf("projection timing/cache = %s/%t, want no FK projection", result.ProjectionElapsed, result.ProjectionCacheHit)
	}
	if projectionCalls != 0 || borrowCalls != 1 || queryCalls != 1 {
		t.Fatalf("calls projection/borrow/query = %d/%d/%d, want 0/1/1", projectionCalls, borrowCalls, queryCalls)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendReusesDirectBatchEQCoveredSuperset(t *testing.T) {
	queryCalls := 0
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryCalls++
					return BitmapQueryResult{
						Success: true,
						Count:   3,
						Rownums: []qsbridge.QuantaRownum{2, 4, 6},
					}, nil, nil
				},
			}, nil, nil
		}),
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
	firstRead.AllowCandidateSuperset = true
	secondRead, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		[]qsbridge.QuantaRownum{8},
	))
	if diagnostics.BlocksNative() {
		t.Fatalf("second read request diagnostics = %#v, want none", diagnostics)
	}
	secondRead.AllowCandidateSuperset = true

	ctx := WithQueryScratchpad(context.Background())
	first, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, firstRead)
	if err != nil {
		t.Fatalf("first ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v, want none", diagnostics)
	}
	if first.CandidateCacheHit || first.CandidateCacheMode != "direct_query" || first.CandidateMode != "direct_batch_eq" {
		t.Fatalf("first candidate cache/mode = %t/%q/%q, want direct query", first.CandidateCacheHit, first.CandidateCacheMode, first.CandidateMode)
	}

	second, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, secondRead)
	if err != nil {
		t.Fatalf("second ReadRelationshipVectorCandidateResult error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", diagnostics)
	}
	if !second.CandidateCacheHit || second.CandidateCacheMode != "covered_superset" || second.CandidateMode != "candidate_cache" {
		t.Fatalf("second candidate cache/mode = %t/%q/%q, want covered superset cache hit", second.CandidateCacheHit, second.CandidateCacheMode, second.CandidateMode)
	}
	if !reflect.DeepEqual(second.TargetCandidates.Rownums, []qsbridge.QuantaRownum{2, 4, 6}) {
		t.Fatalf("second candidates = %#v, want broad seed [2 4 6]", second.TargetCandidates.Rownums)
	}
	if queryCalls != 1 {
		t.Fatalf("direct query calls = %d, want 1", queryCalls)
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendUsesValueSetScanForMediumParentToChildExpansion(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	for rownum := uint64(1); rownum <= 2000; rownum++ {
		fkBSI.SetValue(rownum, int64(rownum%200))
	}
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{BSI: fkBSI},
	}
	sourceRows := make([]qsbridge.QuantaRownum, 0, 64)
	for value := 10; value < 74; value++ {
		sourceRows = append(sourceRows, qsbridge.QuantaRownum(value))
	}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		sourceRows,
	))
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
	if result.CandidateMode != "value_set_scan" {
		t.Fatalf("CandidateMode = %q, want value_set_scan", result.CandidateMode)
	}
	if result.BatchEqualElapsed != 0 {
		t.Fatalf("BatchEqualElapsed = %s, want 0 for scan path", result.BatchEqualElapsed)
	}
	if len(result.TargetCandidates.Rownums) != 640 {
		t.Fatalf("candidate rows = %d, want 640", len(result.TargetCandidates.Rownums))
	}
	for _, rownum := range result.TargetCandidates.Rownums {
		value, ok := fkBSI.GetValue(uint64(rownum))
		if !ok || value < 10 || value >= 74 {
			t.Fatalf("candidate row %d value = %d/%t, want value in [10,74)", rownum, value, ok)
		}
	}
}

func TestLegacyDirectBitIndexRelationshipVectorBackendKeepsBatchEqualForLargeParentToChildExpansion(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	for rownum := uint64(1); rownum <= 33000; rownum++ {
		fkBSI.SetValue(rownum, int64(rownum%2000))
	}
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		ProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{BSI: fkBSI},
	}
	sourceRows := make([]qsbridge.QuantaRownum, 0, 64)
	for value := 10; value < 74; value++ {
		sourceRows = append(sourceRows, qsbridge.QuantaRownum(value))
	}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(testPartLineitemVectorRequest(
		"part",
		"lineitem",
		qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		sourceRows,
	))
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
	if result.CandidateMode != "batch_equal" {
		t.Fatalf("CandidateMode = %q, want batch_equal", result.CandidateMode)
	}
	if result.CandidateScanElapsed != 0 {
		t.Fatalf("CandidateScanElapsed = %s, want 0 for BatchEqual path", result.CandidateScanElapsed)
	}
	if len(result.TargetCandidates.Rownums) == 0 {
		t.Fatalf("candidate rows = 0, want non-empty BatchEqual result")
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

type fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader struct {
	Result      LegacyDirectRelationshipVectorReverseArtifactCandidateResult
	Stats       LegacyDirectRelationshipVectorReverseArtifactStats
	Diagnostics qsbridge.DiagnosticSet
	Err         error
	OK          bool
	StatsOK     bool
	Calls       *int
}

func (r fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader) ReadRelationshipVectorReverseArtifactCandidates(
	context.Context,
	LegacyDirectRelationshipVectorReadRequest,
	[]int64,
) (LegacyDirectRelationshipVectorReverseArtifactCandidateResult, qsbridge.DiagnosticSet, bool, error) {
	if r.Calls != nil {
		*r.Calls++
	}
	return r.Result, r.Diagnostics, r.OK, r.Err
}

func (r fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader) RelationshipVectorReverseArtifactStats(
	context.Context,
	LegacyDirectRelationshipVectorReadRequest,
) (LegacyDirectRelationshipVectorReverseArtifactStats, bool, error) {
	return r.Stats, r.StatsOK, r.Err
}

func testRelationshipVectorBSI(values map[uint64]int64) *roaring64.BSI {
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetValue(rownum, value)
	}
	return bsi
}
