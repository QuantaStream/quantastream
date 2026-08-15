package server

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stvp/rendezvous"
)

func TestRelationshipReverseArtifactMaintainedByBSIUpdates(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
		6: 8,
	}, false))

	rownums, stats, ok, err := index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8, 7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{2, 4, 6}) {
		t.Fatalf("initial rownums = %#v, want [2 4 6]", rownums)
	}
	if stats.Rows != 3 || stats.Values != 2 || stats.TargetRows != 3 {
		t.Fatalf("initial stats = %#v, want rows=3 values=2 targetRows=3", stats)
	}

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{4: 9}, true))

	rownums, _, ok, err = index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates after update error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup after update ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{6}) {
		t.Fatalf("rownums for value 8 after update = %#v, want [6]", rownums)
	}
	rownums, _, ok, err = index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{9})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates for value 9 error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup for value 9 ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4}) {
		t.Fatalf("rownums for value 9 = %#v, want [4]", rownums)
	}

	index.updateBSICache(testRelationshipReverseArtifactClearFragment(t, shardTime, 6))
	rownums, stats, ok, err = index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates after clear error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup after clear ok = false, want true")
	}
	if len(rownums) != 0 {
		t.Fatalf("rownums for value 8 after clear = %#v, want empty", rownums)
	}
	if stats.Rows != 2 || stats.Values != 2 {
		t.Fatalf("stats after clear = %#v, want rows=2 values=2", stats)
	}
}

func TestRelationshipReverseArtifactMaintainsPerShardArtifacts(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	firstShard := time.Unix(0, 0).UTC()
	secondShard := time.Unix(0, int64(time.Hour)).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, firstShard, map[uint64]int64{
		2: 7,
		4: 8,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, secondShard, map[uint64]int64{
		6: 8,
		9: 9,
	}, false))

	artifact := index.reverseArtifactCache["lineitem"]["l_orderkey"]
	if artifact == nil {
		t.Fatal("reverse artifact not created")
	}
	if artifact.rows != 4 {
		t.Fatalf("aggregate artifact rows = %d, want 4", artifact.rows)
	}
	if len(artifact.byShard) != 2 {
		t.Fatalf("shard artifact count = %d, want 2", len(artifact.byShard))
	}
	if got := artifact.byShard[firstShard.UnixNano()].rows; got != 2 {
		t.Fatalf("first shard rows = %d, want 2", got)
	}
	if got := artifact.byShard[secondShard.UnixNano()].rows; got != 2 {
		t.Fatalf("second shard rows = %d, want 2", got)
	}

	rownums, _, ok, err := index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4, 6}) {
		t.Fatalf("rownums for value 8 = %#v, want [4 6]", rownums)
	}
}

func TestRelationshipReverseArtifactDistributedUpdatesWarmOwnedCache(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	conn := shared.NewDefaultConnection("reverse-artifact-owned-cache-test")
	conn.Replicas = 1
	conn.HashTable = rendezvous.New([]string{"this-node"})
	index.Node = &Node{Conn: conn, hashKey: "this-node", State: Active}
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
	}, false))

	artifact := index.reverseArtifactCache["lineitem"]["l_orderkey"]
	if artifact == nil {
		t.Fatal("reverse artifact not created")
	}
	if artifact.owned == nil {
		t.Fatal("owned reverse artifact cache was not warmed during distributed BSI update")
	}
	if artifact.owned.rows != 2 {
		t.Fatalf("owned reverse artifact rows = %d, want 2", artifact.owned.rows)
	}
	if artifact.ownedShardKey == "" {
		t.Fatal("owned reverse artifact shard key is empty")
	}

	rownums, stats, ok, err := index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4}) {
		t.Fatalf("rownums for value 8 = %#v, want [4]", rownums)
	}
	if stats.Rows != 2 || stats.Values != 2 || stats.TargetRows != 1 {
		t.Fatalf("stats = %#v, want rows=2 values=2 targetRows=1", stats)
	}
}

func TestRelationshipReverseArtifactDistributedLookupUsesOwnedShardsWithoutMergedCache(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	conn := shared.NewDefaultConnection("reverse-artifact-owned-shard-test")
	conn.Replicas = 1
	conn.HashTable = rendezvous.New([]string{"this-node", "other-node"})
	index.Node = &Node{Conn: conn, hashKey: "this-node", State: Active}
	ownedShard := testRelationshipReverseArtifactShardTimeForOwnership(t, index, "lineitem", "l_orderkey", true)
	replicaShard := testRelationshipReverseArtifactShardTimeForOwnership(t, index, "lineitem", "l_orderkey", false)

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, ownedShard, map[uint64]int64{
		4: 8,
		5: 9,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, replicaShard, map[uint64]int64{
		6: 8,
		7: 9,
	}, false))

	artifact := index.reverseArtifactCache["lineitem"]["l_orderkey"]
	if artifact == nil {
		t.Fatal("reverse artifact not created")
	}
	artifact.owned = nil
	artifact.ownedShardKey = ""

	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_orderkey", []int64{8, 9})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4, 5}) {
		t.Fatalf("rownums = %#v, want owned shard rows [4 5]", rownums)
	}
	if !reflect.DeepEqual(parentValues, map[uint64]int64{4: 8, 5: 9}) {
		t.Fatalf("parentValues = %#v, want owned shard parent values", parentValues)
	}
	if stats.Rows != 2 || stats.Values != 2 || stats.TargetRows != 2 {
		t.Fatalf("stats = %#v, want rows=2 values=2 targetRows=2", stats)
	}
	if artifact.owned != nil {
		t.Fatal("distributed lookup rebuilt merged owned cache; want direct owned-shard read")
	}

	storageStats, ok, err := index.RelationshipReverseArtifactStatsStorage("lineitem", "l_orderkey")
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactStatsStorage error = %v", err)
	}
	if !ok {
		t.Fatalf("stats lookup ok = false, want true")
	}
	if storageStats.Rows != 2 || storageStats.Values != 2 {
		t.Fatalf("storage stats = %#v, want owned shard rows=2 values=2", storageStats)
	}
	if artifact.owned != nil {
		t.Fatal("distributed stats rebuilt merged owned cache; want direct owned-shard read")
	}
}

func TestRelationshipReverseArtifactDistributedLookupPrefersValidMergedOwnedCache(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	conn := shared.NewDefaultConnection("reverse-artifact-owned-cache-preferred-test")
	conn.Replicas = 1
	conn.HashTable = rendezvous.New([]string{"this-node", "other-node"})
	index.Node = &Node{Conn: conn, hashKey: "this-node", State: Active}
	ownedShard := testRelationshipReverseArtifactShardTimeForOwnership(t, index, "lineitem", "l_orderkey", true)
	replicaShard := testRelationshipReverseArtifactShardTimeForOwnership(t, index, "lineitem", "l_orderkey", false)

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, ownedShard, map[uint64]int64{4: 8}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, replicaShard, map[uint64]int64{6: 8}, false))

	artifact := index.reverseArtifactCache["lineitem"]["l_orderkey"]
	if artifact == nil {
		t.Fatal("reverse artifact not created")
	}
	artifact.owned = &relationshipReverseArtifactData{
		byValue: map[int64]*roaring64.Bitmap{8: roaring64.BitmapOf(44)},
		rows:    1,
	}
	artifact.ownedShardKey = index.relationshipReverseArtifactReadableShardKeyLocked("lineitem", "l_orderkey", artifact)

	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{44}) {
		t.Fatalf("rownums = %#v, want merged owned cache row [44]", rownums)
	}
	if !reflect.DeepEqual(parentValues, map[uint64]int64{44: 8}) {
		t.Fatalf("parentValues = %#v, want merged owned cache parent value", parentValues)
	}
	if stats.Rows != 1 || stats.Values != 1 || stats.TargetRows != 1 {
		t.Fatalf("stats = %#v, want merged owned rows=1 values=1 targetRows=1", stats)
	}
}

func TestRelationshipReverseArtifactSingleNodeUsesAggregateArtifact(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
	}, false))

	artifact := index.reverseArtifactCache["lineitem"]["l_orderkey"]
	if artifact == nil {
		t.Fatal("reverse artifact not created")
	}
	if artifact.owned != nil {
		t.Fatalf("single-node reverse artifact owned cache = %#v, want nil", artifact.owned)
	}

	rownums, _, ok, err := index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4}) {
		t.Fatalf("rownums for value 8 = %#v, want [4]", rownums)
	}
}

func TestRelationshipReverseArtifactRequiresSchemaFlag(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{2: 7}, false))
	_, _, ok, err := index.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if ok {
		t.Fatalf("artifact lookup ok = true, want false without relationshipArtifacts.parentToChild")
	}
}

func TestRelationshipReverseArtifactCandidateValues(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
		6: 8,
	}, false))

	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_orderkey", []int64{8, 7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactCandidateValues ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{2, 4, 6}) {
		t.Fatalf("rownums = %#v, want [2 4 6]", rownums)
	}
	if !reflect.DeepEqual(parentValues, map[uint64]int64{2: 7, 4: 8, 6: 8}) {
		t.Fatalf("parentValues = %#v, want child to parent values", parentValues)
	}
	if stats.TargetRows != 3 || stats.SourceValues != 2 {
		t.Fatalf("stats = %#v, want targetRows=3 sourceValues=2", stats)
	}
}

func TestRelationshipReverseArtifactCandidateValuesUnordered(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
		6: 8,
	}, false))

	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValuesUnordered("lineitem", "l_orderkey", []int64{8, 7, 8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValuesUnordered error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactCandidateValuesUnordered ok = false, want true")
	}
	if !reflect.DeepEqual(parentValues, map[uint64]int64{2: 7, 4: 8, 6: 8}) {
		t.Fatalf("parentValues = %#v, want child to parent values", parentValues)
	}
	if stats.TargetRows != 3 || stats.SourceValues != 2 {
		t.Fatalf("stats = %#v, want targetRows=3 sourceValues=2", stats)
	}
	rownumSet := make(map[uint64]struct{}, len(rownums))
	for _, rownum := range rownums {
		rownumSet[rownum] = struct{}{}
	}
	if !reflect.DeepEqual(rownumSet, map[uint64]struct{}{2: {}, 4: {}, 6: {}}) {
		t.Fatalf("rownum set = %#v, want {2,4,6}", rownumSet)
	}
}

func TestRelationshipReverseArtifactCandidateValuesForRowsFiltersCandidates(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
		6: 8,
	}, false))

	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValuesForRows("lineitem", "l_orderkey", []int64{8, 7}, []uint64{4, 9})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValuesForRows error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactCandidateValuesForRows ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4}) {
		t.Fatalf("rownums = %#v, want [4]", rownums)
	}
	if !reflect.DeepEqual(parentValues, map[uint64]int64{4: 8}) {
		t.Fatalf("parentValues = %#v, want retained child parent value", parentValues)
	}
	if stats.TargetRows != 1 || stats.SourceValues != 2 {
		t.Fatalf("stats = %#v, want targetRows=1 sourceValues=2", stats)
	}
}

func TestRelationshipSiblingDiversityCandidates(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		1: 10,
		2: 10,
		3: 10,
		4: 20,
		5: 20,
		6: 30,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_suppkey", shardTime, map[uint64]int64{
		1: 1,
		2: 1,
		3: 2,
		4: 5,
		5: 5,
		6: 8,
	}, false))

	rownums, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 4, 6})
	if err != nil {
		t.Fatalf("RelationshipSiblingDiversityCandidates error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipSiblingDiversityCandidates ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{1, 2}) {
		t.Fatalf("rownums = %#v, want [1 2]", rownums)
	}
	if stats.Rows != 6 || stats.Values != 3 || stats.CandidateRows != 4 || stats.TargetRows != 2 || stats.Groups != 3 || stats.DiverseGroups != 1 {
		t.Fatalf("stats = %#v, want rows=6 values=3 candidateRows=4 targetRows=2 groups=3 diverseGroups=1", stats)
	}
	if stats.Mode != "sibling_diversity_summary_cache_build" || stats.CacheHit {
		t.Fatalf("summary mode/cache = %q/%t, want build/false", stats.Mode, stats.CacheHit)
	}
}

func TestRelationshipSiblingDiversityCandidatesUsesCachedSummary(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		1: 10,
		2: 10,
		3: 10,
		4: 20,
		5: 20,
		6: 30,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_suppkey", shardTime, map[uint64]int64{
		1: 1,
		2: 1,
		3: 2,
		4: 5,
		5: 5,
		6: 8,
	}, false))

	if _, _, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 4, 6}); err != nil || !ok {
		t.Fatalf("initial RelationshipSiblingDiversityCandidates ok/error = %t/%v, want true/nil", ok, err)
	}
	rownums, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 4, 6})
	if err != nil {
		t.Fatalf("RelationshipSiblingDiversityCandidates cache hit error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipSiblingDiversityCandidates cache hit ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{1}) {
		t.Fatalf("rownums = %#v, want [1]", rownums)
	}
	if !stats.CacheHit || stats.Mode != "sibling_diversity_summary_cache_hit" {
		t.Fatalf("summary mode/cache = %q/%t, want hit/true", stats.Mode, stats.CacheHit)
	}
	if stats.ProjectionRows != 6 || stats.TargetRows != 1 || stats.DiverseGroups != 1 {
		t.Fatalf("stats = %#v, want projectionRows=6 targetRows=1 diverseGroups=1", stats)
	}
}

func TestRelationshipSiblingDiversitySummaryInvalidatedByBSIUpdate(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		1: 10,
		2: 10,
		3: 10,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_suppkey", shardTime, map[uint64]int64{
		1: 1,
		2: 1,
		3: 2,
	}, false))

	if _, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 3}); err != nil || !ok || stats.CacheHit {
		t.Fatalf("initial summary ok/error/cache = %t/%v/%t, want true/nil/false", ok, err, stats.CacheHit)
	}
	if _, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 3}); err != nil || !ok || !stats.CacheHit {
		t.Fatalf("cached summary ok/error/cache = %t/%v/%t, want true/nil/true", ok, err, stats.CacheHit)
	}

	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_suppkey", shardTime, map[uint64]int64{3: 1}, true))
	rownums, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("RelationshipSiblingDiversityCandidates after update error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipSiblingDiversityCandidates after update ok = false, want true")
	}
	if len(rownums) != 0 {
		t.Fatalf("rownums after update = %#v, want empty after summary rebuild", rownums)
	}
	if stats.CacheHit || stats.Mode != "sibling_diversity_summary_cache_build" {
		t.Fatalf("summary mode/cache after update = %q/%t, want rebuild/false", stats.Mode, stats.CacheHit)
	}
}

func TestRelationshipSiblingDiversityCandidatesSkipsLargeProjection(t *testing.T) {
	previousLimit := relationshipSiblingDiversityMaxProjectionRows
	relationshipSiblingDiversityMaxProjectionRows = 3
	previousSummaryEnabled := relationshipSiblingDiversitySummaryCacheEnabled
	relationshipSiblingDiversitySummaryCacheEnabled = false
	t.Cleanup(func() {
		relationshipSiblingDiversityMaxProjectionRows = previousLimit
		relationshipSiblingDiversitySummaryCacheEnabled = previousSummaryEnabled
	})

	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		1: 10,
		2: 10,
		3: 10,
		4: 20,
		5: 20,
		6: 30,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_suppkey", shardTime, map[uint64]int64{
		1: 1,
		2: 1,
		3: 2,
		4: 5,
		5: 5,
		6: 8,
	}, false))

	rownums, stats, ok, err := index.RelationshipSiblingDiversityCandidates("lineitem", "l_orderkey", "l_suppkey", 0, 0, []uint64{1, 2, 4, 6})
	if err != nil {
		t.Fatalf("RelationshipSiblingDiversityCandidates error = %v", err)
	}
	if ok {
		t.Fatalf("RelationshipSiblingDiversityCandidates ok = true, want false")
	}
	if rownums != nil {
		t.Fatalf("rownums = %#v, want nil", rownums)
	}
	if stats.SkipReason != relationshipSiblingDiversitySkipProjectionRowsExceedsLimit {
		t.Fatalf("skip reason = %q, want %q", stats.SkipReason, relationshipSiblingDiversitySkipProjectionRowsExceedsLimit)
	}
	if stats.Rows != 6 || stats.Values != 3 || stats.CandidateRows != 4 || stats.ProjectionRows != 6 || stats.Groups != 3 {
		t.Fatalf("stats = %#v, want rows=6 values=3 candidateRows=4 projectionRows=6 groups=3", stats)
	}
	if stats.TargetRows != 0 || stats.ProjectionElapsed != 0 || stats.EvaluationElapsed != 0 {
		t.Fatalf("stats = %#v, want no projected/evaluated rows", stats)
	}
}

func TestRelationshipReverseArtifactSumGroupsProjectedValues(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2:  7,
		4:  8,
		6:  8,
		10: 9,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2:  1000,
		4:  2500,
		6:  500,
		10: 999,
	}, false))

	groups, stats, ok, err := index.RelationshipReverseArtifactSum("lineitem", "l_orderkey", "l_extendedprice", 0, 0, []uint64{2, 4, 6}, []uint64{8, 7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactSum error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactSum ok = false, want true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want two parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 1000 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 1000", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 3000 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 3000", groups[1])
	}
	if stats.Rows != 4 || stats.Values != 3 || stats.SourceValues != 2 || stats.TargetRows != 3 || stats.Groups != 2 {
		t.Fatalf("stats = %#v, want rows=4 values=3 sourceValues=2 targetRows=3 groups=2", stats)
	}
}

func TestRelationshipAlignedValueSumStorageGroupsProjectedValues(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2: 1000,
		4: 2500,
		6: 500,
	}, false))

	groups, stats, ok, err := index.RelationshipAlignedValueSumStorage("lineitem", "l_extendedprice", 0, 0, []uint64{2, 4, 6, 4}, []uint64{7, 8, 8, 9})
	if err != nil {
		t.Fatalf("RelationshipAlignedValueSumStorage error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipAlignedValueSumStorage ok = false, want true")
	}
	if len(groups) != 3 {
		t.Fatalf("groups = %#v, want three parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 1000 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 1000", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 3000 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 3000", groups[1])
	}
	if groups[2].ParentValue != 9 || groups[2].RepresentativeRow != 4 || groups[2].Count != 1 || groups[2].Sum.Int64() != 2500 {
		t.Fatalf("group[2] = %#v, want duplicate child row counted for parent 9", groups[2])
	}
	if stats.Rows != 4 || stats.Values != 3 || stats.SourceValues != 3 || stats.TargetRows != 4 || stats.Groups != 3 {
		t.Fatalf("stats = %#v, want rows=4 values=3 sourceValues=3 targetRows=4 groups=3", stats)
	}
}

func TestRelationshipVectorValueSumStorageExpandsArtifactAndGroupsValues(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2:  7,
		4:  8,
		6:  8,
		10: 9,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2:  1000,
		4:  2500,
		6:  500,
		10: 999,
	}, false))

	groups, stats, ok, err := index.RelationshipVectorValueSumStorage("lineitem", "l_orderkey", "l_extendedprice", 0, 0, []int64{8, 7, 8})
	if err != nil {
		t.Fatalf("RelationshipVectorValueSumStorage error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipVectorValueSumStorage ok = false, want true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want two parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 1000 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 1000", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 3000 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 3000", groups[1])
	}
	if stats.Rows != 4 || stats.Values != 3 || stats.SourceValues != 2 || stats.TargetRows != 3 || stats.Groups != 2 {
		t.Fatalf("stats = %#v, want rows=4 values=3 sourceValues=2 targetRows=3 groups=2", stats)
	}
}

func TestRelationshipVectorValueSumStorageDiscountedRevenueExpression(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2:  7,
		4:  8,
		6:  8,
		10: 9,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2:  1000,
		4:  2500,
		6:  500,
		10: 999,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_discount", shardTime, map[uint64]int64{
		2:  5,
		4:  10,
		6:  20,
		10: 1,
	}, false))

	valueField := qsbridge.RelationshipAlignedDiscountedRevenueField("l_extendedprice", "l_discount")
	groups, stats, ok, err := index.RelationshipVectorValueSumStorage("lineitem", "l_orderkey", valueField, 0, 0, []int64{8, 7, 8})
	if err != nil {
		t.Fatalf("RelationshipVectorValueSumStorage discounted revenue error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipVectorValueSumStorage discounted revenue ok = false, want true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want two parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 950 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 950", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 2650 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 2650", groups[1])
	}
	if stats.Rows != 4 || stats.Values != 3 || stats.SourceValues != 2 || stats.TargetRows != 3 || stats.Groups != 2 {
		t.Fatalf("stats = %#v, want rows=4 values=3 sourceValues=2 targetRows=3 groups=2", stats)
	}
}

func TestRelationshipAlignedValueSumStorageUsesSortedUniqueRows(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2: 1000,
		4: 2500,
		6: 500,
	}, false))

	groups, stats, ok, err := index.RelationshipAlignedValueSumStorage("lineitem", "l_extendedprice", 0, 0, []uint64{2, 4, 6}, []uint64{7, 8, 8})
	if err != nil {
		t.Fatalf("RelationshipAlignedValueSumStorage sorted unique error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipAlignedValueSumStorage sorted unique ok = false, want true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want two parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 1000 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 1000", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 3000 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 3000", groups[1])
	}
	if stats.Rows != 3 || stats.Values != 2 || stats.SourceValues != 2 || stats.TargetRows != 3 || stats.Groups != 2 {
		t.Fatalf("stats = %#v, want rows=3 values=2 sourceValues=2 targetRows=3 groups=2", stats)
	}
}

func TestRelationshipAlignedValueSumStorageDiscountedRevenueExpression(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_extendedprice", shardTime, map[uint64]int64{
		2: 1000,
		4: 2500,
		6: 500,
	}, false))
	index.updateBSICache(testRelationshipReverseArtifactBSIFragmentForField(t, "l_discount", shardTime, map[uint64]int64{
		2: 5,
		4: 10,
		6: 20,
	}, false))

	valueField := qsbridge.RelationshipAlignedDiscountedRevenueField("l_extendedprice", "l_discount")
	groups, stats, ok, err := index.RelationshipAlignedValueSumStorage("lineitem", valueField, 0, 0, []uint64{2, 4, 6, 4}, []uint64{7, 8, 8, 9})
	if err != nil {
		t.Fatalf("RelationshipAlignedValueSumStorage discounted revenue error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipAlignedValueSumStorage discounted revenue ok = false, want true")
	}
	if len(groups) != 3 {
		t.Fatalf("groups = %#v, want three parent groups", groups)
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 2 || groups[0].Count != 1 || groups[0].Sum.Int64() != 950 {
		t.Fatalf("group[0] = %#v, want parent 7 sum 950", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 4 || groups[1].Count != 2 || groups[1].Sum.Int64() != 2650 {
		t.Fatalf("group[1] = %#v, want parent 8 sum 2650", groups[1])
	}
	if groups[2].ParentValue != 9 || groups[2].RepresentativeRow != 4 || groups[2].Count != 1 || groups[2].Sum.Int64() != 2250 {
		t.Fatalf("group[2] = %#v, want duplicate child row counted for parent 9", groups[2])
	}
	if stats.Rows != 4 || stats.Values != 3 || stats.SourceValues != 3 || stats.TargetRows != 4 || stats.Groups != 3 {
		t.Fatalf("stats = %#v, want rows=4 values=3 sourceValues=3 targetRows=4 groups=3", stats)
	}
}

func TestRelationshipReverseArtifactRebuiltFromPersistedBSIStartup(t *testing.T) {
	hot := newRelationshipReverseArtifactPersistenceTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()
	now := time.Unix(100, 0).UTC()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(2, 7)
	bsi.SetValue(4, 8)
	bsi.SetValue(6, 8)
	hot.bsiCache["lineitem"] = map[string]map[int64]*BSIBitmap{
		"l_orderkey": {
			shardTime.UnixNano(): {
				BSI:         bsi,
				ModTime:     now,
				PersistTime: now.Add(-time.Second),
			},
		},
	}

	if _, _, _, _, err := hot.persistCaches(true); err != nil {
		t.Fatalf("persistCaches returned error: %v", err)
	}
	if err := hot.saveBitmapShardManifestFromCache("test_reverse_artifact"); err != nil {
		t.Fatalf("saveBitmapShardManifestFromCache returned error: %v", err)
	}

	cold := newRelationshipReverseArtifactPersistenceTestIndex(t, true)
	cold.Node.dataDir = hot.Node.dataDir
	if err := cold.readBitmapFiles(cold.fragQueue); err != nil {
		t.Fatalf("cold readBitmapFiles returned error: %v", err)
	}

	rownums, stats, ok, err := cold.RelationshipReverseArtifactCandidatesStorage("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates returned error: %v", err)
	}
	if !ok {
		t.Fatal("artifact lookup ok = false after cold startup, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{4, 6}) {
		t.Fatalf("cold startup rownums = %#v, want [4 6]", rownums)
	}
	if stats.Rows != 3 || stats.Values != 2 || stats.TargetRows != 2 {
		t.Fatalf("cold startup stats = %#v, want rows=3 values=2 targetRows=2", stats)
	}
	snapshot := cold.relationshipReverseArtifactSnapshot()
	if snapshot.Fields != 1 || snapshot.Values != 2 || snapshot.Rows != 3 {
		t.Fatalf("cold startup snapshot = %#v, want fields=1 values=2 rows=3", snapshot)
	}
}

func TestRelationshipReverseArtifactRebuiltAfterSchemaDeploy(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(4, 7)
	bsi.SetValue(6, 8)
	bsi.SetValue(9, 8)
	index.bsiCache["lineitem"] = map[string]map[int64]*BSIBitmap{
		"l_orderkey": {
			shardTime.UnixNano(): {
				BSI: bsi,
			},
		},
	}

	_, _, _, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues before deploy returned error: %v", err)
	}
	if ok {
		t.Fatal("artifact lookup ok before deploy = true, want false")
	}

	index.tableCache["lineitem"] = newRelationshipReverseArtifactTestTable(t, true)
	index.rebuildRelationshipReverseArtifactsForIndex("lineitem")

	rownums, _, stats, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues after deploy returned error: %v", err)
	}
	if !ok {
		t.Fatal("artifact lookup ok after deploy = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{6, 9}) {
		t.Fatalf("rownums after deploy = %#v, want [6 9]", rownums)
	}
	if stats.Rows != 3 || stats.Values != 2 || stats.TargetRows != 2 {
		t.Fatalf("stats after deploy = %#v, want rows=3 values=2 targetRows=2", stats)
	}
}

func newRelationshipReverseArtifactTestIndex(t *testing.T, parentToChild bool) *BitmapIndex {
	t.Helper()
	table := newRelationshipReverseArtifactTestTable(t, parentToChild)
	return &BitmapIndex{
		tableCache:            map[string]*shared.BasicTable{"lineitem": table},
		bsiCache:              make(map[string]map[string]map[int64]*BSIBitmap),
		seedCache:             make(map[string]*SeedBitmap),
		reverseArtifactCache:  make(map[string]map[string]*relationshipReverseArtifact),
		siblingDiversityCache: make(map[string]map[string]map[string]*relationshipSiblingDiversityArtifact),
		siblingDiversityGen:   make(map[string]uint64),
	}
}

func newRelationshipReverseArtifactPersistenceTestIndex(t *testing.T, parentToChild bool) *BitmapIndex {
	t.Helper()
	table := newRelationshipReverseArtifactTestTable(t, parentToChild)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("reverse-artifact-test"),
			dataDir: t.TempDir(),
		},
		bitmapCache:           make(map[string]map[string]map[uint64]map[int64]*StandardBitmap),
		bsiCache:              make(map[string]map[string]map[int64]*BSIBitmap),
		seedCache:             make(map[string]*SeedBitmap),
		reverseArtifactCache:  make(map[string]map[string]*relationshipReverseArtifact),
		siblingDiversityCache: make(map[string]map[string]map[string]*relationshipSiblingDiversityArtifact),
		siblingDiversityGen:   make(map[string]uint64),
		tableCache:            map[string]*shared.BasicTable{"lineitem": table},
		fragQueue:             make(chan *BitmapFragment, 16),
		workers:               []*WorkerThread{NewWorkerThread(0)},
	}
	index.ServicePort = 1
	go index.batchProcessLoop(index.workers[0])
	return index
}

func newRelationshipReverseArtifactTestTable(t *testing.T, parentToChild bool) *shared.BasicTable {
	t.Helper()
	configDir := t.TempDir()
	tableDir := filepath.Join(configDir, "lineitem")
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatalf("MkdirAll schema dir error = %v", err)
	}
	schema := `tableName: lineitem
primaryKey: l_id
attributes:
  - fieldName: l_id
    type: Integer
    mappingStrategy: IntBSI
  - fieldName: l_orderkey
    type: Integer
    mappingStrategy: ParentRelation
    foreignKey: orders.o_orderkey
`
	if parentToChild {
		schema += `    relationshipArtifacts:
      parentToChild: true
`
	}
	schema += `  - fieldName: l_suppkey
    type: Integer
    mappingStrategy: ParentRelation
    foreignKey: supplier.s_suppkey
  - fieldName: l_extendedprice
    type: Float
    mappingStrategy: FloatScaleBSI
    scale: 2
  - fieldName: l_discount
    type: Float
    mappingStrategy: FloatScaleBSI
    scale: 2
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0o644); err != nil {
		t.Fatalf("WriteFile schema error = %v", err)
	}
	table, err := shared.LoadSchema(configDir, "lineitem", nil)
	if err != nil {
		t.Fatalf("LoadSchema error = %v", err)
	}
	return table
}

func testRelationshipReverseArtifactBSIFragment(t *testing.T, shardTime time.Time, values map[uint64]int64, update bool) *BitmapFragment {
	return testRelationshipReverseArtifactBSIFragmentForField(t, "l_orderkey", shardTime, values, update)
}

func testRelationshipReverseArtifactShardTimeForOwnership(t *testing.T, index *BitmapIndex, table, field string, owned bool) time.Time {
	t.Helper()
	for i := int64(0); i < 10000; i++ {
		shardTime := time.Unix(0, i*int64(time.Hour)).UTC()
		if index.relationshipReverseArtifactOwnsShard(table, field, shardTime.UnixNano()) == owned {
			return shardTime
		}
	}
	t.Fatalf("could not find shard ownership=%t for %s.%s", owned, table, field)
	return time.Time{}
}

func testRelationshipReverseArtifactBSIFragmentForField(t *testing.T, field string, shardTime time.Time, values map[uint64]int64, update bool) *BitmapFragment {
	t.Helper()
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetValue(rownum, value)
	}
	data, err := bsi.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary BSI error = %v", err)
	}
	return newBitmapFragment("lineitem", field, 0, shardTime, data, true, false, update)
}

func testRelationshipReverseArtifactClearFragment(t *testing.T, shardTime time.Time, rownums ...uint64) *BitmapFragment {
	t.Helper()
	bitmap := roaring64.NewBitmap()
	for _, rownum := range rownums {
		bitmap.Add(rownum)
	}
	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary clear bitmap error = %v", err)
	}
	return newBitmapFragment("lineitem", "l_orderkey", 0, shardTime, [][]byte{data}, true, true, false)
}
