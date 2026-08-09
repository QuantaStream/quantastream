package server

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestRelationshipReverseArtifactMaintainedByBSIUpdates(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, true)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{
		2: 7,
		4: 8,
		6: 8,
	}, false))

	rownums, stats, ok, err := index.RelationshipReverseArtifactCandidates("lineitem", "l_orderkey", []int64{8, 7})
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

	rownums, _, ok, err = index.RelationshipReverseArtifactCandidates("lineitem", "l_orderkey", []int64{8})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates after update error = %v", err)
	}
	if !ok {
		t.Fatalf("artifact lookup after update ok = false, want true")
	}
	if !reflect.DeepEqual(rownums, []uint64{6}) {
		t.Fatalf("rownums for value 8 after update = %#v, want [6]", rownums)
	}
	rownums, _, ok, err = index.RelationshipReverseArtifactCandidates("lineitem", "l_orderkey", []int64{9})
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
	rownums, stats, ok, err = index.RelationshipReverseArtifactCandidates("lineitem", "l_orderkey", []int64{8})
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

func TestRelationshipReverseArtifactRequiresSchemaFlag(t *testing.T) {
	index := newRelationshipReverseArtifactTestIndex(t, false)
	shardTime := time.Unix(0, 0).UTC()

	index.updateBSICache(testRelationshipReverseArtifactBSIFragment(t, shardTime, map[uint64]int64{2: 7}, false))
	_, _, ok, err := index.RelationshipReverseArtifactCandidates("lineitem", "l_orderkey", []int64{7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidates error = %v", err)
	}
	if ok {
		t.Fatalf("artifact lookup ok = true, want false without relationshipArtifacts.parentToChild")
	}
}

func newRelationshipReverseArtifactTestIndex(t *testing.T, parentToChild bool) *BitmapIndex {
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
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0o644); err != nil {
		t.Fatalf("WriteFile schema error = %v", err)
	}
	table, err := shared.LoadSchema(configDir, "lineitem", nil)
	if err != nil {
		t.Fatalf("LoadSchema error = %v", err)
	}
	return &BitmapIndex{
		tableCache:           map[string]*shared.BasicTable{"lineitem": table},
		bsiCache:             make(map[string]map[string]map[int64]*BSIBitmap),
		seedCache:            make(map[string]*SeedBitmap),
		reverseArtifactCache: make(map[string]map[string]*relationshipReverseArtifact),
	}
}

func testRelationshipReverseArtifactBSIFragment(t *testing.T, shardTime time.Time, values map[uint64]int64, update bool) *BitmapFragment {
	t.Helper()
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetValue(rownum, value)
	}
	data, err := bsi.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary BSI error = %v", err)
	}
	return newBitmapFragment("lineitem", "l_orderkey", 0, shardTime, data, true, false, update)
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
