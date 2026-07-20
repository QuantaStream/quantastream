package qsinabox

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

var _ qsruntime.NativeProjectionBSIBatchReader = StandardProjectionBSIReader{}

func TestStandardProjectionBSIReaderBatchReadsFromScratchpadCache(t *testing.T) {
	ctx := qsruntime.WithQueryScratchpad(context.Background())
	cache := qsruntime.ProjectionBSICacheFromContext(ctx)
	if cache == nil {
		t.Fatalf("ProjectionBSICacheFromContext = nil")
	}
	requests := []qsruntime.NativeProjectionBSIReadRequest{
		{
			Index:           "lineitem",
			Field:           qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_orderkey", PhysicalName: "l_orderkey"},
			PhysicalField:   "l_orderkey",
			Rownums:         []qsbridge.QuantaRownum{101, 102},
			FromEpochMillis: 10,
			ToEpochMillis:   20,
		},
		{
			Index:           "lineitem",
			Field:           qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_suppkey", PhysicalName: "l_suppkey"},
			PhysicalField:   "l_suppkey",
			Rownums:         []qsbridge.QuantaRownum{101, 102},
			FromEpochMillis: 10,
			ToEpochMillis:   20,
		},
	}
	orderKey := roaring64.NewDefaultBSI()
	orderKey.SetBigValue(101, big.NewInt(1001))
	orderKey.SetBigValue(102, big.NewInt(1002))
	suppKey := roaring64.NewDefaultBSI()
	suppKey.SetBigValue(101, big.NewInt(501))
	suppKey.SetBigValue(102, big.NewInt(502))
	fromTime, toTime := standardProjectionWindowNanos(nil, "lineitem", 10, 20)
	cache.Set(qsruntime.ProjectionBSICacheKeyFor(requests[0], fromTime, toTime), standardProjectionBitmap(requests[0].Rownums), orderKey)
	cache.Set(qsruntime.ProjectionBSICacheKeyFor(requests[1], fromTime, toTime), standardProjectionBitmap(requests[1].Rownums), suppKey)

	results, diagnostics, err := (StandardProjectionBSIReader{}).ReadProjectionBSIs(ctx, requests)
	if err != nil {
		t.Fatalf("ReadProjectionBSIs error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	assertStandardProjectionBSIValue(t, results[0].BSI, 102, 1002)
	assertStandardProjectionBSIValue(t, results[1].BSI, 101, 501)
	if !standardProjectionTestProbe(results[0].Probes, "standard_bsi_projection_cache_hit", "true") ||
		!standardProjectionTestProbe(results[1].Probes, "standard_bsi_projection_cache_mode", "exact") {
		t.Fatalf("result probes = %#v / %#v, want exact cache hits", results[0].Probes, results[1].Probes)
	}
	if got := standardProjectionScratchpadCounterCount(qsruntime.ExecutionInstrumentationSnapshotFromContext(ctx), "projection_bsi_cache_hit"); got != 2 {
		t.Fatalf("projection_bsi_cache_hit observations = %d, want 2", got)
	}
}

func assertStandardProjectionBSIValue(t *testing.T, bsi *roaring64.BSI, rownum uint64, want int64) {
	t.Helper()
	if bsi == nil {
		t.Fatalf("BSI = nil")
	}
	got, ok := bsi.GetBigValue(rownum)
	if !ok {
		t.Fatalf("BSI value for rownum %d missing", rownum)
	}
	if got.Int64() != want {
		t.Fatalf("BSI value for rownum %d = %d, want %d", rownum, got.Int64(), want)
	}
}

func standardProjectionTestProbe(probes []qsruntime.ExecutionProbe, name string, value string) bool {
	for _, probe := range probes {
		if probe.Name == name && probe.Value == value {
			return true
		}
	}
	return false
}

func standardProjectionScratchpadCounterCount(snapshot qsruntime.ExecutionInstrumentationSnapshot, name string) int {
	count := 0
	for _, counter := range snapshot.Counters {
		if counter.Section == "query_scratchpad" && counter.Name == name {
			count++
		}
	}
	return count
}
