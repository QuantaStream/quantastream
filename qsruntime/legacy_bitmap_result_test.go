package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	legacy "github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyBitmapQueryResultAdapterReportsNilResponse(t *testing.T) {
	result := LegacyBitmapQueryResultAdapter{}.ToBitmapQueryResult(nil)

	if result.Success {
		t.Fatalf("success = true, want false")
	}
	if result.ErrorMessage == "" {
		t.Fatalf("error message should be populated")
	}
}

func TestLegacyBitmapQueryResultAdapterConvertsBitmapResponse(t *testing.T) {
	bitmap := roaring64.NewBitmap()
	bitmap.Add(10)
	bitmap.Add(12)
	response := &legacy.BitmapQueryResponse{
		Success: true,
		Count:   2,
		Results: bitmap,
		Probes: []legacy.QueryResultProbe{
			{
				Section: "direct_bitmap_server",
				Name:    "bsi_load_elapsed",
				Value:   "7ms",
				Detail:  "lineitem.l_receiptdate",
			},
		},
	}

	result := LegacyBitmapQueryResultAdapter{}.ToBitmapQueryResult(response)
	if !result.Success {
		t.Fatalf("success = false, want true")
	}
	if result.Count != 2 {
		t.Fatalf("count = %d, want 2", result.Count)
	}
	if len(result.Rownums) != 2 {
		t.Fatalf("rownum count = %d, want 2", len(result.Rownums))
	}
	if result.Rownums[0] != qsbridge.QuantaRownum(10) || result.Rownums[1] != qsbridge.QuantaRownum(12) {
		t.Fatalf("rownums = %#v, want 10,12", result.Rownums)
	}
	if len(result.Probes) != 1 {
		t.Fatalf("probe count = %d, want 1", len(result.Probes))
	}
	if result.Probes[0].Section != "direct_bitmap_server" || result.Probes[0].Name != "bsi_load_elapsed" {
		t.Fatalf("probe = %#v, want direct_bitmap_server/bsi_load_elapsed", result.Probes[0])
	}
}

func TestLegacyBitmapQueryResultAdapterCountOnlySkipsRownums(t *testing.T) {
	response := &legacy.BitmapQueryResponse{
		Success: true,
		Results: roaring64.BitmapOf(10, 11, 12),
	}

	result := LegacyBitmapQueryResultAdapter{}.ToCountOnlyBitmapQueryResult(response)

	if !result.Success {
		t.Fatalf("success = false, want true")
	}
	if result.Count != 3 {
		t.Fatalf("count = %d, want 3", result.Count)
	}
	if len(result.Rownums) != 0 {
		t.Fatalf("rownums = %#v, want none for count-only result", result.Rownums)
	}
}
