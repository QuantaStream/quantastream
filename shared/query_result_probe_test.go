package shared

import (
	"testing"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestQueryResultProbeSampleRoundTrip(t *testing.T) {
	sample := NewQueryResultProbeSample("direct_bitmap_server", "bsi_load_elapsed", "12ms", "lineitem.l_shipdate")

	probe, ok := QueryResultProbeFromSample(sample)
	if !ok {
		t.Fatalf("probe sample did not decode")
	}
	if probe.Section != "direct_bitmap_server" || probe.Name != "bsi_load_elapsed" || probe.Value != "12ms" || probe.Detail != "lineitem.l_shipdate" {
		t.Fatalf("probe = %#v, want direct_bitmap_server/bsi_load_elapsed/12ms/lineitem.l_shipdate", probe)
	}
}

func TestIntermediateResultUnmarshalSkipsProbeSamples(t *testing.T) {
	union, err := roaring64.NewBitmap().MarshalBinary()
	if err != nil {
		t.Fatalf("marshal union: %v", err)
	}
	bitmap := roaring64.BitmapOf(1, 2, 3)
	encoded, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	result := &pb.QueryResult{
		Unions:     union,
		Existences: union,
		Samples: []*pb.BitmapResult{
			NewQueryResultProbeSample("direct_bitmap_server", "query_elapsed", "1ms", ""),
			{Field: "visible", RowId: 7, Bitmap: encoded},
		},
	}
	ir := NewIntermediateResult("lineitem")
	if err := ir.UnmarshalAndAdd(result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	samples := ir.GetSamples()
	if len(samples) != 1 {
		t.Fatalf("sample count = %d, want 1", len(samples))
	}
	if samples[0].Field != "visible" || samples[0].RowID != 7 {
		t.Fatalf("sample = %#v, want visible row 7", samples[0])
	}
}
