package server

import (
	"context"
	"strings"
	"testing"

	pb "github.com/QuantaStream/quantastream/grpc"
)

func TestLocalNodeAdapterReportsReadinessWithoutStringSearch(t *testing.T) {
	adapter := LocalNodeAdapter{
		BitmapIndex: &BitmapIndex{},
		KVStore:     &KVStore{},
	}
	readiness := adapter.Readiness()
	if !readiness.Ready {
		t.Fatalf("readiness.Ready = false, want true for bitmap and kv mounted")
	}
	if !readiness.BitmapIndex || !readiness.KVStore {
		t.Fatalf("readiness = %+v, want bitmap and kv mounted", readiness)
	}
	if readiness.StringSearch {
		t.Fatalf("readiness.StringSearch = true, want false until local search shim exists")
	}
	if len(readiness.StreamingRisks) == 0 {
		t.Fatalf("streaming risks were not preserved")
	}
}

func TestLocalBitmapIndexAdapterReportsMissingMount(t *testing.T) {
	_, err := (LocalBitmapIndexAdapter{}).Query(context.Background(), &pb.BitmapQuery{})
	if err == nil || !strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("err = %v, want missing mount error", err)
	}
}
