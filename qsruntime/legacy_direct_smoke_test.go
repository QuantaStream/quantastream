package qsruntime

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectRuntimeSmokeScaffold(t *testing.T) {
	if os.Getenv("QUANTA_LEGACY_DIRECT_SMOKE") != "1" {
		t.Skip("set QUANTA_LEGACY_DIRECT_SMOKE=1 to exercise source.NewQuantaSource wiring")
	}
	config := NewDirectRuntimeConfig(
		os.Getenv("QUANTA_DIRECT_BASE_DIR"),
		os.Getenv("QUANTA_DIRECT_CONSUL"),
		0,
		1,
	)
	runtime, diagnostics, err := LegacyQuantaSourceFactory{
		TableCache: core.NewTableCacheStruct(),
	}.NewDirectRuntime(context.Background(), config)
	if err != nil {
		t.Fatalf("new direct runtime: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	if runtime == nil {
		t.Fatalf("runtime is nil")
	}

	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "part",
			Field:     "p_size",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(1),
		}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "part_count", Type: qsbridge.DataTypeInt}}
	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct bitmap query: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("execute diagnostics: %#v", result.Diagnostics)
	}
	if result.Count == 0 && result.CandidateCount() == 0 {
		t.Fatalf("direct bitmap query returned no count or candidates")
	}
}
