package qsfixture

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

func TestNewTPCHOrderLineitemEnvelopeFixtureBuildsDeterministicEnvelopes(t *testing.T) {
	startedAt := time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC)
	left, err := NewTPCHOrderLineitemEnvelopeFixture(TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        2,
		LineitemsPerOrder: 3,
		StartedAt:         startedAt,
	})
	if err != nil {
		t.Fatalf("fixture error = %v", err)
	}
	right, err := NewTPCHOrderLineitemEnvelopeFixture(TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        2,
		LineitemsPerOrder: 3,
		StartedAt:         startedAt,
	})
	if err != nil {
		t.Fatalf("fixture error = %v", err)
	}

	if len(left.Tables) != 2 || len(left.Envelopes) != 2 {
		t.Fatalf("fixture shape = %d tables/%d envelopes, want 2/2", len(left.Tables), len(left.Envelopes))
	}
	if left.Envelopes[0].EventID != "tpch.orders.1001" || left.Envelopes[0].SourceOffset != "orders:1" {
		t.Fatalf("first envelope metadata = %#v", left.Envelopes[0])
	}
	leftHash, err := core.HashIngestPayload(left.Envelopes[0].Payload)
	if err != nil {
		t.Fatalf("hash left: %v", err)
	}
	rightHash, err := core.HashIngestPayload(right.Envelopes[0].Payload)
	if err != nil {
		t.Fatalf("hash right: %v", err)
	}
	if leftHash != rightHash {
		t.Fatalf("payload hash = %d/%d, want deterministic", leftHash, rightHash)
	}
	data := left.Envelopes[0].Payload["data"].(map[string]interface{})
	if data["o_orderkey"] != int64(1001) {
		t.Fatalf("o_orderkey = %#v, want 1001", data["o_orderkey"])
	}
	lineitems := data["lineitems"].([]interface{})
	if len(lineitems) != 3 {
		t.Fatalf("lineitem count = %d, want 3", len(lineitems))
	}
	firstLine := lineitems[0].(map[string]interface{})
	if firstLine["l_orderkey"] != int64(1001) || firstLine["l_linenumber"] != int64(1) {
		t.Fatalf("first lineitem = %#v", firstLine)
	}
}

func TestNewTPCHOrderLineitemEnvelopeFixtureHonorsBaseOrderKey(t *testing.T) {
	fixture, err := NewTPCHOrderLineitemEnvelopeFixture(TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        2,
		LineitemsPerOrder: 1,
		BaseOrderKey:      9001,
	})
	if err != nil {
		t.Fatalf("fixture error = %v", err)
	}
	if fixture.Envelopes[0].EventID != "tpch.orders.9001" {
		t.Fatalf("first event ID = %s, want tpch.orders.9001", fixture.Envelopes[0].EventID)
	}
	firstData := fixture.Envelopes[0].Payload["data"].(map[string]interface{})
	secondData := fixture.Envelopes[1].Payload["data"].(map[string]interface{})
	if firstData["o_orderkey"] != int64(9001) || secondData["o_orderkey"] != int64(9002) {
		t.Fatalf("order keys = %#v/%#v, want 9001/9002", firstData["o_orderkey"], secondData["o_orderkey"])
	}
	lineitems := firstData["lineitems"].([]interface{})
	firstLine := lineitems[0].(map[string]interface{})
	if firstLine["l_orderkey"] != int64(9001) {
		t.Fatalf("lineitem order key = %#v, want 9001", firstLine["l_orderkey"])
	}
}

func TestTPCHOrderLineitemEnvelopeFixtureRoutesThroughInMemoryHarness(t *testing.T) {
	fixture, err := NewTPCHOrderLineitemEnvelopeFixture(TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        3,
		LineitemsPerOrder: 2,
	})
	if err != nil {
		t.Fatalf("fixture error = %v", err)
	}
	enqueuer := &core.InMemoryIngestRecordEnqueuer{}
	profile := &core.RouterPutRowProfile{}
	harness := core.IngestEnvelopeHarness{
		Enqueuer: enqueuer,
		Profile:  profile,
		ObservedResult: func(index int, route core.IngestRouteResult) (string, core.PutRowResult, bool) {
			return "fixture-shard", core.PutRowResult{
				TableName:    route.Record.TableName,
				Inserted:     true,
				TotalElapsed: time.Duration(index+1) * time.Millisecond,
			}, true
		},
	}

	result, diagnostics, err := harness.Run(fixture.Envelopes, core.IngestEnvelopeRouteOptions{
		Tables: fixture.Tables,
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if err != nil {
		t.Fatalf("harness error = %v", err)
	}

	if len(result.Routes) != 3 {
		t.Fatalf("routes = %d, want 3", len(result.Routes))
	}
	for i, route := range result.Routes {
		if !route.Enqueued {
			t.Fatalf("route %d not enqueued", i)
		}
		if route.Record.TableName != "orders" {
			t.Fatalf("route %d table = %s, want orders", i, route.Record.TableName)
		}
		if route.ShardKey.Mode != core.IngestShardKeyDedup {
			t.Fatalf("route %d shard mode = %s, want dedup", i, route.ShardKey.Mode)
		}
	}
	if got := len(enqueuer.Records()); got != 3 {
		t.Fatalf("enqueued records = %d, want 3", got)
	}
	if result.Profile.RecordCount != 3 || result.Profile.InsertedCount != 3 {
		t.Fatalf("profile counts = %#v, want 3 inserted records", result.Profile)
	}
	if result.Profile.ByTable["orders"].RecordCount != 3 {
		t.Fatalf("orders profile = %#v, want 3", result.Profile.ByTable["orders"])
	}
	if result.Profile.ByShard["fixture-shard"].RecordCount != 3 {
		t.Fatalf("shard profile = %#v, want 3", result.Profile.ByShard["fixture-shard"])
	}
	if result.Profile.TotalElapsed != 6*time.Millisecond {
		t.Fatalf("profile total = %v, want 6ms", result.Profile.TotalElapsed)
	}
}
