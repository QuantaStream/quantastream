package qsloader

import (
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

func TestJSONAdapterDecodesEnvelopeBatch(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	adapter := JSONAdapter{DefaultSource: "default-source", Now: func() time.Time { return now }}

	requests, err := adapter.Decode(strings.NewReader(`{
		"events": [
			{
				"mode": "stream",
				"event_id": "event-1",
				"source": "unit-test",
				"event_time": "2026-08-08T01:02:03Z",
				"source_offset": "orders:1",
				"shard_key": "order-1",
				"payload": {
					"type": "orders",
					"data": {"o_orderkey": 1001, "o_totalprice": 123.45}
				}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.Envelope.Mode != core.IngestSourceStream || request.Envelope.EventID != "event-1" {
		t.Fatalf("envelope metadata = %#v", request.Envelope)
	}
	if request.Envelope.EventTime.Format(time.RFC3339) != "2026-08-08T01:02:03Z" {
		t.Fatalf("event time = %s", request.Envelope.EventTime.Format(time.RFC3339))
	}
	if request.RouteOptions.ExplicitShardKey != "order-1" {
		t.Fatalf("shard key = %q, want order-1", request.RouteOptions.ExplicitShardKey)
	}
	data := request.Envelope.Payload["data"].(map[string]interface{})
	if _, ok := data["o_orderkey"].(int64); !ok {
		t.Fatalf("o_orderkey type = %T, want int64", data["o_orderkey"])
	}
	if _, ok := data["o_totalprice"].(float64); !ok {
		t.Fatalf("o_totalprice type = %T, want float64", data["o_totalprice"])
	}
}

func TestJSONAdapterTreatsRawObjectAsPayload(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	adapter := JSONAdapter{DefaultSource: "raw-json", Now: func() time.Time { return now }}

	requests, err := adapter.Decode(strings.NewReader(`{"type":"orders","data":{"o_orderkey":1001}}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.Envelope.Source != "raw-json" {
		t.Fatalf("source = %q, want raw-json", request.Envelope.Source)
	}
	if request.Envelope.Payload["type"] != "orders" {
		t.Fatalf("payload = %#v, want raw payload", request.Envelope.Payload)
	}
	if !request.Envelope.EventTime.Equal(now) {
		t.Fatalf("event time = %s, want default now", request.Envelope.EventTime)
	}
}

func TestJSONAdapterRejectsEnvelopeWithoutPayload(t *testing.T) {
	adapter := JSONAdapter{}
	if _, err := adapter.Decode(strings.NewReader(`{"event_id":"missing-payload"}`)); err == nil {
		t.Fatalf("Decode() error = nil, want missing payload")
	}
}
