package core

import (
	"fmt"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// IngestSourceMode identifies the operational source shape that produced an
// ingest envelope. It is independent of whether the upstream payload had a
// physical envelope on the wire.
type IngestSourceMode string

const (
	// IngestSourceBatch identifies bounded batch/file-style load input.
	IngestSourceBatch IngestSourceMode = "batch"
	// IngestSourceStream identifies replayable stream/broker-style input.
	IngestSourceStream IngestSourceMode = "stream"
)

// IngestEnvelope is the normalized input contract for parsed ingest events.
// Adapters may synthesize this metadata when the source does not provide a
// physical envelope.
type IngestEnvelope struct {
	Mode         IngestSourceMode
	EventID      string
	Source       string
	EventTime    time.Time
	SourceOffset string
	Payload      map[string]interface{}
	Headers      map[string]interface{}
}

// StreamIngestEnvelopeRequest describes a parsed streaming event.
type StreamIngestEnvelopeRequest struct {
	EventID      string
	Source       string
	EventTime    time.Time
	SourceOffset string
	Payload      map[string]interface{}
	Headers      map[string]interface{}
}

// BatchIngestEnvelopeRequest describes a parsed batch row or record.
type BatchIngestEnvelopeRequest struct {
	Source       string
	EventTime    time.Time
	SourceOffset string
	Payload      map[string]interface{}
	Headers      map[string]interface{}
}

// IngestEnvelopeRouteOptions carries route-only knobs outside the normalized
// source envelope.
type IngestEnvelopeRouteOptions struct {
	Tables           []*Table
	ExplicitShardKey string
	PayloadHash      uint64
	DedupTTL         time.Duration
}

// NewStreamIngestEnvelope normalizes streaming input into the shared ingest
// contract. EventID may be synthesized by the adapter from broker metadata when
// the upstream message lacks one.
func NewStreamIngestEnvelope(request StreamIngestEnvelopeRequest) (IngestEnvelope, error) {
	envelope := IngestEnvelope{
		Mode:         IngestSourceStream,
		EventID:      request.EventID,
		Source:       request.Source,
		EventTime:    request.EventTime,
		SourceOffset: request.SourceOffset,
		Payload:      cloneIngestPayload(request.Payload),
		Headers:      cloneIngestPayload(request.Headers),
	}
	return envelope.validate()
}

// NewBatchIngestEnvelope normalizes bounded batch input into the shared ingest
// contract. Batch sources usually omit EventID and route by explicit shard key
// or table primary key.
func NewBatchIngestEnvelope(request BatchIngestEnvelopeRequest) (IngestEnvelope, error) {
	envelope := IngestEnvelope{
		Mode:         IngestSourceBatch,
		Source:       request.Source,
		EventTime:    request.EventTime,
		SourceOffset: request.SourceOffset,
		Payload:      cloneIngestPayload(request.Payload),
		Headers:      cloneIngestPayload(request.Headers),
	}
	return envelope.validate()
}

// EnvelopeMap exposes normalized metadata to selector expressions under the
// explicit envelope root.
func (e IngestEnvelope) EnvelopeMap() map[string]interface{} {
	values := map[string]interface{}{
		"mode": string(e.Mode),
	}
	if e.EventID != "" {
		values["event_id"] = e.EventID
	}
	if e.Source != "" {
		values["source"] = e.Source
	}
	if !e.EventTime.IsZero() {
		values["event_time"] = e.EventTime
	}
	if e.SourceOffset != "" {
		values["source_offset"] = e.SourceOffset
	}
	if len(e.Headers) > 0 {
		values["headers"] = cloneIngestPayload(e.Headers)
	}
	return values
}

// RouteRequest converts the normalized envelope into the existing selector and
// router adapter request shape.
func (e IngestEnvelope) RouteRequest(options IngestEnvelopeRouteOptions) IngestRouteRequest {
	return IngestRouteRequest{
		Tables:           options.Tables,
		Envelope:         e.EnvelopeMap(),
		Payload:          cloneIngestPayload(e.Payload),
		ExplicitShardKey: options.ExplicitShardKey,
		EventID:          e.EventID,
		Source:           e.Source,
		EventTime:        e.EventTime,
		SourceOffset:     e.SourceOffset,
		PayloadHash:      options.PayloadHash,
		DedupTTL:         options.DedupTTL,
	}
}

// BuildSelectedIngestRecordFromEnvelope evaluates selectors and builds a router
// record from a normalized ingest envelope.
func BuildSelectedIngestRecordFromEnvelope(envelope IngestEnvelope, options IngestEnvelopeRouteOptions) (IngestRouteResult, qsbridge.DiagnosticSet, error) {
	normalized, err := envelope.validate()
	if err != nil {
		return IngestRouteResult{}, nil, err
	}
	return BuildSelectedIngestRecord(normalized.RouteRequest(options))
}

// RouteSelectedIngestEnvelope builds and enqueues one selected ingest record
// from a normalized envelope.
func RouteSelectedIngestEnvelope(enqueuer IngestRecordEnqueuer, envelope IngestEnvelope, options IngestEnvelopeRouteOptions) (IngestRouteResult, qsbridge.DiagnosticSet, error) {
	normalized, err := envelope.validate()
	if err != nil {
		return IngestRouteResult{}, nil, err
	}
	return RouteSelectedIngestRecord(enqueuer, normalized.RouteRequest(options))
}

func (e IngestEnvelope) validate() (IngestEnvelope, error) {
	switch e.Mode {
	case IngestSourceBatch, IngestSourceStream:
	default:
		return IngestEnvelope{}, fmt.Errorf("ingest envelope mode %q is not supported", e.Mode)
	}
	if e.Payload == nil {
		return IngestEnvelope{}, fmt.Errorf("ingest envelope payload is required")
	}
	return e, nil
}
