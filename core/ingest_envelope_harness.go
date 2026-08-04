package core

import (
	"fmt"
	"sync"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// IngestEnvelopeHarnessPutRowResult supplies optional synthetic PutRow results
// for in-memory harness profiling. Real PutRow execution is intentionally out
// of scope for this harness.
type IngestEnvelopeHarnessPutRowResult func(index int, route IngestRouteResult) (shardID string, result PutRowResult, observe bool)

// IngestEnvelopeHarness routes normalized envelopes through selector/router
// plumbing using an in-memory or caller-provided enqueuer.
type IngestEnvelopeHarness struct {
	Enqueuer       IngestRecordEnqueuer
	Profile        *RouterPutRowProfile
	ObservedResult IngestEnvelopeHarnessPutRowResult
}

// IngestEnvelopeHarnessResult is the visible output of a harness run.
type IngestEnvelopeHarnessResult struct {
	Routes  []IngestRouteResult
	Profile RouterPutRowProfileSummary
}

// Run routes each envelope through the existing normalized-envelope adapter.
func (h IngestEnvelopeHarness) Run(envelopes []IngestEnvelope, options IngestEnvelopeRouteOptions) (IngestEnvelopeHarnessResult, qsbridge.DiagnosticSet, error) {
	if h.Enqueuer == nil {
		return IngestEnvelopeHarnessResult{}, nil, fmt.Errorf("ingest envelope harness enqueuer is required")
	}
	result := IngestEnvelopeHarnessResult{
		Routes: make([]IngestRouteResult, 0, len(envelopes)),
	}
	for i, envelope := range envelopes {
		route, diagnostics, err := RouteSelectedIngestEnvelope(h.Enqueuer, envelope, options)
		if diagnostics.BlocksNative() || err != nil {
			result.Profile = h.profileSnapshot()
			return result, diagnostics, err
		}
		result.Routes = append(result.Routes, route)
		h.observeSyntheticResult(i, route)
	}
	result.Profile = h.profileSnapshot()
	return result, nil, nil
}

func (h IngestEnvelopeHarness) observeSyntheticResult(index int, route IngestRouteResult) {
	if h.Profile == nil || h.ObservedResult == nil {
		return
	}
	shardID, result, observe := h.ObservedResult(index, route)
	if observe {
		h.Profile.Observe(shardID, route.Record, result)
	}
}

func (h IngestEnvelopeHarness) profileSnapshot() RouterPutRowProfileSummary {
	if h.Profile == nil {
		return RouterPutRowProfileSummary{}
	}
	return h.Profile.Snapshot()
}

// InMemoryIngestRecordEnqueuer records routed ingest records without touching
// Session or PutRow.
type InMemoryIngestRecordEnqueuer struct {
	mu      sync.Mutex
	records []IngestRecord
}

// Enqueue records one ingest record in memory.
func (e *InMemoryIngestRecordEnqueuer) Enqueue(record IngestRecord) error {
	if e == nil {
		return fmt.Errorf("in-memory ingest enqueuer is nil")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, cloneIngestRecord(record))
	return nil
}

// Records returns a stable copy of enqueued records.
func (e *InMemoryIngestRecordEnqueuer) Records() []IngestRecord {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	records := make([]IngestRecord, len(e.records))
	for i, record := range e.records {
		records[i] = cloneIngestRecord(record)
	}
	return records
}

func cloneIngestRecord(record IngestRecord) IngestRecord {
	record.Data = cloneIngestPayload(record.Data)
	return record
}
