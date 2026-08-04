package qsfixture

import (
	"fmt"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

// SessionEnvelopeIngestRequest describes a deterministic envelope-to-session
// write pass. The caller owns session creation and connection topology.
type SessionEnvelopeIngestRequest struct {
	Session      *core.Session
	Envelopes    []core.IngestEnvelope
	RouteOptions core.IngestEnvelopeRouteOptions
	SkipFlush    bool
}

// SessionEnvelopeIngestResult captures route, PutRow, and final flush details
// for a deterministic ingest pass.
type SessionEnvelopeIngestResult struct {
	Routes       []core.IngestRouteResult
	PutRows      []core.PutRowResult
	Profile      core.RouterPutRowProfileSummary
	FlushProfile shared.BatchBufferFlushProfile
	TotalElapsed time.Duration
}

// IngestEnvelopesWithSession routes normalized envelopes, writes them through
// an already-open Session, and flushes by default. It is a reusable test and
// benchmark spine for standard-native and direct loader paths.
func IngestEnvelopesWithSession(request SessionEnvelopeIngestRequest) (SessionEnvelopeIngestResult, qsbridge.DiagnosticSet, error) {
	if request.Session == nil {
		return SessionEnvelopeIngestResult{}, nil, fmt.Errorf("ingest session is required")
	}
	startedAt := time.Now()
	result := SessionEnvelopeIngestResult{
		Routes:  make([]core.IngestRouteResult, 0, len(request.Envelopes)),
		PutRows: make([]core.PutRowResult, 0, len(request.Envelopes)),
	}
	profile := &core.RouterPutRowProfile{}
	var diagnostics qsbridge.DiagnosticSet

	for _, envelope := range request.Envelopes {
		route, routeDiagnostics, err := core.BuildSelectedIngestRecordFromEnvelope(envelope, request.RouteOptions)
		diagnostics = append(diagnostics, routeDiagnostics...)
		if diagnostics.BlocksNative() || err != nil {
			result.Profile = profile.Snapshot()
			result.TotalElapsed = time.Since(startedAt)
			return result, diagnostics, err
		}
		options, err := route.Record.PutRowOptionsWithPayloadHash()
		if err != nil {
			result.Profile = profile.Snapshot()
			result.TotalElapsed = time.Since(startedAt)
			return result, diagnostics, fmt.Errorf("build PutRow options for envelope %s: %w", envelope.EventID, err)
		}
		route.Record.PayloadHash = options.PayloadHash
		putResult, err := request.Session.PutRowWithOptions(route.Record.TableName, route.Record.Data, 0, false, false, options)
		if err != nil {
			result.Profile = profile.Snapshot()
			result.TotalElapsed = time.Since(startedAt)
			return result, diagnostics, fmt.Errorf("put envelope %s into %s: %w", envelope.EventID, route.Record.TableName, err)
		}
		profile.Observe(route.Record.ShardKey, route.Record, putResult)
		result.Routes = append(result.Routes, route)
		result.PutRows = append(result.PutRows, putResult)
	}

	if !request.SkipFlush {
		if err := request.Session.Flush(); err != nil {
			result.FlushProfile = request.Session.LastFlushProfile()
			result.Profile = profile.Snapshot()
			result.TotalElapsed = time.Since(startedAt)
			return result, diagnostics, err
		}
		result.FlushProfile = request.Session.LastFlushProfile()
	}
	result.Profile = profile.Snapshot()
	result.TotalElapsed = time.Since(startedAt)
	return result, diagnostics, nil
}
