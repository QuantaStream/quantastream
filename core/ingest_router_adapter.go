package core

import (
	"fmt"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// IngestRecordEnqueuer is the narrow SessionRouter surface needed by the
// selector-to-router adapter.
type IngestRecordEnqueuer interface {
	Enqueue(IngestRecord) error
}

// IngestRouteRequest describes a parsed stream event ready for selector
// evaluation and router placement.
type IngestRouteRequest struct {
	Tables           []*Table
	Envelope         map[string]interface{}
	Payload          map[string]interface{}
	ExplicitShardKey string
	BuildShardKey    string
	EventID          string
	Source           string
	EventTime        time.Time
	SourceOffset     string
	PayloadHash      uint64
	DedupTTL         time.Duration
}

// IngestRouteResult captures the selected table, selected shard key, and
// resulting transport-neutral ingest record.
type IngestRouteResult struct {
	Selector      IngestSelectorResult
	ShardKey      IngestShardKeyResult
	BuildShardKey string
	Record        IngestRecord
	Enqueued      bool
}

// BuildSelectedIngestRecord evaluates table selectors and builds the
// IngestRecord that would be sent to the router. It does not enqueue.
func BuildSelectedIngestRecord(request IngestRouteRequest) (IngestRouteResult, qsbridge.DiagnosticSet, error) {
	selector, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables:   request.Tables,
		Envelope: request.Envelope,
		Payload:  request.Payload,
	})
	if diagnostics.BlocksNative() {
		return IngestRouteResult{Selector: selector}, diagnostics, nil
	}
	if !selector.Matched || selector.Table == nil {
		return IngestRouteResult{Selector: selector}, nil, fmt.Errorf("no ingest selector matched payload")
	}
	source := firstNonEmpty(request.Source, stringFromMap(request.Envelope, "source"))
	eventID := firstNonEmpty(request.EventID, stringFromMap(request.Envelope, "event_id"))
	shardKey, err := ResolveIngestShardKey(IngestShardKeyRequest{
		ExplicitShardKey: request.ExplicitShardKey,
		Source:           source,
		EventID:          eventID,
		Table:            selector.Table,
		Payload:          request.Payload,
	})
	if err != nil {
		return IngestRouteResult{Selector: selector}, nil, err
	}
	return IngestRouteResult{
		Selector:      selector,
		ShardKey:      shardKey,
		BuildShardKey: request.BuildShardKey,
		Record: IngestRecord{
			TableName:     selector.TableName,
			Data:          request.Payload,
			ShardKey:      shardKey.ShardKey,
			BuildShardKey: request.BuildShardKey,
			EventID:       eventID,
			Source:        source,
			EventTime:     request.EventTime,
			SourceOffset:  request.SourceOffset,
			PayloadHash:   request.PayloadHash,
			DedupTTL:      request.DedupTTL,
		},
	}, nil, nil
}

// RouteSelectedIngestRecord builds and enqueues one selected ingest record.
func RouteSelectedIngestRecord(enqueuer IngestRecordEnqueuer, request IngestRouteRequest) (IngestRouteResult, qsbridge.DiagnosticSet, error) {
	if enqueuer == nil {
		return IngestRouteResult{}, nil, fmt.Errorf("ingest record enqueuer is required")
	}
	result, diagnostics, err := BuildSelectedIngestRecord(request)
	if diagnostics.BlocksNative() || err != nil {
		return result, diagnostics, err
	}
	if err := enqueuer.Enqueue(result.Record); err != nil {
		return result, nil, err
	}
	result.Enqueued = true
	return result, nil, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func stringFromMap(values map[string]interface{}, key string) string {
	if values == nil {
		return ""
	}
	value, ok := values[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}
