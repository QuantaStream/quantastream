package qsloader

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

// EnvelopeRequest is the loader-internal representation produced by protocol
// adapters before selector routing.
type EnvelopeRequest struct {
	Envelope      core.IngestEnvelope
	RouteOptions  core.IngestEnvelopeRouteOptions
	OriginalIndex int
}

// EventAdapter turns protocol-specific input into normalized ingest envelopes.
type EventAdapter interface {
	Decode(io.Reader) ([]EnvelopeRequest, error)
}

// JSONAdapter decodes the first streaming loader protocol: JSON over HTTP.
type JSONAdapter struct {
	DefaultSource string
	Now           func() time.Time
}

// Decode accepts a single JSON object, a JSON array, or an object containing an
// events/records array. Objects with a payload field are treated as normalized
// envelopes; other objects are treated as raw payloads.
func (a JSONAdapter) Decode(reader io.Reader) ([]EnvelopeRequest, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var raw interface{}
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	raw = normalizeJSONValue(raw)
	records, err := jsonRecords(raw)
	if err != nil {
		return nil, err
	}
	requests := make([]EnvelopeRequest, 0, len(records))
	for i, record := range records {
		req, err := a.decodeRecord(i, record)
		if err != nil {
			return nil, err
		}
		requests = append(requests, req)
	}
	return requests, nil
}

func (a JSONAdapter) decodeRecord(index int, record map[string]interface{}) (EnvelopeRequest, error) {
	payload := record
	envelopeLike := jsonRecordLooksLikeEnvelope(record)
	if envelopeLike {
		rawPayload, ok := record["payload"]
		if !ok || rawPayload == nil {
			return EnvelopeRequest{}, fmt.Errorf("json envelope record %d missing payload", index)
		}
		var okMap bool
		payload, okMap = rawPayload.(map[string]interface{})
		if !okMap {
			return EnvelopeRequest{}, fmt.Errorf("json envelope record %d payload must be an object", index)
		}
	}
	source := stringField(record, "source")
	if source == "" {
		source = strings.TrimSpace(a.DefaultSource)
	}
	eventTime, err := timeField(record, "event_time")
	if err != nil {
		return EnvelopeRequest{}, fmt.Errorf("json envelope record %d event_time: %w", index, err)
	}
	if eventTime.IsZero() && a.Now != nil {
		eventTime = a.Now().UTC()
	}
	headers, err := optionalMap(record, "headers")
	if err != nil {
		return EnvelopeRequest{}, fmt.Errorf("json envelope record %d headers: %w", index, err)
	}
	mode := core.IngestSourceStream
	if modeValue := strings.TrimSpace(stringField(record, "mode")); modeValue != "" {
		mode = core.IngestSourceMode(modeValue)
	}
	common := core.StreamIngestEnvelopeRequest{
		EventID:      stringField(record, "event_id"),
		Source:       source,
		EventTime:    eventTime,
		SourceOffset: stringField(record, "source_offset"),
		Payload:      payload,
		Headers:      headers,
	}
	var envelope core.IngestEnvelope
	switch mode {
	case core.IngestSourceBatch:
		envelope, err = core.NewBatchIngestEnvelope(core.BatchIngestEnvelopeRequest{
			Source:       common.Source,
			EventTime:    common.EventTime,
			SourceOffset: common.SourceOffset,
			Payload:      common.Payload,
			Headers:      common.Headers,
		})
	case core.IngestSourceStream:
		envelope, err = core.NewStreamIngestEnvelope(common)
	default:
		err = fmt.Errorf("unsupported mode %q", mode)
	}
	if err != nil {
		return EnvelopeRequest{}, fmt.Errorf("json envelope record %d: %w", index, err)
	}
	return EnvelopeRequest{
		Envelope: envelope,
		RouteOptions: core.IngestEnvelopeRouteOptions{
			ExplicitShardKey: stringField(record, "shard_key"),
			BuildShardKey:    stringField(record, "build_shard_key"),
		},
		OriginalIndex: index,
	}, nil
}

func jsonRecords(raw interface{}) ([]map[string]interface{}, error) {
	switch typed := raw.(type) {
	case []interface{}:
		return mapArrayRecords(typed)
	case map[string]interface{}:
		for _, key := range []string{"events", "records"} {
			if rawRecords, ok := typed[key]; ok {
				items, ok := rawRecords.([]interface{})
				if !ok {
					return nil, fmt.Errorf("%s must be an array", key)
				}
				return mapArrayRecords(items)
			}
		}
		return []map[string]interface{}{typed}, nil
	default:
		return nil, fmt.Errorf("json ingest body must be an object or array")
	}
}

func mapArrayRecords(items []interface{}) ([]map[string]interface{}, error) {
	records := make([]map[string]interface{}, 0, len(items))
	for i, item := range items {
		record, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("record %d must be an object", i)
		}
		records = append(records, record)
	}
	return records, nil
}

func normalizeJSONValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case json.Number:
		text := typed.String()
		if !strings.ContainsAny(text, ".eE") {
			if v, err := typed.Int64(); err == nil {
				return v
			}
		}
		if v, err := strconv.ParseFloat(text, 64); err == nil {
			return v
		}
		return text
	case []interface{}:
		for i := range typed {
			typed[i] = normalizeJSONValue(typed[i])
		}
		return typed
	case map[string]interface{}:
		for key, child := range typed {
			typed[key] = normalizeJSONValue(child)
		}
		return typed
	default:
		return value
	}
}

func jsonRecordLooksLikeEnvelope(record map[string]interface{}) bool {
	for _, key := range []string{"payload", "event_id", "event_time", "source_offset", "headers", "shard_key", "build_shard_key"} {
		if _, ok := record[key]; ok {
			return true
		}
	}
	if mode := stringField(record, "mode"); mode == string(core.IngestSourceBatch) || mode == string(core.IngestSourceStream) {
		return true
	}
	return false
}

func optionalMap(record map[string]interface{}, key string) (map[string]interface{}, error) {
	value, ok := record[key]
	if !ok || value == nil {
		return nil, nil
	}
	typed, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%s must be an object", key)
	}
	return typed, nil
}

func stringField(record map[string]interface{}, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func timeField(record map[string]interface{}, key string) (time.Time, error) {
	value, ok := record[key]
	if !ok || value == nil {
		return time.Time{}, nil
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return time.Time{}, nil
		}
		if parsed, ok := shared.ParseFastUTCTimestamp(typed); ok {
			return parsed.UTC(), nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return time.Time{}, err
		}
		return parsed.UTC(), nil
	case time.Time:
		return typed.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported value %T", value)
	}
}
