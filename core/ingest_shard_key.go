package core

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/shared"
)

// IngestShardKeyMode identifies the source of a router shard key.
type IngestShardKeyMode string

const (
	// IngestShardKeyExplicit uses a caller-provided shard key.
	IngestShardKeyExplicit IngestShardKeyMode = "explicit"
	// IngestShardKeyDedup uses source and event id so retries route together.
	IngestShardKeyDedup IngestShardKeyMode = "dedup"
	// IngestShardKeyPrimaryKey uses catalog primary-key fields from the payload.
	IngestShardKeyPrimaryKey IngestShardKeyMode = "primary_key"
)

// IngestShardKeyRequest describes the inputs available for deterministic
// streaming router placement.
type IngestShardKeyRequest struct {
	ExplicitShardKey string
	Source           string
	EventID          string
	Table            *Table
	Payload          map[string]interface{}
}

// IngestShardKeyResult captures both the selected key and why it was selected.
type IngestShardKeyResult struct {
	ShardKey string
	Mode     IngestShardKeyMode
	Fields   []string
}

// ResolveIngestShardKey centralizes router placement policy. It does not write
// dedup state; it only chooses the key used to route work to one session owner.
func ResolveIngestShardKey(request IngestShardKeyRequest) (IngestShardKeyResult, error) {
	if key := strings.TrimSpace(request.ExplicitShardKey); key != "" {
		return IngestShardKeyResult{ShardKey: key, Mode: IngestShardKeyExplicit}, nil
	}
	if request.Source != "" && request.EventID != "" {
		key, err := BuildIngestDedupKey(request.Source, request.EventID)
		if err != nil {
			return IngestShardKeyResult{}, err
		}
		return IngestShardKeyResult{ShardKey: key, Mode: IngestShardKeyDedup}, nil
	}
	return resolvePrimaryKeyIngestShardKey(request.Table, request.Payload)
}

func resolvePrimaryKeyIngestShardKey(table *Table, payload map[string]interface{}) (IngestShardKeyResult, error) {
	if table == nil || table.PrimaryKey == "" {
		return IngestShardKeyResult{}, fmt.Errorf("ingest shard key requires explicit key, event source/id, or table primary key")
	}
	if payload == nil {
		return IngestShardKeyResult{}, fmt.Errorf("ingest shard key primary-key mode requires payload")
	}
	fields := strings.Split(table.PrimaryKey, "+")
	tokens := make([]string, 0, len(fields))
	resolvedFields := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		value, err := shared.GetPath(field, payload, false, false)
		if err != nil {
			return IngestShardKeyResult{}, fmt.Errorf("ingest shard key primary-key field %q missing: %v", field, err)
		}
		encoded, err := canonicalIngestShardValue(value)
		if err != nil {
			return IngestShardKeyResult{}, fmt.Errorf("ingest shard key primary-key field %q unsupported: %v", field, err)
		}
		tokens = append(tokens, field+"="+encoded)
		resolvedFields = append(resolvedFields, field)
	}
	if len(tokens) == 0 {
		return IngestShardKeyResult{}, fmt.Errorf("ingest shard key primary-key mode found no fields")
	}
	return IngestShardKeyResult{
		ShardKey: "pk:" + table.Name + ":" + strings.Join(tokens, "|"),
		Mode:     IngestShardKeyPrimaryKey,
		Fields:   resolvedFields,
	}, nil
}

func canonicalIngestShardValue(value interface{}) (string, error) {
	var buf bytes.Buffer
	if err := writeCanonicalPayloadValue(&buf, value); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// BuildIngestDedupKey returns the colocated retry key for source/event id.
func BuildIngestDedupKey(source, eventID string) (string, error) {
	source = strings.TrimSpace(source)
	eventID = strings.TrimSpace(eventID)
	if source == "" {
		return "", fmt.Errorf("dedup source is required")
	}
	if eventID == "" {
		return "", fmt.Errorf("dedup event id is required")
	}
	return "dedup:" + source + ":" + eventID, nil
}

// IngestDedupRecord describes an observed dedup key/hash pair. It is a
// non-enforcing value object; storage and atomicity belong to the future final
// state-changing boundary.
type IngestDedupRecord struct {
	DedupKey    string
	PayloadHash uint64
}

// IngestDedupDecision describes the relationship between an incoming event and
// any existing dedup record.
type IngestDedupDecision string

const (
	IngestDedupNew       IngestDedupDecision = "new"
	IngestDedupDuplicate IngestDedupDecision = "duplicate"
	IngestDedupConflict  IngestDedupDecision = "conflict"
)

// ClassifyIngestDedup compares an incoming dedup key/hash with an optional
// existing record. It intentionally does not persist or reject anything.
func ClassifyIngestDedup(existing *IngestDedupRecord, incoming IngestDedupRecord) IngestDedupDecision {
	if existing == nil || existing.DedupKey != incoming.DedupKey {
		return IngestDedupNew
	}
	if existing.PayloadHash == incoming.PayloadHash {
		return IngestDedupDuplicate
	}
	return IngestDedupConflict
}
