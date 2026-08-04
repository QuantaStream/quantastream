package qsfixture

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

// MemoryBSIPrimaryKeyBackend is a deterministic in-memory fixture for the BSI
// primary-key backend contract. It does not implement bitmap storage; it
// preserves typed identity semantics for resolver and shadow-validation tests.
type MemoryBSIPrimaryKeyBackend struct {
	mu   sync.Mutex
	rows map[string]uint64
}

// NewMemoryBSIPrimaryKeyBackend returns an empty in-memory primary-key backend.
func NewMemoryBSIPrimaryKeyBackend() *MemoryBSIPrimaryKeyBackend {
	return &MemoryBSIPrimaryKeyBackend{rows: map[string]uint64{}}
}

// LookupPrimaryKey returns a staged rownum for the typed primary-key request.
func (b *MemoryBSIPrimaryKeyBackend) LookupPrimaryKey(req core.BSIPrimaryKeyLookupRequest) (core.BSIPrimaryKeyLookupResult, error) {
	if b == nil {
		return core.BSIPrimaryKeyLookupResult{}, fmt.Errorf("memory BSI primary key backend is nil")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	columnID, found := b.rows[memoryBSIPrimaryKeyLookupKey(req)]
	return core.BSIPrimaryKeyLookupResult{ColumnID: columnID, Found: found}, nil
}

// StagePrimaryKey records a typed primary-key mapping for future lookups.
func (b *MemoryBSIPrimaryKeyBackend) StagePrimaryKey(req core.BSIPrimaryKeyStageRequest) error {
	if b == nil {
		return fmt.Errorf("memory BSI primary key backend is nil")
	}
	if req.ColumnID == 0 {
		return fmt.Errorf("memory BSI primary key stage requires a non-zero column ID")
	}
	key := memoryBSIPrimaryKeyStageKey(req)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rows == nil {
		b.rows = map[string]uint64{}
	}
	if existing, found := b.rows[key]; found && existing != req.ColumnID {
		return fmt.Errorf("memory BSI primary key conflict for %s: existing column ID %d, staged column ID %d",
			req.RenderedValue, existing, req.ColumnID)
	}
	b.rows[key] = req.ColumnID
	return nil
}

// Snapshot returns a stable copy of the internal fixture map for tests.
func (b *MemoryBSIPrimaryKeyBackend) Snapshot() map[string]uint64 {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	copy := make(map[string]uint64, len(b.rows))
	for key, value := range b.rows {
		copy[key] = value
	}
	return copy
}

func memoryBSIPrimaryKeyLookupKey(req core.BSIPrimaryKeyLookupRequest) string {
	return memoryBSIPrimaryKeyKey(req.TableName, req.PrimaryKey, req.Attributes, req.Values, req.RenderedValue,
		req.ShardTimestamp)
}

func memoryBSIPrimaryKeyStageKey(req core.BSIPrimaryKeyStageRequest) string {
	return memoryBSIPrimaryKeyKey(req.TableName, req.PrimaryKey, req.Attributes, req.Values, req.RenderedValue,
		req.ShardTimestamp)
}

func memoryBSIPrimaryKeyKey(
	tableName string,
	primaryKey string,
	attributes []*core.Attribute,
	values []interface{},
	renderedValue string,
	shardTimestamp time.Time,
) string {
	var builder strings.Builder
	builder.WriteString(tableName)
	builder.WriteByte(0)
	builder.WriteString(primaryKey)
	builder.WriteByte(0)
	builder.WriteString(strconv.FormatInt(shardTimestamp.UnixNano(), 10))
	if len(values) == 0 {
		builder.WriteByte(0)
		builder.WriteString("rendered:string:")
		builder.WriteString(renderedValue)
		return builder.String()
	}
	for i, value := range values {
		builder.WriteByte(0)
		builder.WriteString(memoryBSIPrimaryKeyAttributeToken(attributes, i))
		builder.WriteByte('=')
		builder.WriteString(memoryBSIPrimaryKeyValueToken(value))
	}
	return builder.String()
}

func memoryBSIPrimaryKeyAttributeToken(attributes []*core.Attribute, index int) string {
	if index >= len(attributes) || attributes[index] == nil {
		return fmt.Sprintf("value%d", index)
	}
	attr := attributes[index]
	return attr.FieldName + ":" + attr.Type + ":" + attr.MappingStrategy
}

func memoryBSIPrimaryKeyValueToken(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "nil:"
	case time.Time:
		return "time.Time:" + strconv.FormatInt(typed.UnixNano(), 10)
	case []byte:
		return "[]byte:" + base64.StdEncoding.EncodeToString(typed)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}
