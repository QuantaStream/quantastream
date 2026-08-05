package qsfixture

import (
	"fmt"
	"sync"

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
	key, err := memoryBSIPrimaryKeyLookupKey(req)
	if err != nil {
		return core.BSIPrimaryKeyLookupResult{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	columnID, found := b.rows[key]
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
	key, err := memoryBSIPrimaryKeyStageKey(req)
	if err != nil {
		return err
	}
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

func memoryBSIPrimaryKeyLookupKey(req core.BSIPrimaryKeyLookupRequest) (string, error) {
	if len(req.Identity) > 0 {
		return string(req.Identity), nil
	}
	encoded, err := core.EncodeBSIPrimaryKeyLookupIdentity(req)
	if err != nil {
		return "", fmt.Errorf("memory BSI primary key lookup encode error - %w", err)
	}
	return string(encoded), nil
}

func memoryBSIPrimaryKeyStageKey(req core.BSIPrimaryKeyStageRequest) (string, error) {
	if len(req.Identity) > 0 {
		return string(req.Identity), nil
	}
	encoded, err := core.EncodeBSIPrimaryKeyStageIdentity(req)
	if err != nil {
		return "", fmt.Errorf("memory BSI primary key stage encode error - %w", err)
	}
	return string(encoded), nil
}
