package qsbridge

import (
	"sort"
	"sync"
)

// CursorRegistry stores adapter-owned cursor metadata.
type CursorRegistry interface {
	Open(cursor CursorDescriptor) (CursorDescriptor, bool)
	Get(id CursorID) (CursorDescriptor, bool)
	List() []CursorDescriptor
	Advance(id CursorID, rows uint64, final bool) (CursorDescriptor, bool)
	Close(id CursorID) (CursorDescriptor, bool)
	Remove(id CursorID) bool
	Clear()
}

// MemoryCursorRegistry is a lock-protected in-memory cursor metadata registry.
type MemoryCursorRegistry struct {
	mu      sync.RWMutex
	cursors map[CursorID]CursorDescriptor
}

// NewMemoryCursorRegistry creates an empty cursor registry.
func NewMemoryCursorRegistry() *MemoryCursorRegistry {
	return &MemoryCursorRegistry{cursors: make(map[CursorID]CursorDescriptor)}
}

// Open stores an open cursor descriptor.
func (r *MemoryCursorRegistry) Open(cursor CursorDescriptor) (CursorDescriptor, bool) {
	if r == nil || cursor.ID == "" || cursor.State != CursorStateOpen {
		return CursorDescriptor{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cursors[cursor.ID] = cloneCursorDescriptor(cursor)
	return cloneCursorDescriptor(cursor), true
}

// Get returns cursor metadata by cursor id.
func (r *MemoryCursorRegistry) Get(id CursorID) (CursorDescriptor, bool) {
	if r == nil || id == "" {
		return CursorDescriptor{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	cursor, ok := r.cursors[id]
	if !ok {
		return CursorDescriptor{}, false
	}
	return cloneCursorDescriptor(cursor), true
}

// List returns cursor metadata ordered by cursor id.
func (r *MemoryCursorRegistry) List() []CursorDescriptor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	cursors := make([]CursorDescriptor, 0, len(r.cursors))
	for _, cursor := range r.cursors {
		cursors = append(cursors, cloneCursorDescriptor(cursor))
	}
	sort.Slice(cursors, func(i, j int) bool {
		return cursors[i].ID < cursors[j].ID
	})
	return cursors
}

// Advance updates cursor position and optionally marks it exhausted.
func (r *MemoryCursorRegistry) Advance(id CursorID, rows uint64, final bool) (CursorDescriptor, bool) {
	if r == nil || id == "" {
		return CursorDescriptor{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cursor, ok := r.cursors[id]
	if !ok || cursor.State == CursorStateClosed {
		return CursorDescriptor{}, false
	}
	cursor.Position += rows
	if final {
		cursor.State = CursorStateExhausted
	}
	r.cursors[id] = cursor
	return cloneCursorDescriptor(cursor), true
}

// Close marks a cursor closed without removing its metadata.
func (r *MemoryCursorRegistry) Close(id CursorID) (CursorDescriptor, bool) {
	if r == nil || id == "" {
		return CursorDescriptor{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cursor, ok := r.cursors[id]
	if !ok {
		return CursorDescriptor{}, false
	}
	cursor.State = CursorStateClosed
	r.cursors[id] = cursor
	return cloneCursorDescriptor(cursor), true
}

// Remove deletes cursor metadata.
func (r *MemoryCursorRegistry) Remove(id CursorID) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.cursors[id]; !ok {
		return false
	}
	delete(r.cursors, id)
	return true
}

// Clear removes all cursor metadata.
func (r *MemoryCursorRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cursors = make(map[CursorID]CursorDescriptor)
}
