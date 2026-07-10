package qsbridge

import (
	"sort"
	"sync"
)

// SessionRegistry stores adapter-owned session metadata.
//
// It is a convenience scaffold for protocol adapters. qsbridge does not own
// network connections, transaction state, authentication storage, or session
// lifetimes.
type SessionRegistry interface {
	Put(session SessionContext) bool
	Get(id SessionID) (SessionContext, bool)
	List() []SessionContext
	Apply(transition SessionTransition) (SessionContext, bool)
	Remove(id SessionID) bool
	Clear()
}

// MemorySessionRegistry is a lock-protected in-memory session registry.
type MemorySessionRegistry struct {
	mu       sync.RWMutex
	sessions map[SessionID]SessionContext
}

// NewMemorySessionRegistry creates an empty session registry.
func NewMemorySessionRegistry() *MemorySessionRegistry {
	return &MemorySessionRegistry{sessions: make(map[SessionID]SessionContext)}
}

// Put stores one session by id.
func (r *MemorySessionRegistry) Put(session SessionContext) bool {
	if r == nil || session.ID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[session.ID] = session.Clone()
	return true
}

// Get returns a copied session by id.
func (r *MemorySessionRegistry) Get(id SessionID) (SessionContext, bool) {
	if r == nil || id == "" {
		return SessionContext{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[id]
	if !ok {
		return SessionContext{}, false
	}
	return session.Clone(), true
}

// List returns registered sessions ordered by session id.
func (r *MemorySessionRegistry) List() []SessionContext {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := make([]SessionContext, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session.Clone())
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].ID < sessions[j].ID
	})
	return sessions
}

// Apply stores the after-session from a supported transition.
func (r *MemorySessionRegistry) Apply(transition SessionTransition) (SessionContext, bool) {
	if r == nil || !transition.Supported() || transition.After.ID == "" {
		return SessionContext{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[transition.After.ID] = transition.After.Clone()
	return transition.After.Clone(), true
}

// Remove deletes one session by id.
func (r *MemorySessionRegistry) Remove(id SessionID) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[id]; !ok {
		return false
	}
	delete(r.sessions, id)
	return true
}

// Clear removes all sessions.
func (r *MemorySessionRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions = make(map[SessionID]SessionContext)
}
