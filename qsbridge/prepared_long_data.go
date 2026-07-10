package qsbridge

import (
	"sort"
	"strconv"
	"sync"
)

// PreparedLongDataFragment describes one chunk of adapter-owned parameter data.
//
// qsbridge records the handle, parameter identity, value kind, and byte count so
// protocol adapters can validate long-data flows without retaining large payloads.
type PreparedLongDataFragment struct {
	Handle     PreparedStatementHandle
	Parameter  ParameterValue
	ChunkBytes uint64
	Final      bool
}

// PreparedLongDataState records accumulated long-data metadata for one parameter.
type PreparedLongDataState struct {
	Handle     PreparedStatementHandle
	Parameter  ParameterValue
	Chunks     uint64
	TotalBytes uint64
	Final      bool
}

// PreparedLongDataRegistry stores adapter-owned long parameter metadata.
type PreparedLongDataRegistry interface {
	Append(fragment PreparedLongDataFragment) (PreparedLongDataState, bool)
	Get(handle PreparedStatementHandle, parameter ParameterValue) (PreparedLongDataState, bool)
	List() []PreparedLongDataState
	ClearHandle(handle PreparedStatementHandle) bool
	Clear()
}

// MemoryPreparedLongDataRegistry is a lock-protected long-data metadata registry.
type MemoryPreparedLongDataRegistry struct {
	mu     sync.RWMutex
	states map[string]PreparedLongDataState
}

// NewMemoryPreparedLongDataRegistry creates an empty long-data registry.
func NewMemoryPreparedLongDataRegistry() *MemoryPreparedLongDataRegistry {
	return &MemoryPreparedLongDataRegistry{states: make(map[string]PreparedLongDataState)}
}

// Append records one long-data fragment and returns the accumulated state.
func (r *MemoryPreparedLongDataRegistry) Append(fragment PreparedLongDataFragment) (PreparedLongDataState, bool) {
	if r == nil || fragment.Handle.Empty() || parameterValueKey(fragment.Parameter) == "index:0" {
		return PreparedLongDataState{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	key := preparedLongDataKey(fragment.Handle, fragment.Parameter)
	state := r.states[key]
	if state.Handle.Empty() {
		state.Handle = fragment.Handle
		state.Parameter = fragment.Parameter
	}
	state.Chunks++
	state.TotalBytes += fragment.ChunkBytes
	if fragment.Final {
		state.Final = true
	}
	r.states[key] = clonePreparedLongDataState(state)
	return clonePreparedLongDataState(state), true
}

// Get returns accumulated long-data metadata for a handle and parameter.
func (r *MemoryPreparedLongDataRegistry) Get(handle PreparedStatementHandle, parameter ParameterValue) (PreparedLongDataState, bool) {
	if r == nil || handle.Empty() {
		return PreparedLongDataState{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.states[preparedLongDataKey(handle, parameter)]
	if !ok {
		return PreparedLongDataState{}, false
	}
	return clonePreparedLongDataState(state), true
}

// List returns accumulated long-data states ordered by handle and parameter.
func (r *MemoryPreparedLongDataRegistry) List() []PreparedLongDataState {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	states := make([]PreparedLongDataState, 0, len(r.states))
	for _, state := range r.states {
		states = append(states, clonePreparedLongDataState(state))
	}
	sort.Slice(states, func(i, j int) bool {
		return preparedLongDataKey(states[i].Handle, states[i].Parameter) < preparedLongDataKey(states[j].Handle, states[j].Parameter)
	})
	return states
}

// ClearHandle removes all long-data metadata for one prepared handle.
func (r *MemoryPreparedLongDataRegistry) ClearHandle(handle PreparedStatementHandle) bool {
	if r == nil || handle.Empty() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := false
	prefix := preparedLongDataHandleKey(handle) + "|"
	for key := range r.states {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(r.states, key)
			removed = true
		}
	}
	return removed
}

// Clear removes all long-data metadata.
func (r *MemoryPreparedLongDataRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = make(map[string]PreparedLongDataState)
}

func clonePreparedLongDataFragment(fragment PreparedLongDataFragment) PreparedLongDataFragment {
	return fragment
}

func clonePreparedLongDataState(state PreparedLongDataState) PreparedLongDataState {
	return state
}

func preparedLongDataKey(handle PreparedStatementHandle, parameter ParameterValue) string {
	return preparedLongDataHandleKey(handle) + "|" + parameterValueKey(parameter)
}

func preparedLongDataHandleKey(handle PreparedStatementHandle) string {
	if handle.ID != 0 {
		return "id:" + strconv.FormatUint(uint64(handle.ID), 10)
	}
	return "name:" + handle.Name
}
