package qsbridge

import (
	"sort"
	"sync"
)

// PreparedStatementRegistry stores adapter-owned prepared statement handles.
//
// The registry is optional scaffolding for protocol adapters. It does not
// participate in planning, plan-cache identity, execution, or session storage.
type PreparedStatementRegistry interface {
	Register(plan PreparedPlan) PreparedPlanDescription
	Get(handle PreparedStatementHandle) (PreparedPlan, bool)
	List() []PreparedPlan
	Close(request PreparedStatementCloseRequest) bool
	Clear()
}

// MemoryPreparedStatementRegistry is a lock-protected in-memory registry.
type MemoryPreparedStatementRegistry struct {
	mu     sync.RWMutex
	nextID PreparedStatementID
	byID   map[PreparedStatementID]PreparedPlan
	byName map[string]PreparedStatementID
}

// NewMemoryPreparedStatementRegistry creates an empty prepared statement registry.
func NewMemoryPreparedStatementRegistry() *MemoryPreparedStatementRegistry {
	return &MemoryPreparedStatementRegistry{
		nextID: 1,
		byID:   make(map[PreparedStatementID]PreparedPlan),
		byName: make(map[string]PreparedStatementID),
	}
}

// Register stores a prepared plan and returns its protocol-neutral description.
func (r *MemoryPreparedStatementRegistry) Register(plan PreparedPlan) PreparedPlanDescription {
	if r == nil {
		return plan.Description()
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	handle := plan.Handle
	if handle.ID == 0 {
		handle.ID = r.nextStatementID()
	}
	plan = clonePreparedPlan(plan.WithHandle(handle))
	r.byID[handle.ID] = plan
	if handle.Name != "" {
		r.byName[handle.Name] = handle.ID
	}
	return plan.Description()
}

// Get returns a prepared plan by id or name.
func (r *MemoryPreparedStatementRegistry) Get(handle PreparedStatementHandle) (PreparedPlan, bool) {
	if r == nil || handle.Empty() {
		return PreparedPlan{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	id := handle.ID
	if id == 0 && handle.Name != "" {
		var ok bool
		id, ok = r.byName[handle.Name]
		if !ok {
			return PreparedPlan{}, false
		}
	}
	plan, ok := r.byID[id]
	if !ok {
		return PreparedPlan{}, false
	}
	return clonePreparedPlan(plan), true
}

// List returns registered prepared plans ordered by statement id.
func (r *MemoryPreparedStatementRegistry) List() []PreparedPlan {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	plans := make([]PreparedPlan, 0, len(r.byID))
	for _, plan := range r.byID {
		plans = append(plans, clonePreparedPlan(plan))
	}
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].Handle.ID < plans[j].Handle.ID
	})
	return plans
}

// Close removes a prepared plan by close request handle.
func (r *MemoryPreparedStatementRegistry) Close(request PreparedStatementCloseRequest) bool {
	if r == nil || !request.Supported() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	id := request.Handle.ID
	if id == 0 && request.Handle.Name != "" {
		var ok bool
		id, ok = r.byName[request.Handle.Name]
		if !ok {
			return false
		}
	}
	plan, ok := r.byID[id]
	if !ok {
		return false
	}
	delete(r.byID, id)
	if plan.Handle.Name != "" {
		delete(r.byName, plan.Handle.Name)
	}
	return true
}

// Clear removes all registered prepared statements.
func (r *MemoryPreparedStatementRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID = make(map[PreparedStatementID]PreparedPlan)
	r.byName = make(map[string]PreparedStatementID)
}

func (r *MemoryPreparedStatementRegistry) nextStatementID() PreparedStatementID {
	for {
		id := r.nextID
		r.nextID++
		if id != 0 {
			if _, exists := r.byID[id]; !exists {
				return id
			}
		}
	}
}
