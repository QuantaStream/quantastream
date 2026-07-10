package qsbridge

import "sync"

// ExecutionRequestKind classifies one registered in-flight request.
type ExecutionRequestKind string

const (
	// ExecutionRequestSingle identifies one ordinary execution request.
	ExecutionRequestSingle ExecutionRequestKind = "single"
	// ExecutionRequestBatch identifies one batch execution request.
	ExecutionRequestBatch ExecutionRequestKind = "batch"
)

// ExecutionRecord is metadata for an adapter-owned in-flight request.
//
// It is intentionally descriptive only. qsbridge does not start goroutines,
// interrupt execution, enforce deadlines, or own connection/session state.
type ExecutionRecord struct {
	ID          ExecutionRequestID
	Kind        ExecutionRequestKind
	Status      ExecutionStatus
	Session     SessionContext
	Options     ExecutionOptions
	Request     ExecutionRequest
	Batch       BatchExecutionRequest
	Diagnostics DiagnosticSet
}

// ExecutionRegistry stores adapter-owned in-flight execution metadata.
type ExecutionRegistry interface {
	Register(request ExecutionRequest) ExecutionRecord
	RegisterBatch(request BatchExecutionRequest) ExecutionRecord
	Get(id ExecutionRequestID) (ExecutionRecord, bool)
	List() []ExecutionRecord
	MarkStatus(id ExecutionRequestID, status ExecutionStatus) bool
	Cancel(id ExecutionRequestID, reason CancellationReason, message string) CancellationRequest
	Remove(id ExecutionRequestID) bool
	Clear()
}

// MemoryExecutionRegistry is a lock-protected in-memory execution registry.
type MemoryExecutionRegistry struct {
	mu      sync.RWMutex
	records map[ExecutionRequestID]ExecutionRecord
}

// NewMemoryExecutionRegistry creates an empty in-memory execution registry.
func NewMemoryExecutionRegistry() *MemoryExecutionRegistry {
	return &MemoryExecutionRegistry{records: make(map[ExecutionRequestID]ExecutionRecord)}
}

// Register stores one single execution request by request id.
func (r *MemoryExecutionRegistry) Register(request ExecutionRequest) ExecutionRecord {
	record := executionRecordFromRequest(request)
	if r == nil || record.ID == "" {
		return record
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[record.ID] = cloneExecutionRecord(record)
	return cloneExecutionRecord(record)
}

// RegisterBatch stores one batch execution request by request id.
func (r *MemoryExecutionRegistry) RegisterBatch(request BatchExecutionRequest) ExecutionRecord {
	record := executionRecordFromBatchRequest(request)
	if r == nil || record.ID == "" {
		return record
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[record.ID] = cloneExecutionRecord(record)
	return cloneExecutionRecord(record)
}

// Get returns in-flight metadata by request id.
func (r *MemoryExecutionRegistry) Get(id ExecutionRequestID) (ExecutionRecord, bool) {
	if r == nil || id == "" {
		return ExecutionRecord{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.records[id]
	if !ok {
		return ExecutionRecord{}, false
	}
	return cloneExecutionRecord(record), true
}

// List returns all registered in-flight request metadata.
func (r *MemoryExecutionRegistry) List() []ExecutionRecord {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	records := make([]ExecutionRecord, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, cloneExecutionRecord(record))
	}
	return records
}

// MarkStatus updates the adapter-visible status for a registered request.
func (r *MemoryExecutionRegistry) MarkStatus(id ExecutionRequestID, status ExecutionStatus) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.records[id]
	if !ok {
		return false
	}
	record.Status = status
	r.records[id] = record
	return true
}

// Cancel creates cancellation metadata for a registered request.
func (r *MemoryExecutionRegistry) Cancel(id ExecutionRequestID, reason CancellationReason, message string) CancellationRequest {
	record, ok := r.Get(id)
	if !ok {
		return missingExecutionCancellation(id, reason, message)
	}
	cancel := newCancellationRequest(record.Options, reason, message, false)
	if cancel.Supported() {
		_ = r.MarkStatus(id, ExecutionCancelRequested)
	}
	return cancel
}

// Remove deletes registered metadata for a request id.
func (r *MemoryExecutionRegistry) Remove(id ExecutionRequestID) bool {
	if r == nil || id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[id]; !ok {
		return false
	}
	delete(r.records, id)
	return true
}

// Clear removes all registered execution metadata.
func (r *MemoryExecutionRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = make(map[ExecutionRequestID]ExecutionRecord)
}

func executionRecordFromRequest(request ExecutionRequest) ExecutionRecord {
	record := ExecutionRecord{
		ID:          request.Options.RequestID,
		Kind:        ExecutionRequestSingle,
		Status:      ExecutionPending,
		Session:     request.Bound.Prepared.Session.Clone(),
		Options:     request.Options,
		Request:     cloneExecutionRequest(request),
		Diagnostics: cloneDiagnosticSet(request.Diagnostics),
	}
	if record.ID == "" {
		record.Status = ExecutionFailed
		record.Diagnostics = append(record.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"execution registry requires a request id",
		))
	}
	if record.Diagnostics.BlocksNative() {
		record.Status = ExecutionFailed
	}
	return record
}

func executionRecordFromBatchRequest(request BatchExecutionRequest) ExecutionRecord {
	record := ExecutionRecord{
		ID:          request.Options.RequestID,
		Kind:        ExecutionRequestBatch,
		Status:      ExecutionPending,
		Session:     request.Prepared.Session.Clone(),
		Options:     request.Options,
		Batch:       cloneBatchExecutionRequest(request),
		Diagnostics: cloneDiagnosticSet(request.Diagnostics),
	}
	if record.ID == "" {
		record.Status = ExecutionFailed
		record.Diagnostics = append(record.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"execution registry requires a request id",
		))
	}
	if record.Diagnostics.BlocksNative() {
		record.Status = ExecutionFailed
	}
	return record
}

func missingExecutionCancellation(id ExecutionRequestID, reason CancellationReason, message string) CancellationRequest {
	if reason == "" {
		reason = CancellationClientRequest
	}
	return CancellationRequest{
		RequestID: id,
		Reason:    reason,
		Message:   message,
		Diagnostics: DiagnosticSet{ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"execution request is not registered",
		)},
	}
}

func cloneExecutionRecord(record ExecutionRecord) ExecutionRecord {
	record.Session = record.Session.Clone()
	record.Request = cloneExecutionRequest(record.Request)
	record.Batch = cloneBatchExecutionRequest(record.Batch)
	record.Diagnostics = cloneDiagnosticSet(record.Diagnostics)
	return record
}
