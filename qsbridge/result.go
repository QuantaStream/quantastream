package qsbridge

// ResultCell is one protocol-neutral value in an executor result chunk.
type ResultCell struct {
	Kind  ValueKind
	Value any
}

// ResultRow is one protocol-neutral row.
type ResultRow []ResultCell

// ResultChunk is a batch of rows produced by a future executor.
type ResultChunk struct {
	Rows     []ResultRow
	Sequence int
	Final    bool
}

// ExecutionResult is the protocol-neutral response envelope for execution.
//
// It is a contract, not an executor. Future executors can populate chunks and
// profile data while MySQL/gRPC adapters can read one stable shape for query
// rows, OK metadata, diagnostics, and session actions.
type ExecutionResult struct {
	RequestID      ExecutionRequestID
	Status         ExecutionStatus
	Kind           ResultKind
	Columns        []ResultColumn
	Chunks         []ResultChunk
	Statement      StatementResult
	SessionActions []SessionAction
	Diagnostics    DiagnosticSet
	Cancellation   CancellationRequest
	Profile        ExecutionProfile
	Cursor         CursorDescriptor
	Complete       bool
	RowsReturned   uint64
}

// BatchExecutionResult is the protocol-neutral response envelope for batch execution.
//
// It groups one result envelope per parameter set under the shared batch
// request id. Future executors can fill item results incrementally; qsbridge
// only constructs and copies the metadata.
type BatchExecutionResult struct {
	RequestID      ExecutionRequestID
	Status         ExecutionStatus
	Kind           ResultKind
	Items          []ExecutionResult
	Diagnostics    DiagnosticSet
	Cancellation   CancellationRequest
	Complete       bool
	RowsReturned   uint64
	RowsAffected   uint64
	SessionActions []SessionAction
}

// EmptyResult creates an execution response envelope without row data.
func (r ExecutionRequest) EmptyResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:      r.Options.RequestID,
		Status:         ExecutionPending,
		Kind:           r.Result.Kind,
		Columns:        append([]ResultColumn(nil), r.ResultColumns...),
		Statement:      cloneStatementResult(r.Statement),
		SessionActions: cloneSessionActions(r.SessionActions),
		Diagnostics:    cloneDiagnosticSet(r.Diagnostics),
		Profile:        r.ExecutionProfile(),
		Cursor:         r.CursorDescriptor(),
		Complete:       r.Result.Kind == ResultStatement && !r.Diagnostics.BlocksNative(),
	}
	if result.Kind == "" && len(result.Columns) > 0 {
		result.Kind = ResultQuery
	}
	if result.Kind == "" && (result.Statement.Status != "" || len(result.SessionActions) > 0) {
		result.Kind = ResultStatement
		result.Complete = !result.Diagnostics.BlocksNative()
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
	} else if result.Complete {
		result.Status = ExecutionComplete
	}
	return result
}

// EmptyResult creates a batch execution response envelope without per-item data.
func (r BatchExecutionRequest) EmptyResult() BatchExecutionResult {
	result := BatchExecutionResult{
		RequestID:      r.Options.RequestID,
		Status:         ExecutionPending,
		Kind:           r.Result.Kind,
		Diagnostics:    cloneDiagnosticSet(r.Diagnostics),
		SessionActions: cloneSessionActions(r.SessionActions),
		Complete:       len(r.ParameterSets) == 0 && !r.Diagnostics.BlocksNative(),
	}
	if result.Kind == "" {
		result.Kind = r.Result.Kind
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = false
	} else if result.Complete {
		result.Status = ExecutionComplete
	}
	return result
}

// WithChunk returns a copy of result with chunk appended.
func (r ExecutionResult) WithChunk(chunk ResultChunk) ExecutionResult {
	r.Chunks = append(append([]ResultChunk(nil), r.Chunks...), cloneResultChunk(chunk))
	for _, row := range chunk.Rows {
		if row != nil {
			r.RowsReturned++
		}
	}
	if chunk.Final {
		r.Complete = true
		r.Status = ExecutionComplete
		if r.Cursor.State == CursorStateOpen {
			r.Cursor.State = CursorStateExhausted
		}
	} else if len(chunk.Rows) > 0 && !r.Complete {
		r.Status = ExecutionStreaming
	}
	if r.Cursor.State == CursorStateOpen {
		r.Cursor.Position = r.RowsReturned
	}
	return r
}

// WithProjectedRowSet returns a copy of result with a projected row set appended as a chunk.
func (r ExecutionResult) WithProjectedRowSet(rowSet QuantaProjectedRowSet, sequence int, final bool) ExecutionResult {
	chunk, diagnostics := rowSet.ToResultChunk(sequence, final)
	if diagnostics.BlocksNative() {
		r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, diagnostics)
		r.Status = ExecutionFailed
		r.Complete = true
		return r
	}
	return r.WithChunk(chunk)
}

// WithItem returns a copy of the batch result with one item result appended.
func (r BatchExecutionResult) WithItem(item ExecutionResult) BatchExecutionResult {
	r.Items = append(append([]ExecutionResult(nil), r.Items...), cloneExecutionResult(item))
	r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, item.Diagnostics)
	r.RowsReturned += item.RowsReturned
	r.RowsAffected += item.Statement.AffectedRows
	r.SessionActions = mergeSessionActions(r.SessionActions, item.SessionActions)
	if item.Status == ExecutionFailed || item.Diagnostics.BlocksNative() {
		r.Status = ExecutionFailed
		r.Complete = true
		return r
	}
	if item.Status == ExecutionCanceled {
		r.Status = ExecutionCanceled
		r.Complete = true
		return r
	}
	if item.Status == ExecutionStreaming {
		r.Status = ExecutionStreaming
		return r
	}
	if r.Status == "" || r.Status == ExecutionPending {
		r.Status = ExecutionPending
	}
	return r
}

// WithComplete returns a copy of the batch result marked complete when no item failed.
func (r BatchExecutionResult) WithComplete() BatchExecutionResult {
	if r.Diagnostics.BlocksNative() || r.Status == ExecutionFailed || r.Status == ExecutionCanceled {
		if r.Status == "" {
			r.Status = ExecutionFailed
		}
		r.Complete = true
		return r
	}
	r.Status = ExecutionComplete
	r.Complete = true
	return r
}

// WithCancellation returns a copy of result marked by cancellation metadata.
func (r ExecutionResult) WithCancellation(cancel CancellationRequest) ExecutionResult {
	r.Cancellation = cloneCancellationRequest(cancel)
	r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, cancel.Diagnostics)
	if cancel.Supported() {
		r.Status = ExecutionCanceled
		r.Complete = true
		return r
	}
	if r.Diagnostics.BlocksNative() {
		r.Status = ExecutionFailed
	}
	return r
}

// WithCancellation returns a copy of batch result marked by cancellation metadata.
func (r BatchExecutionResult) WithCancellation(cancel CancellationRequest) BatchExecutionResult {
	r.Cancellation = cloneCancellationRequest(cancel)
	r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, cancel.Diagnostics)
	if cancel.Supported() {
		r.Status = ExecutionCanceled
		r.Complete = true
		return r
	}
	if r.Diagnostics.BlocksNative() {
		r.Status = ExecutionFailed
	}
	return r
}

// WithProfile returns a copy of result with profile metadata attached.
func (r ExecutionResult) WithProfile(profile ExecutionProfile) ExecutionResult {
	r.Profile = cloneExecutionProfile(profile)
	return r
}

// Supported reports whether the result envelope has no blocking diagnostics.
func (r ExecutionResult) Supported() bool {
	return !r.Diagnostics.BlocksNative()
}

// Supported reports whether the batch result envelope has no blocking diagnostics.
func (r BatchExecutionResult) Supported() bool {
	return !r.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch result diagnostics into adapter-facing error metadata.
func (r BatchExecutionResult) ProtocolErrors() []ProtocolError {
	return r.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch result error, if any.
func (r BatchExecutionResult) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics.FirstProtocolError()
}

func cloneExecutionResult(result ExecutionResult) ExecutionResult {
	result.Columns = append([]ResultColumn(nil), result.Columns...)
	result.Chunks = cloneResultChunks(result.Chunks)
	result.Statement = cloneStatementResult(result.Statement)
	result.SessionActions = cloneSessionActions(result.SessionActions)
	result.Diagnostics = cloneDiagnosticSet(result.Diagnostics)
	result.Cancellation = cloneCancellationRequest(result.Cancellation)
	result.Profile = cloneExecutionProfile(result.Profile)
	return result
}

func cloneResultChunks(chunks []ResultChunk) []ResultChunk {
	if len(chunks) == 0 {
		return nil
	}
	cloned := make([]ResultChunk, 0, len(chunks))
	for _, chunk := range chunks {
		cloned = append(cloned, cloneResultChunk(chunk))
	}
	return cloned
}

func mergeSessionActions(left, right []SessionAction) []SessionAction {
	merged := cloneSessionActions(left)
	merged = append(merged, cloneSessionActions(right)...)
	return merged
}

func cloneResultChunk(chunk ResultChunk) ResultChunk {
	cloned := chunk
	cloned.Rows = make([]ResultRow, 0, len(chunk.Rows))
	for _, row := range chunk.Rows {
		cloned.Rows = append(cloned.Rows, cloneResultRow(row))
	}
	return cloned
}

func cloneResultRow(row ResultRow) ResultRow {
	if row == nil {
		return nil
	}
	return append(ResultRow(nil), row...)
}

func cloneCancellationRequest(request CancellationRequest) CancellationRequest {
	request.Diagnostics = cloneDiagnosticSet(request.Diagnostics)
	return request
}
