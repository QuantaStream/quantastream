package qsbridge

// CursorID identifies an adapter-owned result cursor.
type CursorID string

// CursorState describes the lifecycle of a protocol-visible cursor.
type CursorState string

const (
	// CursorStateNone means no cursor is associated with the request.
	CursorStateNone CursorState = ""
	// CursorStateOpen means a forward-only cursor can still produce rows.
	CursorStateOpen CursorState = "open"
	// CursorStateExhausted means the cursor reached its final result chunk.
	CursorStateExhausted CursorState = "exhausted"
	// CursorStateClosed means an adapter closed the cursor.
	CursorStateClosed CursorState = "closed"
)

// CursorDescriptor is protocol-neutral cursor metadata.
//
// qsbridge records cursor intent and result progress. Future executors and
// protocol adapters own cursor storage, fetch mechanics, and close behavior.
type CursorDescriptor struct {
	ID        CursorID
	RequestID ExecutionRequestID
	Mode      CursorMode
	State     CursorState
	BatchSize int
	MaxRows   int
	Position  uint64
}

// CursorDescriptor returns the cursor metadata implied by request options.
func (r ExecutionRequest) CursorDescriptor() CursorDescriptor {
	return cursorDescriptorFromOptions(r.Options, r.Result.Kind)
}

// CursorDescriptor returns the cursor metadata implied by batch request options.
func (r BatchExecutionRequest) CursorDescriptor() CursorDescriptor {
	return cursorDescriptorFromOptions(r.Options, r.Result.Kind)
}

func cursorDescriptorFromOptions(options ExecutionOptions, resultKind ResultKind) CursorDescriptor {
	if resultKind == ResultStatement {
		return CursorDescriptor{}
	}
	if options.Cursor == CursorNone && !options.Streaming {
		return CursorDescriptor{}
	}
	mode := options.Cursor
	if mode == CursorNone {
		mode = CursorForwardOnly
	}
	return CursorDescriptor{
		ID:        CursorID(options.RequestID),
		RequestID: options.RequestID,
		Mode:      mode,
		State:     CursorStateOpen,
		BatchSize: options.BatchSize,
		MaxRows:   options.MaxRows,
	}
}

// Open reports whether the descriptor represents a cursor that may produce rows.
func (c CursorDescriptor) Open() bool {
	return c.State == CursorStateOpen
}

func cloneCursorDescriptor(cursor CursorDescriptor) CursorDescriptor {
	return cursor
}
