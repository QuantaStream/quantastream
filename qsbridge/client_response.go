package qsbridge

// ClientResponseKind identifies the protocol response shape for one statement.
type ClientResponseKind string

const (
	// ClientResponseQuery means the adapter should expect row metadata and rows.
	ClientResponseQuery ClientResponseKind = "query"
	// ClientResponseStatement means the adapter should emit OK/status metadata.
	ClientResponseStatement ClientResponseKind = "statement"
	// ClientResponseError means the adapter should emit an error response.
	ClientResponseError ClientResponseKind = "error"
)

// ClientResponseFlag records adapter-facing response sequence traits.
type ClientResponseFlag string

const (
	// ClientResponseFlagMoreResults means another response item follows.
	ClientResponseFlagMoreResults ClientResponseFlag = "more_results"
	// ClientResponseFlagFinal means this item is the final response in a sequence.
	ClientResponseFlagFinal ClientResponseFlag = "final"
	// ClientResponseFlagQuery means this item carries row schema metadata.
	ClientResponseFlagQuery ClientResponseFlag = "query"
	// ClientResponseFlagStatement means this item carries OK/status metadata.
	ClientResponseFlagStatement ClientResponseFlag = "statement"
	// ClientResponseFlagError means this item carries protocol-facing errors.
	ClientResponseFlagError ClientResponseFlag = "error"
	// ClientResponseFlagStreaming means this item represents an incomplete streaming result.
	ClientResponseFlagStreaming ClientResponseFlag = "streaming"
	// ClientResponseFlagComplete means this item represents a complete result.
	ClientResponseFlagComplete ClientResponseFlag = "complete"
	// ClientResponseFlagCursorOpen means this item has an open cursor.
	ClientResponseFlagCursorOpen ClientResponseFlag = "cursor_open"
	// ClientResponseFlagCursorExhausted means this item's cursor is exhausted.
	ClientResponseFlagCursorExhausted ClientResponseFlag = "cursor_exhausted"
)

// ClientResponseItem is one ordered protocol response descriptor.
//
// It is intentionally metadata-only. Query items may carry schema metadata,
// statement items may carry OK/status metadata, and error items carry
// protocol-facing errors.
type ClientResponseItem struct {
	Ordinal           int
	Statement         ClientStatementText
	Kind              ClientResponseKind
	Outcome           ExecutionHandoffOutcome
	Result            ExecutionResult
	Schema            ProtocolResultSchema
	StatementResponse ProtocolStatementResponse
	Errors            []ProtocolError
	MoreResults       bool
	Final             bool
	Flags             []ClientResponseFlag
}

// ClientResponseSequence is the ordered set of response descriptors for a client exchange.
type ClientResponseSequence struct {
	Connection  ConnectionContext
	Items       []ClientResponseItem
	Diagnostics DiagnosticSet
}

// ResponseSequence returns ordered protocol response descriptors for this exchange.
func (e ClientExchange) ResponseSequence() ClientResponseSequence {
	sequence := ClientResponseSequence{
		Connection:  cloneConnectionContext(e.Connection),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if len(e.Preview.Statements) == 0 {
		return sequence
	}
	sequence.Items = make([]ClientResponseItem, 0, len(e.Preview.Statements))
	for i, preview := range e.Preview.Statements {
		item := preview.responseItem(e.Connection.Protocol, i, len(e.Preview.Statements))
		sequence.Items = append(sequence.Items, item)
		sequence.Diagnostics = mergeDiagnosticSets(sequence.Diagnostics, preview.Diagnostics())
	}
	return sequence
}

// Supported reports whether the response sequence has no blocking diagnostics.
func (s ClientResponseSequence) Supported() bool {
	return s.Connection.Supported() && !s.Diagnostics.BlocksNative()
}

// ProtocolErrors converts sequence diagnostics into protocol-facing errors.
func (s ClientResponseSequence) ProtocolErrors() []ProtocolError {
	return s.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking sequence error, if any.
func (s ClientResponseSequence) FirstProtocolError() (ProtocolError, bool) {
	return s.Diagnostics.FirstProtocolError()
}

func (p ClientStatementResultPreview) responseItem(profile ProtocolProfile, index, total int) ClientResponseItem {
	item := ClientResponseItem{
		Ordinal:     p.Statement.Ordinal,
		Statement:   p.Statement,
		Outcome:     p.Outcome,
		Result:      cloneExecutionResult(p.Result),
		MoreResults: index < total-1,
		Final:       index == total-1,
	}
	if !p.Outcome.Supported || p.Diagnostics().BlocksNative() {
		item.Kind = ClientResponseError
		item.Errors = p.Diagnostics().ProtocolErrors()
		item.Flags = clientResponseFlags(item)
		return item
	}
	if p.HasSchema {
		item.Kind = ClientResponseQuery
		item.Schema = cloneProtocolResultSchema(p.Schema)
		item.Flags = clientResponseFlags(item)
		return item
	}
	if p.HasStatementResponse {
		item.Kind = ClientResponseStatement
		item.StatementResponse = cloneProtocolStatementResponse(p.StatementResponse)
		item.Flags = clientResponseFlags(item)
		return item
	}
	item.Kind = ClientResponseStatement
	item.StatementResponse = p.Result.ProtocolStatementResponse(profile)
	item.Flags = clientResponseFlags(item)
	return item
}

func clientResponseFlags(item ClientResponseItem) []ClientResponseFlag {
	flags := make([]ClientResponseFlag, 0, 6)
	if item.MoreResults {
		flags = append(flags, ClientResponseFlagMoreResults)
	}
	if item.Final {
		flags = append(flags, ClientResponseFlagFinal)
	}
	switch item.Kind {
	case ClientResponseQuery:
		flags = append(flags, ClientResponseFlagQuery)
	case ClientResponseStatement:
		flags = append(flags, ClientResponseFlagStatement)
	case ClientResponseError:
		flags = append(flags, ClientResponseFlagError)
	}
	switch item.Result.Status {
	case ExecutionStreaming:
		flags = append(flags, ClientResponseFlagStreaming)
	case ExecutionComplete:
		flags = append(flags, ClientResponseFlagComplete)
	}
	switch item.Result.Cursor.State {
	case CursorStateOpen:
		flags = append(flags, ClientResponseFlagCursorOpen)
	case CursorStateExhausted:
		flags = append(flags, ClientResponseFlagCursorExhausted)
	}
	return flags
}

func cloneProtocolResultSchema(schema ProtocolResultSchema) ProtocolResultSchema {
	cloned := ProtocolResultSchema{
		Profile: schema.Profile.Clone(),
		Columns: make([]ProtocolColumn, 0, len(schema.Columns)),
	}
	for _, column := range schema.Columns {
		column.Flags = append([]ProtocolColumnFlag(nil), column.Flags...)
		cloned.Columns = append(cloned.Columns, column)
	}
	return cloned
}

func cloneProtocolStatementResponse(response ProtocolStatementResponse) ProtocolStatementResponse {
	response.Profile = response.Profile.Clone()
	response.Notices = cloneStatementNotices(response.Notices)
	response.SessionActions = cloneSessionActions(response.SessionActions)
	response.Flags = append([]ProtocolStatusFlag(nil), response.Flags...)
	response.Diagnostics = cloneDiagnosticSet(response.Diagnostics)
	return response
}
