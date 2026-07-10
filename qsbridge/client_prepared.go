package qsbridge

// ClientPrepareRequest is adapter metadata for preparing one SQL statement.
type ClientPrepareRequest struct {
	Connection  ConnectionContext
	PlanOptions ConnectionPlanOptions
	Handle      PreparedStatementHandle
	SQL         string
}

// ClientPrepareExchange is the metadata response for a prepare request.
//
// It may register the prepared plan in an adapter-provided registry, but it
// does not allocate protocol packets, execute statements, or mutate sessions.
type ClientPrepareExchange struct {
	Connection  ConnectionContext
	Request     ClientPrepareRequest
	Prepared    PreparedPlan
	Description PreparedPlanDescription
	Registered  bool
	Diagnostics DiagnosticSet
}

// ClientPreparedExecutionOptions are execution preferences for one prepared handle.
type ClientPreparedExecutionOptions struct {
	Mode    ProtocolExecutionMode
	Options ExecutionOptions
	Values  []ParameterValue
}

// ClientPreparedBatchExecutionOptions are execution preferences for a prepared batch.
type ClientPreparedBatchExecutionOptions struct {
	Options       ExecutionOptions
	ParameterSets []ParameterValueSet
}

// ClientPreparedExecutionExchange is the metadata response for executing a prepared handle.
type ClientPreparedExecutionExchange struct {
	Connection  ConnectionContext
	Handle      PreparedStatementHandle
	Prepared    PreparedPlan
	Handoff     ProtocolRoutedAuthorizedExecutionRequest
	Preview     ClientStatementResultPreview
	Response    ClientResponseItem
	Diagnostics DiagnosticSet
}

// ClientPreparedBatchExecutionExchange is the metadata response for executing a prepared batch.
type ClientPreparedBatchExecutionExchange struct {
	Connection  ConnectionContext
	Handle      PreparedStatementHandle
	Prepared    PreparedPlan
	Handoff     ProtocolRoutedAuthorizedBatchExecutionRequest
	Result      BatchExecutionResult
	Diagnostics DiagnosticSet
}

// ClientPreparedCloseExchange is the metadata response for closing a prepared handle.
type ClientPreparedCloseExchange struct {
	Connection  ConnectionContext
	Request     PreparedStatementCloseRequest
	Closed      bool
	Response    ClientResponseItem
	Diagnostics DiagnosticSet
}

// PrepareClientPreparedStatement prepares and optionally registers one statement.
func (s PlanningService) PrepareClientPreparedStatement(request ClientPrepareRequest, registry PreparedStatementRegistry) ClientPrepareExchange {
	result := ClientPrepareExchange{
		Connection:  cloneConnectionContext(request.Connection),
		Request:     cloneClientPrepareRequest(request),
		Diagnostics: cloneDiagnosticSet(request.Connection.Diagnostics),
	}
	if !request.Connection.Supported() {
		return result
	}

	prepared := s.PrepareWithRequest(request.Connection.PlanRequest(request.SQL, request.PlanOptions)).WithHandle(request.Handle)
	description := prepared.Description()
	registered := false
	if registry != nil && description.SupportedForPrepare() {
		description = registry.Register(prepared)
		prepared = prepared.WithHandle(description.Handle)
		registered = true
	}
	result.Prepared = clonePreparedPlan(prepared)
	result.Description = clonePreparedPlanDescription(description)
	result.Registered = registered
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, description.Diagnostics)
	return result
}

// ExecuteClientPreparedStatement prepares final metadata for executing a registered handle.
func (s PlanningService) ExecuteClientPreparedStatement(connection ConnectionContext, registry PreparedStatementRegistry, handle PreparedStatementHandle, options ClientPreparedExecutionOptions) ClientPreparedExecutionExchange {
	result := ClientPreparedExecutionExchange{
		Connection:  cloneConnectionContext(connection),
		Handle:      handle,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if registry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	prepared, ok := registry.Get(handle)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement handle not found"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if options.Mode == "" {
		options.Mode = ProtocolPreparedExecution
	}

	request := prepared.ExecutionRequest(options.Options, options.Values...)
	handoff := s.authorizeRouteAndNegotiate(
		prepared,
		request,
		connection.Protocol.NegotiateExecution(options.Mode, options.Options),
	)
	statement := ClientStatementText{Ordinal: 0, SQL: prepared.SQL}
	plan := ClientStatementPlan{
		Statement: statement,
		Request:   connection.PlanRequest(prepared.SQL, ConnectionPlanOptions{DefaultSchema: prepared.DefaultSchema, CatalogVersion: prepared.CatalogVersion, Scope: prepared.Scope}),
		Prepared:  clonePreparedPlan(prepared),
	}
	preview := ClientStatementHandoff{
		Statement: statement,
		Plan:      plan,
		Options: ClientStatementExecutionOptions{
			Ordinal: 0,
			Mode:    options.Mode,
			Options: options.Options,
			Values:  append([]ParameterValue(nil), options.Values...),
		},
		Handoff: handoff,
	}.resultPreview(connection.Protocol)

	result.Prepared = clonePreparedPlan(prepared)
	result.Handoff = handoff
	result.Preview = preview
	result.Response = preview.responseItem(connection.Protocol, 0, 1)
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, handoff.Diagnostics(), preview.Diagnostics())
	return result
}

// ExecuteClientPreparedBatchStatement prepares final metadata for executing a registered prepared batch.
func (s PlanningService) ExecuteClientPreparedBatchStatement(connection ConnectionContext, registry PreparedStatementRegistry, handle PreparedStatementHandle, options ClientPreparedBatchExecutionOptions) ClientPreparedBatchExecutionExchange {
	result := ClientPreparedBatchExecutionExchange{
		Connection:  cloneConnectionContext(connection),
		Handle:      handle,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		result.Result = errorClientBatchResult(options.Options, result.Diagnostics)
		return result
	}
	if registry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
		result.Result = errorClientBatchResult(options.Options, result.Diagnostics)
		return result
	}
	prepared, ok := registry.Get(handle)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement handle not found"),
		})
		result.Result = errorClientBatchResult(options.Options, result.Diagnostics)
		return result
	}

	request := prepared.BatchExecutionRequest(options.Options, options.ParameterSets...)
	handoff := s.authorizeRouteAndNegotiateBatch(
		prepared,
		request,
		connection.Protocol.NegotiateExecution(ProtocolBatchExecution, options.Options),
	)

	result.Prepared = clonePreparedPlan(prepared)
	result.Handoff = handoff
	result.Result = request.EmptyResult()
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, handoff.Diagnostics(), result.Result.Diagnostics)
	return result
}

// CloseClientPreparedStatement prepares final metadata for closing a prepared handle.
func (s PlanningService) CloseClientPreparedStatement(connection ConnectionContext, registry PreparedStatementRegistry, handle PreparedStatementHandle) ClientPreparedCloseExchange {
	request := PreparedPlan{}.WithHandle(handle).CloseRequest()
	result := ClientPreparedCloseExchange{
		Connection:  cloneConnectionContext(connection),
		Request:     clonePreparedStatementCloseRequest(request),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, request.Diagnostics),
	}
	if !connection.Supported() {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if !request.Supported() {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if registry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}

	response := preparedStatementResponse(connection.Protocol, "Prepared statement closed")
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, response.StatementResponse.Diagnostics)
	if response.Kind == ClientResponseError {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if !registry.Close(request) {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement handle not found"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	result.Closed = true
	result.Response = response
	result.Response.Outcome.Diagnostics = cloneDiagnosticSet(result.Diagnostics)
	return result
}

// Supported reports whether prepare metadata was produced successfully.
func (e ClientPrepareExchange) Supported() bool {
	return e.Connection.Supported() && e.Description.SupportedForPrepare() && !e.Diagnostics.BlocksNative()
}

// Supported reports whether prepared execution metadata can proceed.
func (e ClientPreparedExecutionExchange) Supported() bool {
	return e.Connection.Supported() && e.Handoff.Supported() && !e.Diagnostics.BlocksNative()
}

// Supported reports whether prepared batch execution metadata can proceed.
func (e ClientPreparedBatchExecutionExchange) Supported() bool {
	return e.Connection.Supported() && e.Handoff.Supported() && !e.Diagnostics.BlocksNative()
}

// Supported reports whether prepared close metadata can proceed.
func (e ClientPreparedCloseExchange) Supported() bool {
	return e.Connection.Supported() && e.Closed && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepare diagnostics into protocol-facing errors.
func (e ClientPrepareExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// ProtocolErrors converts prepared execution diagnostics into protocol-facing errors.
func (e ClientPreparedExecutionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// ProtocolErrors converts prepared batch execution diagnostics into protocol-facing errors.
func (e ClientPreparedBatchExecutionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// ProtocolErrors converts prepared close diagnostics into protocol-facing errors.
func (e ClientPreparedCloseExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepare error, if any.
func (e ClientPrepareExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// FirstProtocolError returns the first blocking prepared execution error, if any.
func (e ClientPreparedExecutionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// FirstProtocolError returns the first blocking prepared batch execution error, if any.
func (e ClientPreparedBatchExecutionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// FirstProtocolError returns the first blocking prepared close error, if any.
func (e ClientPreparedCloseExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func cloneClientPrepareRequest(request ClientPrepareRequest) ClientPrepareRequest {
	return ClientPrepareRequest{
		Connection:  cloneConnectionContext(request.Connection),
		PlanOptions: cloneConnectionPlanOptions(request.PlanOptions),
		Handle:      request.Handle,
		SQL:         request.SQL,
	}
}

func clonePreparedPlanDescription(description PreparedPlanDescription) PreparedPlanDescription {
	description.Parameters = append([]ParameterRef(nil), description.Parameters...)
	description.ResultColumns = append([]ResultColumn(nil), description.ResultColumns...)
	description.Statement = cloneStatementResult(description.Statement)
	description.Diagnostics = cloneDiagnosticSet(description.Diagnostics)
	return description
}

func clonePreparedStatementCloseRequest(request PreparedStatementCloseRequest) PreparedStatementCloseRequest {
	request.Diagnostics = cloneDiagnosticSet(request.Diagnostics)
	return request
}

func preparedStatementResponse(profile ProtocolProfile, status string) ClientResponseItem {
	result := ExecutionResult{
		Status:    ExecutionComplete,
		Kind:      ResultStatement,
		Statement: StatementResult{Status: status},
		Complete:  true,
	}
	statementResponse := result.ProtocolStatementResponse(profile)
	item := ClientResponseItem{
		Ordinal:           0,
		Kind:              ClientResponseStatement,
		Outcome:           ExecutionHandoffOutcome{Supported: statementResponse.Supported(), Diagnostics: cloneDiagnosticSet(statementResponse.Diagnostics)},
		Result:            cloneExecutionResult(result),
		StatementResponse: cloneProtocolStatementResponse(statementResponse),
		Final:             true,
	}
	if !statementResponse.Supported() {
		item.Kind = ClientResponseError
		item.Outcome.Kind = ExecutionHandoffRejected
		item.Errors = statementResponse.ProtocolErrors()
	}
	return item
}

func errorClientBatchResult(options ExecutionOptions, diagnostics DiagnosticSet) BatchExecutionResult {
	return BatchExecutionResult{
		RequestID:   options.RequestID,
		Status:      ExecutionFailed,
		Diagnostics: cloneDiagnosticSet(diagnostics),
		Complete:    true,
	}
}

func errorClientResponseItem(ordinal int, diagnostics DiagnosticSet) ClientResponseItem {
	return ClientResponseItem{
		Ordinal: ordinal,
		Kind:    ClientResponseError,
		Outcome: ExecutionHandoffOutcome{
			Kind:        ExecutionHandoffRejected,
			Supported:   false,
			Diagnostics: cloneDiagnosticSet(diagnostics),
		},
		Errors: diagnostics.ProtocolErrors(),
		Final:  true,
	}
}
