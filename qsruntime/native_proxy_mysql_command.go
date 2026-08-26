package qsruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// NativeProxyMySQLCommandHandler adapts decoded MySQL commands to the native proxy front door.
type NativeProxyMySQLCommandHandler struct {
	FrontDoor NativeProxyFrontDoor
	Options   qsbridge.ExecutionOptions
	Profile   *NativeProxyMySQLSessionProfile
}

// HandleCommand handles one decoded MySQL command through the native proxy front door.
func (h NativeProxyMySQLCommandHandler) HandleCommand(ctx context.Context, command qsmysql.Command) (qsmysql.CommandResponse, error) {
	return h.handleMySQLCommand(ctx, command)
}

// ServeMySQLCommand reads, handles, and writes one MySQL command through packet interfaces.
func (f NativeProxyFrontDoor) ServeMySQLCommand(ctx context.Context, reader qsmysql.PacketReader, writer qsmysql.PacketWriter, options qsbridge.ExecutionOptions) (qsmysql.CommandResponse, error) {
	return (qsmysql.CommandLoop{
		Reader:        reader,
		Writer:        writer,
		Handler:       NativeProxyMySQLCommandHandler{FrontDoor: f, Options: options, Profile: NewNativeProxyMySQLSessionProfile()},
		CommandLogger: f.MySQLCommandLogger,
	}).ServeNext(ctx)
}

// HandleMySQLCommand returns the socket-free MySQL packet response for one decoded command.
func (f NativeProxyFrontDoor) HandleMySQLCommand(ctx context.Context, command qsmysql.Command, options qsbridge.ExecutionOptions) (qsmysql.CommandResponse, error) {
	return (NativeProxyMySQLCommandHandler{FrontDoor: f, Options: options}).handleMySQLCommand(ctx, command)
}

func (h NativeProxyMySQLCommandHandler) handleMySQLCommand(ctx context.Context, command qsmysql.Command) (qsmysql.CommandResponse, error) {
	switch command.Kind {
	case qsmysql.CommandKindPing:
		return qsmysql.PingResponse(), nil
	case qsmysql.CommandKindQuit:
		return qsmysql.QuitResponse(), nil
	case qsmysql.CommandKindFieldList:
		return h.handleMySQLFieldList(ctx, command)
	case qsmysql.CommandKindStmtPrepare:
		return h.handleMySQLPreparedStatementPrepare(command)
	case qsmysql.CommandKindStmtExecute:
		return h.handleMySQLPreparedStatementExecute(ctx, command)
	case qsmysql.CommandKindStmtSendLongData:
		return h.handleMySQLPreparedStatementLongData(command)
	case qsmysql.CommandKindStmtClose:
		h.sessionProfile().ClosePreparedStatement(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)})
		return qsmysql.NoResponse(), nil
	case qsmysql.CommandKindStmtReset:
		profile := h.sessionProfile()
		handle := qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)}
		if _, ok := profile.PreparedStatements().Get(handle); !ok {
			return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(nativeProxyMySQLUnknownPreparedStatement(command.StatementID, "reset")), nil, true)
		}
		profile.ClearPreparedLongData(handle)
		return h.rememberMySQLCommandResponse(qsmysql.StatementOKResponse(qsbridge.StatementResult{}), nil, true)
	case qsmysql.CommandKindQuery:
		profile := h.sessionProfile()
		if response, ok, err := nativeProxyMySQLDiagnosticQueryResponse(command, profile); ok || err != nil {
			return response, err
		}
		if response, ok, err := nativeProxyMySQLMetadataQueryResponse(command); ok || err != nil {
			return response, err
		}
		if response, ok, err := nativeProxyMySQLProfileQueryResponse(command, profile); ok || err != nil {
			return response, err
		}
		result, err := h.FrontDoor.Server.ExecuteSQLWithSession(ctx, nativeProxyMySQLSessionForCommand(profile, command), command.SQL, h.Options)
		if err != nil {
			return h.rememberMySQLCommandResponse(qsmysql.ErrorResponseFromError(err), nil, true)
		}
		profile.Store(result.Instrumentation)
		result.Runtime.Diagnostics = append(result.Runtime.Diagnostics, profile.ApplySessionActions(nativeProxySessionActions(result))...)
		response, responseErr := nativeProxyMySQLResponseFromSQLResult(result, command.Database)
		return h.rememberMySQLCommandResponse(response, responseErr, true)
	default:
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(qsmysql.ProtocolErrorFromError(nil)), nil, true)
	}
}

func (h NativeProxyMySQLCommandHandler) handleMySQLFieldList(ctx context.Context, command qsmysql.Command) (qsmysql.CommandResponse, error) {
	table := strings.TrimSpace(command.Table)
	if table == "" {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateBaseTableNotFound,
			VendorCode: 1146,
			Message:    "COM_FIELD_LIST requires table name",
		}), nil, true)
	}
	profile := h.sessionProfile()
	sql := "select * from " + nativeProxyMySQLQuoteIdentifierPath(table) + " limit 0"
	result, err := h.FrontDoor.Server.ExecuteSQLWithSession(ctx, nativeProxyMySQLSessionForCommand(profile, command), sql, h.Options)
	if err != nil {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponseFromError(err), nil, true)
	}
	profile.Store(result.Instrumentation)
	result.Runtime.Diagnostics = append(result.Runtime.Diagnostics, profile.ApplySessionActions(nativeProxySessionActions(result))...)
	response, responseErr := nativeProxyMySQLFieldListResponseFromSQLResult(result, command.Database, command.FieldPattern)
	return h.rememberMySQLCommandResponse(response, responseErr, true)
}

func (h NativeProxyMySQLCommandHandler) handleMySQLPreparedStatementPrepare(command qsmysql.Command) (qsmysql.CommandResponse, error) {
	profile := h.sessionProfile()
	prepared, diagnostics := h.FrontDoor.Server.PrepareSQLWithSession(command.SQL, nativeProxyMySQLSessionForCommand(profile, command))
	if protocolError, ok := diagnostics.FirstProtocolError(); ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(protocolError), nil, true)
	}
	description := h.preparedStatementRegistry().Register(prepared)
	if protocolError, ok := description.Diagnostics.FirstProtocolError(); ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(protocolError), nil, true)
	}
	response, err := qsmysql.PreparedStatementResponse(description)
	return h.rememberMySQLCommandResponse(response, err, true)
}

func (h NativeProxyMySQLCommandHandler) handleMySQLPreparedStatementExecute(ctx context.Context, command qsmysql.Command) (qsmysql.CommandResponse, error) {
	profile := h.sessionProfile()
	handle := qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)}
	prepared, ok := profile.PreparedStatements().Get(handle)
	if !ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(nativeProxyMySQLUnknownPreparedStatement(command.StatementID, "execute")), nil, true)
	}
	longData := profile.PreparedLongDataValues(handle)
	if len(longData) > 0 {
		defer profile.ClearPreparedLongData(handle)
	}
	values, parameterTypes, err := qsmysql.DecodePreparedExecuteParametersWithOptions(command.Execute, prepared.Parameters, qsmysql.PreparedExecuteDecodeOptions{
		CachedTypes: profile.PreparedParameterTypes(handle),
		LongData:    longData,
	})
	if err != nil {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateInvalidParameter,
			VendorCode: 1210,
			Message:    err.Error(),
		}), nil, true)
	}
	profile.StorePreparedParameterTypes(handle, parameterTypes)
	result, err := h.FrontDoor.Server.ExecuteSQLWithSession(ctx, nativeProxyMySQLSessionForCommand(profile, command), prepared.SQL, h.Options, values...)
	if err != nil {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponseFromError(err), nil, true)
	}
	profile.Store(result.Instrumentation)
	result.Runtime.Diagnostics = append(result.Runtime.Diagnostics, profile.ApplySessionActions(nativeProxySessionActions(result))...)
	response, responseErr := nativeProxyMySQLPreparedResponseFromSQLResult(result, command.Database)
	return h.rememberMySQLCommandResponse(response, responseErr, true)
}

func (h NativeProxyMySQLCommandHandler) handleMySQLPreparedStatementLongData(command qsmysql.Command) (qsmysql.CommandResponse, error) {
	profile := h.sessionProfile()
	handle := qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)}
	prepared, ok := profile.PreparedStatements().Get(handle)
	if !ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(nativeProxyMySQLUnknownPreparedStatement(command.StatementID, "send_long_data")), nil, true)
	}
	parameter, ok := nativeProxyMySQLLongDataParameter(prepared.Parameters, command.LongData.ParameterIndex())
	if !ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateInvalidParameter,
			VendorCode: 1210,
			Message:    fmt.Sprintf("unknown prepared long-data parameter %d for statement %d", command.LongData.ParameterID, command.StatementID),
		}), nil, true)
	}
	if _, ok := profile.AppendPreparedLongData(handle, parameter, command.LongData.Data); !ok {
		return h.rememberMySQLCommandResponse(qsmysql.ErrorResponse(qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateInvalidParameter,
			VendorCode: 1210,
			Message:    fmt.Sprintf("could not store prepared long-data parameter %d for statement %d", command.LongData.ParameterID, command.StatementID),
		}), nil, true)
	}
	return qsmysql.NoResponse(), nil
}

func (h NativeProxyMySQLCommandHandler) rememberMySQLCommandResponse(response qsmysql.CommandResponse, err error, clearOnSuccess bool) (qsmysql.CommandResponse, error) {
	profile := h.sessionProfile()
	if err != nil {
		profile.StoreLastProtocolError(qsmysql.ProtocolErrorFromError(err))
		return response, err
	}
	if response.Kind == qsmysql.CommandResponseError && response.ProtocolError != nil {
		profile.StoreLastProtocolError(*response.ProtocolError)
		return response, nil
	}
	if clearOnSuccess {
		profile.ClearLastProtocolError()
	}
	return response, nil
}

func (h NativeProxyMySQLCommandHandler) preparedStatementRegistry() *qsbridge.MemoryPreparedStatementRegistry {
	return h.sessionProfile().PreparedStatements()
}

func (h NativeProxyMySQLCommandHandler) sessionProfile() *NativeProxyMySQLSessionProfile {
	if h.Profile != nil {
		return h.Profile
	}
	return NewNativeProxyMySQLSessionProfile()
}

func nativeProxyMySQLSessionForCommand(profile *NativeProxyMySQLSessionProfile, command qsmysql.Command) qsbridge.SessionContext {
	if profile == nil {
		return qsbridge.SessionContext{
			User:          qsbridge.UserName(command.Username),
			Roles:         append([]qsbridge.RoleName(nil), command.Roles...),
			CurrentSchema: command.Database,
		}
	}
	profile.SetAuthenticatedUser(command.Username)
	profile.SetAuthenticatedRoles(command.Roles)
	profile.SetCurrentSchema(command.Database)
	session := profile.Session()
	if session.User == "" {
		session.User = qsbridge.UserName(command.Username)
	}
	if len(session.Roles) == 0 {
		session.Roles = append([]qsbridge.RoleName(nil), command.Roles...)
	}
	if session.CurrentSchema == "" {
		session.CurrentSchema = command.Database
	}
	return session
}

func nativeProxyMySQLUnknownPreparedStatement(statementID uint32, operation string) qsbridge.ProtocolError {
	return qsbridge.ProtocolError{
		SQLState:   qsbridge.SQLStateInvalidParameter,
		VendorCode: 1243,
		Message:    fmt.Sprintf("unknown prepared statement handler (%d) given to COM_STMT_%s", statementID, operation),
	}
}

func nativeProxyMySQLLongDataParameter(parameters []qsbridge.ParameterRef, oneBasedIndex int) (qsbridge.ParameterValue, bool) {
	if oneBasedIndex <= 0 || oneBasedIndex > len(parameters) {
		return qsbridge.ParameterValue{}, false
	}
	ref := parameters[oneBasedIndex-1]
	index := ref.Index
	if index == 0 {
		index = oneBasedIndex
	}
	return qsbridge.IndexedParameterValue(index, qsbridge.ValueString, nil), true
}

func nativeProxyMySQLResponseFromSQLResult(result SQLExecutionResult, database string) (qsmysql.CommandResponse, error) {
	if protocolError, ok := result.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	if protocolError, ok := result.Runtime.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	clientResult := nativeProxyClientExecutionResult(result)
	if protocolError, ok := clientResult.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	if clientResult.Kind == qsbridge.ResultQuery || len(clientResult.Columns) > 0 {
		return qsmysql.QueryResponseWithOptions(clientResult, nativeProxyMySQLResultSetOptions(database))
	}
	return qsmysql.StatementOKResponse(clientResult.Statement), nil
}

func nativeProxyMySQLPreparedResponseFromSQLResult(result SQLExecutionResult, database string) (qsmysql.CommandResponse, error) {
	if protocolError, ok := result.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	if protocolError, ok := result.Runtime.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	clientResult := nativeProxyClientExecutionResult(result)
	if protocolError, ok := clientResult.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	if clientResult.Kind == qsbridge.ResultQuery || len(clientResult.Columns) > 0 {
		return qsmysql.BinaryQueryResponseWithOptions(clientResult, nativeProxyMySQLResultSetOptions(database))
	}
	return qsmysql.StatementOKResponse(clientResult.Statement), nil
}

func nativeProxyMySQLFieldListResponseFromSQLResult(result SQLExecutionResult, database string, pattern string) (qsmysql.CommandResponse, error) {
	if protocolError, ok := result.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	if protocolError, ok := result.Runtime.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	clientResult := nativeProxyClientExecutionResult(result)
	if protocolError, ok := clientResult.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	return qsmysql.FieldListResponseWithOptions(clientResult, nativeProxyMySQLResultSetOptions(database), pattern)
}

func nativeProxyMySQLResultSetOptions(database string) qsmysql.ResultSetOptions {
	return qsmysql.ResultSetOptions{DefaultSchema: nativeProxyDefaultSchema(database)}
}

func nativeProxyMySQLQuoteIdentifierPath(identifier string) string {
	parts := strings.Split(identifier, ".")
	for i, part := range parts {
		parts[i] = "`" + strings.ReplaceAll(strings.TrimSpace(part), "`", "``") + "`"
	}
	return strings.Join(parts, ".")
}

func nativeProxyDefaultSchema(database string) string {
	if database != "" {
		return database
	}
	return "quanta"
}

func nativeProxyClientExecutionResult(result SQLExecutionResult) qsbridge.ExecutionResult {
	request := result.Request
	statement := result.Runtime.Statement
	if statementResultEmpty(statement) {
		statement = request.Statement
	}
	clientResult := qsbridge.ExecutionResult{
		RequestID:      request.Options.RequestID,
		Kind:           request.Result.Kind,
		Columns:        append([]qsbridge.ResultColumn(nil), request.ResultColumns...),
		Statement:      cloneStatementResult(statement),
		SessionActions: qsbridge.CloneSessionActions(request.SessionActions),
		Diagnostics:    append(qsbridge.DiagnosticSet(nil), result.Diagnostics...),
		Complete:       true,
	}
	clientResult.Diagnostics = append(clientResult.Diagnostics, result.Runtime.Diagnostics...)
	if clientResult.Kind == "" {
		if len(clientResult.Columns) > 0 {
			clientResult.Kind = qsbridge.ResultQuery
		} else {
			clientResult.Kind = qsbridge.ResultStatement
		}
	}
	if clientResult.Kind == qsbridge.ResultStatement && clientResult.Statement.AffectedRows == 0 && result.Runtime.Count > 0 {
		clientResult.Statement.AffectedRows = result.Runtime.Count
	}
	if len(result.Runtime.RowSet.ProjectionVectors) == 0 && result.Runtime.RowSet.CandidateCount() == 0 {
		if clientResult.Diagnostics.BlocksNative() {
			clientResult.Status = qsbridge.ExecutionFailed
		} else {
			clientResult.Status = qsbridge.ExecutionComplete
		}
		return clientResult
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		clientResult.Diagnostics = append(clientResult.Diagnostics, diagnostics...)
		clientResult.Status = qsbridge.ExecutionFailed
		return clientResult
	}
	return clientResult.WithChunk(chunk)
}

func statementResultEmpty(statement qsbridge.StatementResult) bool {
	return statement.AffectedRows == 0 &&
		statement.LastInsertID == 0 &&
		statement.Warnings == 0 &&
		statement.Status == "" &&
		len(statement.Notices) == 0 &&
		len(statement.SessionActions) == 0
}
