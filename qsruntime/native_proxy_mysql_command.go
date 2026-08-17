package qsruntime

import (
	"context"
	"fmt"

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
		Reader:  reader,
		Writer:  writer,
		Handler: NativeProxyMySQLCommandHandler{FrontDoor: f, Options: options, Profile: NewNativeProxyMySQLSessionProfile()},
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
	case qsmysql.CommandKindStmtPrepare:
		return h.handleMySQLPreparedStatementPrepare(command)
	case qsmysql.CommandKindStmtExecute:
		return h.handleMySQLPreparedStatementExecute(ctx, command)
	case qsmysql.CommandKindStmtClose:
		h.preparedStatementRegistry().Close(qsbridge.PreparedStatementCloseRequest{
			Handle: qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)},
		})
		return qsmysql.NoResponse(), nil
	case qsmysql.CommandKindStmtReset:
		if _, ok := h.preparedStatementRegistry().Get(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)}); !ok {
			return qsmysql.ErrorResponse(nativeProxyMySQLUnknownPreparedStatement(command.StatementID, "reset")), nil
		}
		return qsmysql.StatementOKResponse(qsbridge.StatementResult{}), nil
	case qsmysql.CommandKindQuery:
		if response, ok, err := nativeProxyMySQLMetadataQueryResponse(command); ok || err != nil {
			return response, err
		}
		if response, ok, err := nativeProxyMySQLProfileQueryResponse(command, h.Profile); ok || err != nil {
			return response, err
		}
		result, err := h.FrontDoor.Server.ExecuteSQL(ctx, command.SQL, h.Options)
		if err != nil {
			return qsmysql.ErrorResponseFromError(err), nil
		}
		h.Profile.Store(result.Instrumentation)
		return nativeProxyMySQLResponseFromSQLResult(result)
	default:
		return qsmysql.ErrorResponse(qsmysql.ProtocolErrorFromError(nil)), nil
	}
}

func (h NativeProxyMySQLCommandHandler) handleMySQLPreparedStatementPrepare(command qsmysql.Command) (qsmysql.CommandResponse, error) {
	prepared, diagnostics := h.FrontDoor.Server.PrepareSQL(command.SQL)
	if protocolError, ok := diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	description := h.preparedStatementRegistry().Register(prepared)
	if protocolError, ok := description.Diagnostics.FirstProtocolError(); ok {
		return qsmysql.ErrorResponse(protocolError), nil
	}
	return qsmysql.PreparedStatementResponse(description)
}

func (h NativeProxyMySQLCommandHandler) handleMySQLPreparedStatementExecute(ctx context.Context, command qsmysql.Command) (qsmysql.CommandResponse, error) {
	prepared, ok := h.preparedStatementRegistry().Get(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(command.StatementID)})
	if !ok {
		return qsmysql.ErrorResponse(nativeProxyMySQLUnknownPreparedStatement(command.StatementID, "execute")), nil
	}
	values, err := qsmysql.DecodePreparedExecuteParameters(command.Execute, prepared.Parameters)
	if err != nil {
		return qsmysql.ErrorResponse(qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateInvalidParameter,
			VendorCode: 1210,
			Message:    err.Error(),
		}), nil
	}
	result, err := h.FrontDoor.Server.ExecuteSQL(ctx, prepared.SQL, h.Options, values...)
	if err != nil {
		return qsmysql.ErrorResponseFromError(err), nil
	}
	profile := h.sessionProfile()
	profile.Store(result.Instrumentation)
	return nativeProxyMySQLPreparedResponseFromSQLResult(result)
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

func nativeProxyMySQLUnknownPreparedStatement(statementID uint32, operation string) qsbridge.ProtocolError {
	return qsbridge.ProtocolError{
		SQLState:   qsbridge.SQLStateInvalidParameter,
		VendorCode: 1243,
		Message:    fmt.Sprintf("unknown prepared statement handler (%d) given to COM_STMT_%s", statementID, operation),
	}
}

func nativeProxyMySQLResponseFromSQLResult(result SQLExecutionResult) (qsmysql.CommandResponse, error) {
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
		return qsmysql.QueryResponse(clientResult)
	}
	return qsmysql.StatementOKResponse(clientResult.Statement), nil
}

func nativeProxyMySQLPreparedResponseFromSQLResult(result SQLExecutionResult) (qsmysql.CommandResponse, error) {
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
		return qsmysql.BinaryQueryResponse(clientResult)
	}
	return qsmysql.StatementOKResponse(clientResult.Statement), nil
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
		SessionActions: append([]qsbridge.SessionAction(nil), request.SessionActions...),
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
