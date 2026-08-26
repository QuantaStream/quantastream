package qsmysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const commandTraceMaxSQLBytes = 4096

// CommandKindDecodeError identifies a packet that could not be decoded as a MySQL command.
const CommandKindDecodeError CommandKind = "decode_error"

// CommandTraceEvent is a diagnostic view of one MySQL command. SQL text is logged as sent; prepared value payloads are not logged.
type CommandTraceEvent struct {
	ConnectionID  uint32
	Username      string
	Database      string
	Kind          CommandKind
	SQL           string
	StatementID   uint32
	ParameterID   int
	LongDataBytes int
	ResponseKind  CommandResponseKind
	Elapsed       time.Duration
	Error         string
}

// CommandLogger receives opt-in MySQL command trace events.
type CommandLogger interface {
	LogCommandTrace(CommandTraceEvent)
}

// CommandLoggerFunc adapts a function into a command logger.
type CommandLoggerFunc func(CommandTraceEvent)

// LogCommandTrace emits a command trace event.
func (f CommandLoggerFunc) LogCommandTrace(event CommandTraceEvent) {
	if f != nil {
		f(event)
	}
}

// TraceEvent returns a value-redacted diagnostic event for the decoded command.
func (c Command) TraceEvent() CommandTraceEvent {
	event := CommandTraceEvent{
		ConnectionID: c.ConnectionID,
		Username:     c.Username,
		Database:     c.Database,
		Kind:         c.Kind,
		SQL:          c.SQL,
		StatementID:  c.StatementID,
	}
	if c.Kind == CommandKindStmtSendLongData {
		event.ParameterID = c.LongData.ParameterIndex()
		event.LongDataBytes = len(c.LongData.Data)
	}
	return event
}

// LogLine renders the event as a stable single-line diagnostic string.
func (e CommandTraceEvent) LogLine() string {
	parts := []string{
		"MYSQL_COMMAND_TRACE",
		fmt.Sprintf("connection_id=%d", e.ConnectionID),
		"user=" + commandTraceQuoteOrDash(e.Username),
		"db=" + commandTraceQuoteOrDash(e.Database),
		fmt.Sprintf("kind=%s", e.Kind),
	}
	if strings.TrimSpace(e.SQL) != "" {
		parts = append(parts, "sql="+strconv.Quote(commandTraceSQL(e.SQL)))
	}
	if e.StatementID != 0 || commandTraceStatementKind(e.Kind) {
		parts = append(parts, fmt.Sprintf("statement_id=%d", e.StatementID))
	}
	if e.ParameterID > 0 {
		parts = append(parts, fmt.Sprintf("parameter_id=%d", e.ParameterID))
	}
	if e.LongDataBytes > 0 {
		parts = append(parts, fmt.Sprintf("long_data_bytes=%d", e.LongDataBytes))
	}
	if e.ResponseKind != "" {
		parts = append(parts, fmt.Sprintf("response=%s", e.ResponseKind))
	}
	if e.Elapsed > 0 {
		parts = append(parts, "elapsed="+e.Elapsed.String())
	}
	if strings.TrimSpace(e.Error) != "" {
		parts = append(parts, "error="+strconv.Quote(e.Error))
	}
	return strings.Join(parts, " ")
}

func commandTraceStatementKind(kind CommandKind) bool {
	switch kind {
	case CommandKindStmtExecute, CommandKindStmtSendLongData, CommandKindStmtClose, CommandKindStmtReset:
		return true
	default:
		return false
	}
}

func commandTraceQuoteOrDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return strconv.Quote(value)
}

func commandTraceSQL(sql string) string {
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) <= commandTraceMaxSQLBytes {
		return sql
	}
	cut := commandTraceMaxSQLBytes
	for cut > 0 && (sql[cut]&0xc0) == 0x80 {
		cut--
	}
	return sql[:cut] + "..."
}
