package qsbridge

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"
)

var (
	_ driver.Connector        = SQLDriverConnector{}
	_ driver.Driver           = sqlDriver{}
	_ driver.Conn             = (*sqlDriverConn)(nil)
	_ driver.Pinger           = (*sqlDriverConn)(nil)
	_ driver.QueryerContext   = (*sqlDriverConn)(nil)
	_ driver.ExecerContext    = (*sqlDriverConn)(nil)
	_ driver.Stmt             = (*sqlDriverStmt)(nil)
	_ driver.StmtQueryContext = (*sqlDriverStmt)(nil)
	_ driver.StmtExecContext  = (*sqlDriverStmt)(nil)
	_ driver.Rows             = (*sqlDriverRows)(nil)
	_ driver.Result           = sqlDriverResult{}
)

// SQLDriverConnector adapts qsbridge planning and execution contracts to database/sql.
//
// The connector is intentionally not globally registered. Tests and future
// adapters can use sql.OpenDB(connector) without claiming a process-wide driver
// name or importing the legacy qlbridge SQL driver.
type SQLDriverConnector struct {
	Service    PlanningService
	Dispatcher ExecutionDispatcher
	Options    ExecutionOptions
}

// NewSQLDriverConnector creates a database/sql connector for qsbridge.
func NewSQLDriverConnector(service PlanningService, dispatcher ExecutionDispatcher) SQLDriverConnector {
	if dispatcher.Native == nil && dispatcher.Legacy == nil {
		dispatcher.Native = PlanOnlyNativeExecutor{}
	}
	return SQLDriverConnector{Service: service, Dispatcher: dispatcher}
}

// Connect creates one database/sql connection backed by qsbridge contracts.
func (c SQLDriverConnector) Connect(context.Context) (driver.Conn, error) {
	connector := c
	if connector.Dispatcher.Native == nil && connector.Dispatcher.Legacy == nil {
		connector.Dispatcher.Native = PlanOnlyNativeExecutor{}
	}
	return &sqlDriverConn{connector: connector}, nil
}

// Driver returns the database/sql driver associated with this connector.
func (c SQLDriverConnector) Driver() driver.Driver {
	return sqlDriver{connector: c}
}

type sqlDriver struct {
	connector SQLDriverConnector
}

// Open creates a connector-backed connection for database/sql.
func (d sqlDriver) Open(string) (driver.Conn, error) {
	return d.connector.Connect(context.Background())
}

type sqlDriverConn struct {
	connector SQLDriverConnector
	closed    bool
}

// Prepare creates a lightweight statement wrapper for this connection.
func (c *sqlDriverConn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &sqlDriverStmt{
		conn:     c,
		query:    strings.TrimSpace(query),
		numInput: strings.Count(query, "?"),
	}, nil
}

// Close marks the database/sql connection closed.
func (c *sqlDriverConn) Close() error {
	c.closed = true
	return nil
}

// Begin reports that qsbridge does not implement database/sql transactions yet.
func (c *sqlDriverConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// Ping validates that the connector-backed connection is still open.
func (c *sqlDriverConn) Ping(context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	return nil
}

// QueryContext plans, dispatches, and adapts a query result to driver.Rows.
func (c *sqlDriverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	result := c.execute(ctx, query, args)
	if err := sqlDriverResultError(result); err != nil {
		return nil, err
	}
	return newSQLDriverRows(result), nil
}

// ExecContext plans, dispatches, and adapts a statement result to driver.Result.
func (c *sqlDriverConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	result := c.execute(ctx, query, args)
	if err := sqlDriverResultError(result); err != nil {
		return nil, err
	}
	return sqlDriverResult{
		rowsAffected: int64(result.Statement.AffectedRows),
		lastInsertID: int64(result.Statement.LastInsertID),
	}, nil
}

func (c *sqlDriverConn) execute(ctx context.Context, query string, args []driver.NamedValue) ExecutionResult {
	_ = ctx
	options := c.connector.Options
	values := sqlDriverParameterValues(args)
	handoff := c.connector.Service.PrepareRoutedAuthorizedExecutionRequest(PlanRequest{SQL: query}, options, values...)
	return c.connector.Dispatcher.Dispatch(handoff)
}

type sqlDriverStmt struct {
	conn     *sqlDriverConn
	query    string
	numInput int
}

// Close releases the statement wrapper.
func (s *sqlDriverStmt) Close() error {
	return nil
}

// NumInput returns the positional placeholder count.
func (s *sqlDriverStmt) NumInput() int {
	return s.numInput
}

// Exec executes the prepared statement with positional values.
func (s *sqlDriverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), sqlDriverNamedValues(args))
}

// Query executes the prepared statement with positional values.
func (s *sqlDriverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), sqlDriverNamedValues(args))
}

// ExecContext executes the prepared statement with named values.
func (s *sqlDriverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

// QueryContext executes the prepared statement with named values.
func (s *sqlDriverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

type sqlDriverRows struct {
	columns    []string
	chunks     []ResultChunk
	chunkIndex int
	rowIndex   int
}

func newSQLDriverRows(result ExecutionResult) *sqlDriverRows {
	columns := make([]string, 0, len(result.Columns))
	for _, column := range result.Columns {
		columns = append(columns, column.Name)
	}
	return &sqlDriverRows{columns: columns, chunks: cloneResultChunks(result.Chunks)}
}

// Columns returns the driver-visible result column names.
func (r *sqlDriverRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

// Close releases buffered result rows.
func (r *sqlDriverRows) Close() error {
	r.chunks = nil
	return nil
}

// Next copies the next result row into dest.
func (r *sqlDriverRows) Next(dest []driver.Value) error {
	row, ok := r.nextRow()
	if !ok {
		return io.EOF
	}
	for i := range dest {
		if i >= len(row) {
			dest[i] = nil
			continue
		}
		dest[i] = sqlDriverValue(row[i])
	}
	return nil
}

func (r *sqlDriverRows) nextRow() (ResultRow, bool) {
	for r.chunkIndex < len(r.chunks) {
		chunk := r.chunks[r.chunkIndex]
		if r.rowIndex < len(chunk.Rows) {
			row := chunk.Rows[r.rowIndex]
			r.rowIndex++
			if row == nil {
				continue
			}
			return row, true
		}
		r.chunkIndex++
		r.rowIndex = 0
	}
	return nil, false
}

type sqlDriverResult struct {
	rowsAffected int64
	lastInsertID int64
}

// LastInsertId returns statement last-insert-id metadata.
func (r sqlDriverResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns statement affected-row metadata.
func (r sqlDriverResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type sqlDriverError struct {
	protocol ProtocolError
}

// Error returns the protocol diagnostic as a database/sql driver error.
func (e sqlDriverError) Error() string {
	return fmt.Sprintf("qsbridge sql driver error %d (%s): %s", e.protocol.VendorCode, e.protocol.SQLState, e.protocol.Message)
}

func sqlDriverResultError(result ExecutionResult) error {
	if protocol, ok := result.FirstProtocolError(); ok {
		return sqlDriverError{protocol: protocol}
	}
	if result.Status == ExecutionFailed {
		return fmt.Errorf("qsbridge sql driver execution failed")
	}
	return nil
}

func sqlDriverNamedValues(args []driver.Value) []driver.NamedValue {
	values := make([]driver.NamedValue, 0, len(args))
	for i, value := range args {
		values = append(values, driver.NamedValue{Ordinal: i + 1, Value: value})
	}
	return values
}

func sqlDriverParameterValues(args []driver.NamedValue) []ParameterValue {
	values := make([]ParameterValue, 0, len(args))
	for i, arg := range args {
		ordinal := arg.Ordinal
		if ordinal == 0 {
			ordinal = i + 1
		}
		kind, value := sqlDriverParameterKind(arg.Value)
		if arg.Name != "" {
			values = append(values, NamedParameterValue(arg.Name, kind, value))
			continue
		}
		values = append(values, IndexedParameterValue(ordinal, kind, value))
	}
	return values
}

func sqlDriverParameterKind(value any) (ValueKind, any) {
	switch typed := value.(type) {
	case nil:
		return ValueNull, nil
	case bool:
		return ValueBool, typed
	case int64:
		return ValueInt, typed
	case float64:
		return ValueFloat, typed
	case string:
		return ValueString, typed
	case []byte:
		return ValueString, string(typed)
	case time.Time:
		return ValueTime, typed
	default:
		return ValueUnknown, typed
	}
}

func sqlDriverValue(cell ResultCell) driver.Value {
	switch typed := cell.Value.(type) {
	case nil:
		return nil
	case bool:
		return typed
	case int:
		return int64(typed)
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint:
		return int64(typed)
	case uint8:
		return int64(typed)
	case uint16:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		return int64(typed)
	case float32:
		return float64(typed)
	case float64:
		return typed
	case []byte:
		return typed
	case string:
		return typed
	case time.Time:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}
