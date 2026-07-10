package qsbridge

import "sort"

// ClientSQLMode describes one adapter-visible SQL compatibility mode.
type ClientSQLMode struct {
	Name        SQLMode
	Description string
	Supported   bool
	Default     bool
	Enabled     bool
}

// ClientSQLModesSummaryRow describes aggregate SQL compatibility mode metadata.
type ClientSQLModesSummaryRow struct {
	ModeCount             int
	SupportedCount        int
	UnsupportedCount      int
	DefaultCount          int
	EnabledCount          int
	DefaultEnabledCount   int
	SupportedEnabledCount int
}

// ClientSQLModesExchange is adapter-facing SQL mode metadata.
type ClientSQLModesExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Modes        []ClientSQLMode
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientSQLModes returns adapter-supplied SQL mode metadata with session enablement.
func (s PlanningService) ListClientSQLModes(connection ConnectionContext, modes []ClientSQLMode, pattern string) ClientSQLModesExchange {
	_ = s
	exchange := ClientSQLModesExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Modes = filterClientSQLModes(markEnabledSQLModes(modes, connection.Session), pattern)
	}
	exchange.Result = exchange.sqlModesResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether SQL mode metadata can be returned.
func (e ClientSQLModesExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts SQL mode diagnostics into protocol-facing errors.
func (e ClientSQLModesExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking SQL mode error, if any.
func (e ClientSQLModesExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSQLModesExchange) sqlModesResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sqlModesResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sqlModeRows(),
		Final: true,
	})
}

func sqlModesResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Mode", Type: DataTypeString},
		{Name: "Description", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Default", Type: DataTypeBool},
		{Name: "Enabled", Type: DataTypeBool},
	}
}

func (e ClientSQLModesExchange) sqlModeRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Modes))
	for _, mode := range e.Modes {
		rows = append(rows, ResultRow{
			metadataStringCell(string(mode.Name)),
			metadataStringCell(mode.Description),
			metadataBoolCell(mode.Supported),
			metadataBoolCell(mode.Default),
			metadataBoolCell(mode.Enabled),
		})
	}
	return rows
}

func markEnabledSQLModes(modes []ClientSQLMode, session SessionContext) []ClientSQLMode {
	cloned := cloneClientSQLModes(modes)
	for i := range cloned {
		cloned[i].Enabled = session.HasSQLMode(cloned[i].Name)
	}
	return cloned
}

func filterClientSQLModes(modes []ClientSQLMode, pattern string) []ClientSQLMode {
	cloned := cloneClientSQLModes(modes)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientSQLMode, 0, len(cloned))
	for _, mode := range cloned {
		if catalogFieldPatternMatch(pattern, string(mode.Name)) ||
			catalogFieldPatternMatch(pattern, mode.Description) {
			filtered = append(filtered, mode)
		}
	}
	return filtered
}

func cloneClientSQLModes(modes []ClientSQLMode) []ClientSQLMode {
	if len(modes) == 0 {
		return nil
	}
	return append([]ClientSQLMode(nil), modes...)
}

func summarizeClientSQLModes(modes []ClientSQLMode) ClientSQLModesSummaryRow {
	summary := ClientSQLModesSummaryRow{ModeCount: len(modes)}
	for _, mode := range modes {
		if mode.Supported {
			summary.SupportedCount++
		} else {
			summary.UnsupportedCount++
		}
		if mode.Default {
			summary.DefaultCount++
		}
		if mode.Enabled {
			summary.EnabledCount++
		}
		if mode.Default && mode.Enabled {
			summary.DefaultEnabledCount++
		}
		if mode.Supported && mode.Enabled {
			summary.SupportedEnabledCount++
		}
	}
	return summary
}
