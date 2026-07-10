package qsbridge

import "sort"

// ClientStorageEngineSupport describes whether a storage engine is available.
type ClientStorageEngineSupport string

const (
	// ClientStorageEngineSupportYes means the engine is available.
	ClientStorageEngineSupportYes ClientStorageEngineSupport = "YES"
	// ClientStorageEngineSupportNo means the engine is known but unavailable.
	ClientStorageEngineSupportNo ClientStorageEngineSupport = "NO"
	// ClientStorageEngineSupportDefault means the engine is available and default.
	ClientStorageEngineSupportDefault ClientStorageEngineSupport = "DEFAULT"
	// ClientStorageEngineSupportDisabled means the engine is disabled.
	ClientStorageEngineSupportDisabled ClientStorageEngineSupport = "DISABLED"
)

// ClientStorageEngine describes one adapter-visible storage engine row.
type ClientStorageEngine struct {
	Name         string
	Support      ClientStorageEngineSupport
	Comment      string
	Transactions bool
	XA           bool
	Savepoints   bool
}

// ClientStorageEnginesSummaryRow describes aggregate SHOW ENGINES-style metadata.
type ClientStorageEnginesSummaryRow struct {
	EngineCount       int
	DefaultCount      int
	AvailableCount    int
	UnavailableCount  int
	DisabledCount     int
	TransactionsCount int
	XACount           int
	SavepointsCount   int
}

// ClientStorageEnginesExchange is adapter-facing storage engine metadata.
type ClientStorageEnginesExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Engines      []ClientStorageEngine
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientStorageEngines returns SHOW ENGINES-style metadata supplied by the adapter.
func (s PlanningService) ListClientStorageEngines(connection ConnectionContext, engines []ClientStorageEngine, pattern string) ClientStorageEnginesExchange {
	_ = s
	exchange := ClientStorageEnginesExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Engines = filterClientStorageEngines(engines, pattern)
	}
	exchange.Result = exchange.storageEnginesResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether storage engine metadata can be returned.
func (e ClientStorageEnginesExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts storage engine diagnostics into protocol-facing errors.
func (e ClientStorageEnginesExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking storage engine error, if any.
func (e ClientStorageEnginesExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStorageEnginesExchange) storageEnginesResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     storageEnginesResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.storageEngineRows(),
		Final: true,
	})
}

func storageEnginesResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Engine", Type: DataTypeString},
		{Name: "Support", Type: DataTypeString},
		{Name: "Comment", Type: DataTypeString, Nullable: true},
		{Name: "Transactions", Type: DataTypeBool},
		{Name: "XA", Type: DataTypeBool},
		{Name: "Savepoints", Type: DataTypeBool},
	}
}

func (e ClientStorageEnginesExchange) storageEngineRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Engines))
	for _, engine := range e.Engines {
		rows = append(rows, ResultRow{
			metadataStringCell(engine.Name),
			metadataStringCell(string(engine.Support)),
			metadataStringCell(engine.Comment),
			metadataBoolCell(engine.Transactions),
			metadataBoolCell(engine.XA),
			metadataBoolCell(engine.Savepoints),
		})
	}
	return rows
}

func filterClientStorageEngines(engines []ClientStorageEngine, pattern string) []ClientStorageEngine {
	cloned := cloneClientStorageEngines(engines)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientStorageEngine, 0, len(cloned))
	for _, engine := range cloned {
		if catalogFieldPatternMatch(pattern, engine.Name) {
			filtered = append(filtered, engine)
		}
	}
	return filtered
}

func cloneClientStorageEngines(engines []ClientStorageEngine) []ClientStorageEngine {
	if len(engines) == 0 {
		return nil
	}
	return append([]ClientStorageEngine(nil), engines...)
}

func summarizeClientStorageEngines(engines []ClientStorageEngine) ClientStorageEnginesSummaryRow {
	summary := ClientStorageEnginesSummaryRow{EngineCount: len(engines)}
	for _, engine := range engines {
		switch engine.Support {
		case ClientStorageEngineSupportDefault:
			summary.DefaultCount++
			summary.AvailableCount++
		case ClientStorageEngineSupportYes:
			summary.AvailableCount++
		case ClientStorageEngineSupportDisabled:
			summary.DisabledCount++
		default:
			summary.UnavailableCount++
		}
		if engine.Transactions {
			summary.TransactionsCount++
		}
		if engine.XA {
			summary.XACount++
		}
		if engine.Savepoints {
			summary.SavepointsCount++
		}
	}
	return summary
}
