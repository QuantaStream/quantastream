package qsruntime

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/version"
)

// nativeProxyMySQLMetadataQueryResponse answers MySQL client metadata probes
// that are legal no-FROM SELECT statements but do not belong in the analytical
// SQL parser. Standard drivers use these during connection setup.
func nativeProxyMySQLMetadataQueryResponse(command qsmysql.Command) (qsmysql.CommandResponse, bool, error) {
	result, ok := nativeProxyMySQLMetadataQueryResult(command)
	if !ok {
		return qsmysql.CommandResponse{}, false, nil
	}
	response, err := qsmysql.QueryResponse(result)
	return response, true, err
}

func nativeProxyMySQLMetadataQueryResult(command qsmysql.Command) (qsbridge.ExecutionResult, bool) {
	normalized := nativeProxyNormalizeMetadataSQL(command.SQL)
	switch normalized {
	case "select @@max_allowed_packet":
		return nativeProxySingleCellMetadataResult("@@max_allowed_packet", qsbridge.DataTypeInt, qsbridge.ValueInt, int64(64*1024*1024)), true
	case "select database()", "select schema()":
		return nativeProxySingleCellMetadataResult("DATABASE()", qsbridge.DataTypeString, qsbridge.ValueString, nativeProxyMetadataDatabase(command)), true
	case "select version()":
		return nativeProxySingleCellMetadataResult("VERSION()", qsbridge.DataTypeString, qsbridge.ValueString, version.MySQLVersion()), true
	case "select connection_id()":
		return nativeProxySingleCellMetadataResult("CONNECTION_ID()", qsbridge.DataTypeInt, qsbridge.ValueInt, int64(nativeProxyMetadataConnectionID(command))), true
	default:
		if result, ok := nativeProxyPerformanceSchemaMetadataResult(command.SQL); ok {
			return result, true
		}
		return qsbridge.ExecutionResult{}, false
	}
}

func nativeProxyMetadataDatabase(command qsmysql.Command) string {
	database := strings.TrimSpace(command.Database)
	if database == "" {
		return "quanta"
	}
	return database
}

func nativeProxyMetadataConnectionID(command qsmysql.Command) uint32 {
	if command.ConnectionID == 0 {
		return 1
	}
	return command.ConnectionID
}

func nativeProxySingleCellMetadataResult(name string, dataType qsbridge.DataType, valueKind qsbridge.ValueKind, value any) qsbridge.ExecutionResult {
	return qsbridge.ExecutionResult{
		Status: qsbridge.ExecutionComplete,
		Kind:   qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{
			Name: name,
			Type: dataType,
		}},
		Chunks: []qsbridge.ResultChunk{{
			Rows: []qsbridge.ResultRow{{
				{Kind: valueKind, Value: value},
			}},
			Final: true,
		}},
		Complete:     true,
		RowsReturned: 1,
	}
}

func nativeProxyNormalizeMetadataSQL(sql string) string {
	text := strings.TrimSpace(sql)
	text = strings.TrimSuffix(text, ";")
	fields := strings.Fields(text)
	return strings.ToLower(strings.Join(fields, " "))
}

func nativeProxyPerformanceSchemaMetadataResult(sql string) (qsbridge.ExecutionResult, bool) {
	normalized := nativeProxyNormalizeMetadataSQL(strings.ReplaceAll(sql, "`", ""))
	table, ok := nativeProxyPerformanceSchemaTable(normalized)
	if !ok {
		return qsbridge.ExecutionResult{}, false
	}
	columns := nativeProxyPerformanceSchemaProjectionColumns(sql)
	if len(columns) == 0 {
		columns = nativeProxyPerformanceSchemaDefaultColumns(table)
	}
	return nativeProxyEmptyMetadataResult(columns), true
}

func nativeProxyPerformanceSchemaTable(normalized string) (string, bool) {
	for _, table := range []string{
		"events_statements_current",
		"events_stages_current",
		"events_stages_history",
		"events_stages_history_long",
		"events_waits_current",
		"events_waits_history",
		"events_waits_history_long",
	} {
		if strings.Contains(normalized, " from performance_schema."+table) ||
			strings.Contains(normalized, " join performance_schema."+table) {
			return table, true
		}
	}
	return "", false
}

func nativeProxyPerformanceSchemaProjectionColumns(sql string) []string {
	normalized := strings.ToLower(sql)
	selectIndex := strings.Index(normalized, "select")
	fromIndex := strings.Index(normalized, "from")
	if selectIndex < 0 || fromIndex <= selectIndex {
		return nil
	}
	projection := strings.TrimSpace(sql[selectIndex+len("select") : fromIndex])
	if projection == "" || projection == "*" {
		return nil
	}
	parts := splitNativeProxyMetadataProjectionList(projection)
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := nativeProxyMetadataProjectionName(part); name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

func splitNativeProxyMetadataProjectionList(projection string) []string {
	var parts []string
	start := 0
	depth := 0
	quote := rune(0)
	for i, r := range projection {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"' || r == '`':
			quote = r
		case r == '(':
			depth++
		case r == ')' && depth > 0:
			depth--
		case r == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(projection[start:i]))
			start = i + len(string(r))
		}
	}
	parts = append(parts, strings.TrimSpace(projection[start:]))
	return parts
}

func nativeProxyMetadataProjectionName(projection string) string {
	projection = strings.TrimSpace(projection)
	if projection == "" {
		return ""
	}
	lower := strings.ToLower(projection)
	if idx := strings.LastIndex(lower, " as "); idx >= 0 {
		return nativeProxyCleanMetadataIdentifier(projection[idx+4:])
	}
	fields := strings.Fields(projection)
	if len(fields) > 1 {
		return nativeProxyCleanMetadataIdentifier(fields[len(fields)-1])
	}
	if dot := strings.LastIndex(projection, "."); dot >= 0 && dot+1 < len(projection) {
		projection = projection[dot+1:]
	}
	return nativeProxyCleanMetadataIdentifier(projection)
}

func nativeProxyCleanMetadataIdentifier(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`\"'")
	value = strings.TrimRight(value, ",")
	if value == "" || strings.ContainsAny(value, "()+-*/") {
		return "expr"
	}
	return value
}

func nativeProxyPerformanceSchemaDefaultColumns(table string) []string {
	switch table {
	case "events_statements_current":
		return []string{"THREAD_ID", "EVENT_ID", "EVENT_NAME", "SQL_TEXT", "TIMER_WAIT", "LOCK_TIME", "ERRORS", "WARNINGS", "ROWS_SENT", "ROWS_EXAMINED"}
	case "events_stages_current", "events_stages_history", "events_stages_history_long":
		return []string{"THREAD_ID", "EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_WAIT", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"}
	case "events_waits_current", "events_waits_history", "events_waits_history_long":
		return []string{"THREAD_ID", "EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_WAIT", "OPERATION", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"}
	default:
		return []string{"THREAD_ID", "EVENT_ID", "EVENT_NAME"}
	}
}

func nativeProxyEmptyMetadataResult(columns []string) qsbridge.ExecutionResult {
	resultColumns := make([]qsbridge.ResultColumn, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		resultColumns = append(resultColumns, qsbridge.ResultColumn{
			Name:     column,
			Type:     qsbridge.DataTypeString,
			Nullable: true,
		})
	}
	if len(resultColumns) == 0 {
		resultColumns = []qsbridge.ResultColumn{{Name: "metadata", Type: qsbridge.DataTypeString, Nullable: true}}
	}
	return qsbridge.ExecutionResult{
		Status:   qsbridge.ExecutionComplete,
		Kind:     qsbridge.ResultQuery,
		Columns:  resultColumns,
		Complete: true,
	}
}
