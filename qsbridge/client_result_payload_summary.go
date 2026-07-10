package qsbridge

import (
	"sort"
	"strings"
)

// ClientResultPayloadSummaryRow describes observed result payload shape by column.
type ClientResultPayloadSummaryRow struct {
	RequestID    ExecutionRequestID
	Ordinal      int
	ColumnName   string
	LogicalType  DataType
	Chunks       int
	Cells        int
	MissingCells int
	NullCells    int
	ValueKinds   []ValueKind
}

// ClientResultPayloadSummaryExchange is adapter-facing payload shape metadata.
type ClientResultPayloadSummaryExchange struct {
	Connection   ConnectionContext
	Execution    ExecutionResult
	Rows         []ClientResultPayloadSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientResultPayloadSummary returns payload shape rows without exposing values.
func (s PlanningService) ListClientResultPayloadSummary(connection ConnectionContext, execution ExecutionResult) ClientResultPayloadSummaryExchange {
	_ = s
	exchange := ClientResultPayloadSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Execution:   cloneExecutionResult(execution),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, execution.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = resultPayloadSummaryRows(execution)
	}
	exchange.Result = exchange.resultPayloadSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether result payload summary metadata can be returned.
func (e ClientResultPayloadSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts result payload summary diagnostics into protocol-facing errors.
func (e ClientResultPayloadSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking result payload summary error, if any.
func (e ClientResultPayloadSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientResultPayloadSummaryExchange) resultPayloadSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     resultPayloadSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.resultPayloadSummaryResultRows(),
		Final: true,
	})
}

func resultPayloadSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Column_name", Type: DataTypeString, Nullable: true},
		{Name: "Logical_type", Type: DataTypeString, Nullable: true},
		{Name: "Chunks", Type: DataTypeInt},
		{Name: "Cells", Type: DataTypeInt},
		{Name: "Missing_cells", Type: DataTypeInt},
		{Name: "Null_cells", Type: DataTypeInt},
		{Name: "Value_kinds", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientResultPayloadSummaryExchange) resultPayloadSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.RequestID)),
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.ColumnName),
			metadataStringCell(string(row.LogicalType)),
			metadataIntCell(row.Chunks),
			metadataIntCell(row.Cells),
			metadataIntCell(row.MissingCells),
			metadataIntCell(row.NullCells),
			metadataStringCell(joinValueKinds(row.ValueKinds)),
		})
	}
	return rows
}

func resultPayloadSummaryRows(execution ExecutionResult) []ClientResultPayloadSummaryRow {
	maxColumns := len(execution.Columns)
	for _, chunk := range execution.Chunks {
		for _, row := range chunk.Rows {
			if row != nil && len(row) > maxColumns {
				maxColumns = len(row)
			}
		}
	}
	if maxColumns == 0 {
		return nil
	}

	rows := make([]ClientResultPayloadSummaryRow, 0, maxColumns)
	for ordinal := 0; ordinal < maxColumns; ordinal++ {
		summary := ClientResultPayloadSummaryRow{
			RequestID: execution.RequestID,
			Ordinal:   ordinal + 1,
			Chunks:    len(execution.Chunks),
		}
		if ordinal < len(execution.Columns) {
			summary.ColumnName = execution.Columns[ordinal].Name
			summary.LogicalType = execution.Columns[ordinal].Type
		}
		kinds := make(map[ValueKind]struct{})
		for _, chunk := range execution.Chunks {
			for _, row := range chunk.Rows {
				if row == nil {
					continue
				}
				if ordinal >= len(row) {
					summary.MissingCells++
					continue
				}
				cell := row[ordinal]
				kind := normalizedResultCellKind(cell)
				summary.Cells++
				if kind == ValueNull {
					summary.NullCells++
				}
				kinds[kind] = struct{}{}
			}
		}
		summary.ValueKinds = sortedValueKinds(kinds)
		rows = append(rows, summary)
	}
	return rows
}

func normalizedResultCellKind(cell ResultCell) ValueKind {
	if cell.Kind != "" {
		return cell.Kind
	}
	if cell.Value == nil {
		return ValueNull
	}
	return ValueKind("unknown")
}

func sortedValueKinds(kinds map[ValueKind]struct{}) []ValueKind {
	if len(kinds) == 0 {
		return nil
	}
	values := make([]ValueKind, 0, len(kinds))
	for kind := range kinds {
		values = append(values, kind)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func joinValueKinds(kinds []ValueKind) string {
	if len(kinds) == 0 {
		return ""
	}
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, string(kind))
	}
	return strings.Join(parts, ",")
}
