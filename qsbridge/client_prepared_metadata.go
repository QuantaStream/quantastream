package qsbridge

import "strings"

// ClientPreparedMetadataSection identifies the kind of prepare metadata row.
type ClientPreparedMetadataSection string

const (
	// ClientPreparedMetadataParameter describes one prepared-statement placeholder.
	ClientPreparedMetadataParameter ClientPreparedMetadataSection = "parameter"
	// ClientPreparedMetadataResultColumn describes one row-result column.
	ClientPreparedMetadataResultColumn ClientPreparedMetadataSection = "result_column"
)

// ClientPreparedMetadataRow describes one parameter or result column from a prepared plan.
type ClientPreparedMetadataRow struct {
	Section        ClientPreparedMetadataSection
	Ordinal        int
	StatementID    PreparedStatementID
	StatementName  string
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Name           string
	LogicalType    DataType
	TypeName       string
	WireType       string
	Nullable       bool
	Source         string
	Flags          []ProtocolColumnFlag
	SQL            string
}

// ClientPreparedMetadataSummaryRow describes aggregate prepared metadata.
type ClientPreparedMetadataSummaryRow struct {
	RowCount               int
	ParameterCount         int
	ResultColumnCount      int
	ReadIntentCount        int
	WriteIntentCount       int
	SelectLifecycleCount   int
	MutationLifecycleCount int
	NullableCount          int
	SourceCount            int
	FlaggedCount           int
}

// ClientPreparedMetadataExchange is adapter-facing prepare parameter and result-column metadata.
type ClientPreparedMetadataExchange struct {
	Connection   ConnectionContext
	Prepared     PreparedPlan
	Rows         []ClientPreparedMetadataRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientPreparedMetadata returns one row per prepared parameter and result column.
func (s PlanningService) ListClientPreparedMetadata(connection ConnectionContext, prepared PreparedPlan) ClientPreparedMetadataExchange {
	_ = s
	description := prepared.Description()
	exchange := ClientPreparedMetadataExchange{
		Connection:  cloneConnectionContext(connection),
		Prepared:    clonePreparedPlan(prepared),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), description.Diagnostics),
	}
	if connection.Supported() && description.SupportedForPrepare() {
		exchange.Rows = preparedMetadataRows(description, connection.Protocol)
	}
	exchange.Result = exchange.preparedMetadataResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepare metadata can be returned.
func (e ClientPreparedMetadataExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepare metadata diagnostics into protocol-facing errors.
func (e ClientPreparedMetadataExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepare metadata error, if any.
func (e ClientPreparedMetadataExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPreparedMetadataExchange) preparedMetadataResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedMetadataResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedMetadataResultRows(),
		Final: true,
	})
}

func preparedMetadataResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Section", Type: DataTypeString},
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Name", Type: DataTypeString, Nullable: true},
		{Name: "Logical_type", Type: DataTypeString, Nullable: true},
		{Name: "Type_name", Type: DataTypeString, Nullable: true},
		{Name: "Wire_type", Type: DataTypeString, Nullable: true},
		{Name: "Nullable", Type: DataTypeBool},
		{Name: "Source", Type: DataTypeString, Nullable: true},
		{Name: "Flags", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedMetadataExchange) preparedMetadataResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Section)),
			metadataIntCell(row.Ordinal),
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(row.Name),
			metadataStringCell(string(row.LogicalType)),
			metadataStringCell(row.TypeName),
			metadataStringCell(row.WireType),
			metadataBoolCell(row.Nullable),
			metadataStringCell(row.Source),
			metadataStringCell(joinProtocolColumnFlags(row.Flags)),
			metadataStringCell(row.SQL),
		})
	}
	return rows
}

func preparedMetadataRows(description PreparedPlanDescription, profile ProtocolProfile) []ClientPreparedMetadataRow {
	rows := make([]ClientPreparedMetadataRow, 0, len(description.Parameters)+len(description.ResultColumns))
	for i, parameter := range description.Parameters {
		typeName, wireType := protocolTypeNames(profile.Kind, parameter.Type)
		rows = append(rows, ClientPreparedMetadataRow{
			Section:        ClientPreparedMetadataParameter,
			Ordinal:        i + 1,
			StatementID:    description.Handle.ID,
			StatementName:  description.Handle.Name,
			AccessIntent:   description.AccessIntent,
			Lifecycle:      clientPlanLifecycleKind(description.Kind),
			LifecycleSteps: clientPlanLifecycleStepCount(description.Kind),
			Name:           parameterRefLabel(parameter),
			LogicalType:    parameter.Type,
			TypeName:       typeName,
			WireType:       wireType,
			Nullable:       parameter.Nullable,
			Flags:          []ProtocolColumnFlag{ProtocolColumnFlag("parameter")},
			SQL:            description.SQL,
		})
	}
	for i, column := range description.ResultColumns {
		protocolColumn := column.ProtocolColumn(profile)
		rows = append(rows, ClientPreparedMetadataRow{
			Section:        ClientPreparedMetadataResultColumn,
			Ordinal:        i + 1,
			StatementID:    description.Handle.ID,
			StatementName:  description.Handle.Name,
			AccessIntent:   description.AccessIntent,
			Lifecycle:      clientPlanLifecycleKind(description.Kind),
			LifecycleSteps: clientPlanLifecycleStepCount(description.Kind),
			Name:           protocolColumn.Name,
			LogicalType:    protocolColumn.LogicalType,
			TypeName:       protocolColumn.TypeName,
			WireType:       protocolColumn.WireType,
			Nullable:       protocolColumn.Nullable,
			Source:         protocolColumn.Source,
			Flags:          append([]ProtocolColumnFlag(nil), protocolColumn.Flags...),
			SQL:            description.SQL,
		})
	}
	return rows
}

func joinProtocolColumnFlags(flags []ProtocolColumnFlag) string {
	if len(flags) == 0 {
		return ""
	}
	values := make([]string, 0, len(flags))
	for _, flag := range flags {
		values = append(values, string(flag))
	}
	return strings.Join(values, ",")
}

func summarizePreparedMetadataRows(rows []ClientPreparedMetadataRow) ClientPreparedMetadataSummaryRow {
	summary := ClientPreparedMetadataSummaryRow{RowCount: len(rows)}
	for _, row := range rows {
		switch row.Section {
		case ClientPreparedMetadataParameter:
			summary.ParameterCount++
		case ClientPreparedMetadataResultColumn:
			summary.ResultColumnCount++
		}
		switch row.AccessIntent {
		case PhysicalAccessRead:
			summary.ReadIntentCount++
		case PhysicalAccessWrite:
			summary.WriteIntentCount++
		}
		switch row.Lifecycle {
		case ClientPlanLifecycleSelect:
			summary.SelectLifecycleCount++
		case ClientPlanLifecycleMutation:
			summary.MutationLifecycleCount++
		}
		if row.Nullable {
			summary.NullableCount++
		}
		if row.Source != "" {
			summary.SourceCount++
		}
		if len(row.Flags) > 0 {
			summary.FlaggedCount++
		}
	}
	return summary
}
