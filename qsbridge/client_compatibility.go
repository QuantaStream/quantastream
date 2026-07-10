package qsbridge

import "sort"

// ClientCompatibilityExchange is adapter-facing qsbridge compatibility metadata.
type ClientCompatibilityExchange struct {
	Connection   ConnectionContext
	Profile      CompatibilityProfile
	Rows         []CompatibilityCapability
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCompatibility returns protocol-neutral rows for the qsbridge compatibility manifest.
func (s PlanningService) ListClientCompatibility(connection ConnectionContext) ClientCompatibilityExchange {
	profile := s.CompatibilityProfile()
	exchange := ClientCompatibilityExchange{
		Connection:  cloneConnectionContext(connection),
		Profile:     profile.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), profile.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = compatibilityRows(profile.Capabilities)
	}
	exchange.Result = exchange.compatibilityResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether compatibility metadata can be returned.
func (e ClientCompatibilityExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts compatibility diagnostics into protocol-facing errors.
func (e ClientCompatibilityExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking compatibility error, if any.
func (e ClientCompatibilityExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCompatibilityExchange) compatibilityResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     compatibilityResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.compatibilityResultRows(),
		Final: true,
	})
}

func compatibilityResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Capability", Type: DataTypeString},
		{Name: "Layer", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Runtime_owned", Type: DataTypeBool},
		{Name: "Adapter_owned", Type: DataTypeBool},
		{Name: "Description", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCompatibilityExchange) compatibilityResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, capability := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(capability.Name),
			metadataStringCell(string(capability.Layer)),
			metadataStringCell(string(capability.Status)),
			metadataBoolCell(capability.RuntimeOwned),
			metadataBoolCell(capability.AdapterOwned),
			metadataStringCell(capability.Description),
		})
	}
	return rows
}

func compatibilityRows(capabilities []CompatibilityCapability) []CompatibilityCapability {
	rows := cloneCompatibilityCapabilities(capabilities)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Layer != rows[j].Layer {
			return rows[i].Layer < rows[j].Layer
		}
		if rows[i].Status != rows[j].Status {
			return rows[i].Status < rows[j].Status
		}
		return rows[i].Name < rows[j].Name
	})
	return rows
}
