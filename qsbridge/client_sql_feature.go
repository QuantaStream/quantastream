package qsbridge

import "sort"

// ClientSQLFeatureExchange is adapter-facing SQL feature matrix metadata.
type ClientSQLFeatureExchange struct {
	Connection   ConnectionContext
	Matrix       SQLFeatureMatrix
	Rows         []SQLFeature
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientSQLFeatures returns protocol-neutral rows for the qsbridge SQL feature matrix.
func (s PlanningService) ListClientSQLFeatures(connection ConnectionContext) ClientSQLFeatureExchange {
	matrix := s.SQLFeatureMatrix()
	exchange := ClientSQLFeatureExchange{
		Connection:  cloneConnectionContext(connection),
		Matrix:      matrix.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), matrix.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = sqlFeatureRows(matrix.Features)
	}
	exchange.Result = exchange.sqlFeatureResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether SQL feature metadata can be returned.
func (e ClientSQLFeatureExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts SQL feature diagnostics into protocol-facing errors.
func (e ClientSQLFeatureExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking SQL feature error, if any.
func (e ClientSQLFeatureExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSQLFeatureExchange) sqlFeatureResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sqlFeatureResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sqlFeatureResultRows(),
		Final: true,
	})
}

func sqlFeatureResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Feature", Type: DataTypeString},
		{Name: "Category", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Description", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSQLFeatureExchange) sqlFeatureResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, feature := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(feature.Name),
			metadataStringCell(string(feature.Category)),
			metadataStringCell(string(feature.Status)),
			metadataStringCell(joinPlanCapabilities(feature.Capabilities)),
			metadataStringCell(joinDiagnosticCodes(feature.Diagnostics)),
			metadataStringCell(feature.Description),
		})
	}
	return rows
}

func sqlFeatureRows(features []SQLFeature) []SQLFeature {
	rows := cloneSQLFeatures(features)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Category != rows[j].Category {
			return rows[i].Category < rows[j].Category
		}
		if rows[i].Status != rows[j].Status {
			return rows[i].Status < rows[j].Status
		}
		return rows[i].Name < rows[j].Name
	})
	return rows
}
