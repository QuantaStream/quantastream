package qsbridge

// ClientPackageBoundarySummaryRow describes one future qsbridge package boundary.
type ClientPackageBoundarySummaryRow struct {
	Name                PackageBoundaryName
	SplitPhase          PackageSplitPhase
	SplitOrder          int
	ResponsibilityCount int
	FilePrefixCount     int
	FileNameCount       int
	DependencyCount     int
	Responsibilities    []string
	FilePrefixes        []string
	FileNames           []string
	MayDependOn         []PackageBoundaryName
}

// ClientPackageBoundarySummaryExchange is adapter-facing package-boundary metadata.
type ClientPackageBoundarySummaryExchange struct {
	Connection   ConnectionContext
	Boundaries   []PackageBoundary
	Rows         []ClientPackageBoundarySummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientPackageBoundarySummary returns metadata for the intended qsbridge package split.
func (s PlanningService) ListClientPackageBoundarySummary(connection ConnectionContext) ClientPackageBoundarySummaryExchange {
	_ = s
	boundaries := DefaultPackageBoundaries()
	exchange := ClientPackageBoundarySummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Boundaries:  clonePackageBoundaries(boundaries),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = packageBoundarySummaryRows(boundaries)
	}
	exchange.Result = exchange.packageBoundarySummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether package-boundary metadata can be returned.
func (e ClientPackageBoundarySummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts package-boundary diagnostics into protocol-facing errors.
func (e ClientPackageBoundarySummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking package-boundary summary error, if any.
func (e ClientPackageBoundarySummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPackageBoundarySummaryExchange) packageBoundarySummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     packageBoundarySummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.packageBoundarySummaryRows(),
		Final: true,
	})
}

func packageBoundarySummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Boundary", Type: DataTypeString},
		{Name: "Split_phase", Type: DataTypeString},
		{Name: "Split_order", Type: DataTypeInt},
		{Name: "Responsibility_count", Type: DataTypeInt},
		{Name: "File_prefix_count", Type: DataTypeInt},
		{Name: "File_name_count", Type: DataTypeInt},
		{Name: "Dependency_count", Type: DataTypeInt},
		{Name: "Responsibilities", Type: DataTypeString, Nullable: true},
		{Name: "File_prefixes", Type: DataTypeString, Nullable: true},
		{Name: "File_names", Type: DataTypeString, Nullable: true},
		{Name: "May_depend_on", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPackageBoundarySummaryExchange) packageBoundarySummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Name)),
			metadataStringCell(string(row.SplitPhase)),
			metadataIntCell(row.SplitOrder),
			metadataIntCell(row.ResponsibilityCount),
			metadataIntCell(row.FilePrefixCount),
			metadataIntCell(row.FileNameCount),
			metadataIntCell(row.DependencyCount),
			metadataStringCell(joinStringValues(row.Responsibilities)),
			metadataStringCell(joinStringValues(row.FilePrefixes)),
			metadataStringCell(joinStringValues(row.FileNames)),
			metadataStringCell(joinPackageBoundaryNames(row.MayDependOn)),
		})
	}
	return rows
}

func packageBoundarySummaryRows(boundaries []PackageBoundary) []ClientPackageBoundarySummaryRow {
	rows := make([]ClientPackageBoundarySummaryRow, 0, len(boundaries))
	for _, boundary := range boundaries {
		rows = append(rows, ClientPackageBoundarySummaryRow{
			Name:                boundary.Name,
			SplitPhase:          boundary.SplitPhase,
			SplitOrder:          boundary.SplitOrder,
			ResponsibilityCount: len(boundary.Responsibilities),
			FilePrefixCount:     len(boundary.FilePrefixes),
			FileNameCount:       len(boundary.FileNames),
			DependencyCount:     len(boundary.MayDependOn),
			Responsibilities:    append([]string(nil), boundary.Responsibilities...),
			FilePrefixes:        append([]string(nil), boundary.FilePrefixes...),
			FileNames:           append([]string(nil), boundary.FileNames...),
			MayDependOn:         append([]PackageBoundaryName(nil), boundary.MayDependOn...),
		})
	}
	return rows
}

func joinPackageBoundaryNames(names []PackageBoundaryName) string {
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, string(name))
	}
	return joinStringValues(values)
}
