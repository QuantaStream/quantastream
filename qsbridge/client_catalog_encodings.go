package qsbridge

// ClientCatalogEncoding describes one adapter-visible field encoding profile.
type ClientCatalogEncoding struct {
	Schema                 string
	Table                  string
	Field                  string
	PhysicalName           string
	Type                   DataType
	Index                  IndexKind
	Encoding               EncodingKind
	LegacyName             string
	Multiplicity           ValueMultiplicity
	Rehydration            RehydrationKind
	RehydrationStore       string
	PredicateCapabilities  PredicateCapabilities
	ProjectionCapabilities ProjectionCapabilities
	TimeGranularity        TimeGranularity
	Scale                  int
	Signed                 bool
	PrefixLength           int
	MaxLength              int
	RemainderStore         string
	Searchable             bool
	SearchMode             string
}

// ClientCatalogEncodingExchange is client-facing encoding metadata for one table.
type ClientCatalogEncodingExchange struct {
	Connection   ConnectionContext
	Schema       string
	Table        string
	Encodings    []ClientCatalogEncoding
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogEncodings returns field encoding metadata for one table.
func (s PlanningService) ListClientCatalogEncodings(connection ConnectionContext, catalog Catalog, schema string, table string) ClientCatalogEncodingExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogEncodingExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Table:       table,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogEncodingResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "encoding metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogEncodingResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "encoding metadata requires a table name"),
		})
		exchange.Result = exchange.catalogEncodingResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	definition, diagnostics := catalog.Table(schema, table)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Encodings = tableCatalogEncodings(definition)
	}
	exchange.Result = exchange.catalogEncodingResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether encoding metadata can be returned.
func (e ClientCatalogEncodingExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts encoding metadata diagnostics into protocol-facing errors.
func (e ClientCatalogEncodingExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking encoding metadata error, if any.
func (e ClientCatalogEncodingExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogEncodingExchange) catalogEncodingResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogEncodingResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogEncodingRows(),
		Final: true,
	})
}

func catalogEncodingResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Table", Type: DataTypeString},
		{Name: "Field", Type: DataTypeString},
		{Name: "Physical_name", Type: DataTypeString, Nullable: true},
		{Name: "Type", Type: DataTypeString},
		{Name: "Index", Type: DataTypeString, Nullable: true},
		{Name: "Encoding", Type: DataTypeString, Nullable: true},
		{Name: "Legacy_name", Type: DataTypeString, Nullable: true},
		{Name: "Multiplicity", Type: DataTypeString},
		{Name: "Rehydration", Type: DataTypeString, Nullable: true},
		{Name: "Rehydration_store", Type: DataTypeString, Nullable: true},
		{Name: "Predicate_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Projection_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Time_granularity", Type: DataTypeString, Nullable: true},
		{Name: "Scale", Type: DataTypeInt},
		{Name: "Signed", Type: DataTypeBool},
		{Name: "Prefix_length", Type: DataTypeInt},
		{Name: "Max_length", Type: DataTypeInt},
		{Name: "Remainder_store", Type: DataTypeString, Nullable: true},
		{Name: "Searchable", Type: DataTypeBool},
		{Name: "Search_mode", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCatalogEncodingExchange) catalogEncodingRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Encodings))
	for _, encoding := range e.Encodings {
		rows = append(rows, ResultRow{
			metadataStringCell(encoding.Schema),
			metadataStringCell(encoding.Table),
			metadataStringCell(encoding.Field),
			metadataStringCell(encoding.PhysicalName),
			metadataStringCell(string(encoding.Type)),
			metadataStringCell(string(encoding.Index)),
			metadataStringCell(string(encoding.Encoding)),
			metadataStringCell(encoding.LegacyName),
			metadataStringCell(string(encoding.Multiplicity)),
			metadataStringCell(string(encoding.Rehydration)),
			metadataStringCell(encoding.RehydrationStore),
			metadataStringCell(joinPredicateCapabilities(encoding.PredicateCapabilities)),
			metadataStringCell(joinProjectionCapabilities(encoding.ProjectionCapabilities)),
			metadataStringCell(string(encoding.TimeGranularity)),
			metadataIntCell(encoding.Scale),
			metadataBoolCell(encoding.Signed),
			metadataIntCell(encoding.PrefixLength),
			metadataIntCell(encoding.MaxLength),
			metadataStringCell(encoding.RemainderStore),
			metadataBoolCell(encoding.Searchable),
			metadataStringCell(encoding.SearchMode),
		})
	}
	return rows
}

func tableCatalogEncodings(table TableDefinition) []ClientCatalogEncoding {
	encodings := make([]ClientCatalogEncoding, 0, len(table.Fields))
	for _, field := range table.Fields {
		profile := field.Encoding
		encodings = append(encodings, ClientCatalogEncoding{
			Schema:                 table.Schema,
			Table:                  table.Name,
			Field:                  field.Name,
			PhysicalName:           field.PhysicalName,
			Type:                   field.Type,
			Index:                  field.Index,
			Encoding:               profile.Kind,
			LegacyName:             profile.LegacyName,
			Multiplicity:           profile.EffectiveMultiplicity(),
			Rehydration:            profile.Rehydration.Kind,
			RehydrationStore:       profile.Rehydration.Store,
			PredicateCapabilities:  append(PredicateCapabilities(nil), profile.PredicateCapabilities...),
			ProjectionCapabilities: append(ProjectionCapabilities(nil), profile.ProjectionCapabilities...),
			TimeGranularity:        profile.Granularity,
			Scale:                  profile.Scale,
			Signed:                 profile.Signed,
			PrefixLength:           profile.PrefixLength,
			MaxLength:              profile.MaxLength,
			RemainderStore:         profile.RemainderStore,
			Searchable:             profile.Searchable(),
			SearchMode:             profile.Search.Mode,
		})
	}
	return encodings
}
