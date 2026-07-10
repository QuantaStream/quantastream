package qsbridge

// ClientCatalogDictionary describes one adapter-visible dictionary-backed field.
type ClientCatalogDictionary struct {
	Schema               string
	Table                string
	Field                string
	Dictionary           string
	Version              DictionaryVersion
	Cardinality          uint64
	UpdateMode           DictionaryUpdateMode
	Consistency          DictionaryConsistencyMode
	RequiresInvalidation bool
	Capabilities         DictionaryCapabilities
}

// ClientCatalogDictionaryExchange is client-facing dictionary metadata for one table.
type ClientCatalogDictionaryExchange struct {
	Connection   ConnectionContext
	Schema       string
	Table        string
	Dictionaries []ClientCatalogDictionary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogDictionaries returns dictionary metadata for fields in one table.
func (s PlanningService) ListClientCatalogDictionaries(connection ConnectionContext, catalog Catalog, schema string, table string) ClientCatalogDictionaryExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogDictionaryExchange{
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
		exchange.Result = exchange.catalogDictionaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "dictionary metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogDictionaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "dictionary metadata requires a table name"),
		})
		exchange.Result = exchange.catalogDictionaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	definition, diagnostics := catalog.Table(schema, table)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Dictionaries = tableCatalogDictionaries(definition)
	}
	exchange.Result = exchange.catalogDictionaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether dictionary metadata can be returned.
func (e ClientCatalogDictionaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts dictionary metadata diagnostics into protocol-facing errors.
func (e ClientCatalogDictionaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking dictionary metadata error, if any.
func (e ClientCatalogDictionaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogDictionaryExchange) catalogDictionaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogDictionaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogDictionaryRows(),
		Final: true,
	})
}

func catalogDictionaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Table", Type: DataTypeString},
		{Name: "Field", Type: DataTypeString},
		{Name: "Dictionary", Type: DataTypeString},
		{Name: "Version", Type: DataTypeString, Nullable: true},
		{Name: "Cardinality", Type: DataTypeInt},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Stable_ids", Type: DataTypeBool},
		{Name: "Prefix_match", Type: DataTypeBool},
		{Name: "Contains_match", Type: DataTypeBool},
		{Name: "Mutable", Type: DataTypeBool},
		{Name: "Update_mode", Type: DataTypeString},
		{Name: "Consistency", Type: DataTypeString},
		{Name: "Requires_invalidation", Type: DataTypeBool},
	}
}

func (e ClientCatalogDictionaryExchange) catalogDictionaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Dictionaries))
	for _, dictionary := range e.Dictionaries {
		rows = append(rows, ResultRow{
			metadataStringCell(dictionary.Schema),
			metadataStringCell(dictionary.Table),
			metadataStringCell(dictionary.Field),
			metadataStringCell(dictionary.Dictionary),
			metadataStringCell(string(dictionary.Version)),
			metadataIntCell(int(dictionary.Cardinality)),
			metadataStringCell(joinDictionaryCapabilities(dictionary.Capabilities)),
			metadataBoolCell(dictionary.Capabilities.Has(DictionaryCapabilityStableIDs)),
			metadataBoolCell(dictionary.Capabilities.Has(DictionaryCapabilityPrefixMatch)),
			metadataBoolCell(dictionary.Capabilities.Has(DictionaryCapabilityContainsMatch)),
			metadataBoolCell(dictionary.Capabilities.Has(DictionaryCapabilityMutable)),
			metadataStringCell(string(dictionary.UpdateMode)),
			metadataStringCell(string(dictionary.Consistency)),
			metadataBoolCell(dictionary.RequiresInvalidation),
		})
	}
	return rows
}

func tableCatalogDictionaries(table TableDefinition) []ClientCatalogDictionary {
	dictionaries := make([]ClientCatalogDictionary, 0, len(table.Fields))
	for _, field := range table.Fields {
		dictionary := field.Dictionary
		if !dictionaryCatalogVisible(dictionary) {
			continue
		}
		ref := effectiveDictionaryRef(table, field, dictionary.Ref)
		dictionaries = append(dictionaries, ClientCatalogDictionary{
			Schema:               table.Schema,
			Table:                table.Name,
			Field:                field.Name,
			Dictionary:           ref.QualifiedName(),
			Version:              dictionary.Version,
			Cardinality:          dictionary.Cardinality,
			UpdateMode:           dictionary.EffectiveUpdateMode(),
			Consistency:          dictionary.EffectiveConsistency(),
			RequiresInvalidation: dictionary.RequiresInvalidation(),
			Capabilities:         append(DictionaryCapabilities(nil), dictionary.Capabilities...),
		})
	}
	return dictionaries
}

func dictionaryCatalogVisible(dictionary DictionaryDefinition) bool {
	return dictionary.Ref.Valid() || dictionary.Version != "" || dictionary.Cardinality != 0 ||
		dictionary.UpdateMode != DictionaryUpdateUnknown || dictionary.Consistency != DictionaryConsistencyUnknown ||
		len(dictionary.Capabilities) > 0
}

func effectiveDictionaryRef(table TableDefinition, field FieldDefinition, ref DictionaryRef) DictionaryRef {
	if ref.Schema == "" {
		ref.Schema = table.Schema
	}
	if ref.Table == "" {
		ref.Table = table.Name
	}
	if ref.Field == "" {
		ref.Field = field.Name
	}
	return ref
}

func joinDictionaryCapabilities(capabilities DictionaryCapabilities) string {
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}
