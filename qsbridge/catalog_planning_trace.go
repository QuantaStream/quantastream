package qsbridge

// CatalogPlanningTrace summarizes catalog evidence used by one planned statement.
//
// The trace is intentionally runtime-neutral: it exposes table, field, encoding,
// dictionary, and capability traits that binding already carried into QueryIR,
// without importing storage packages or choosing an executor.
type CatalogPlanningTrace struct {
	SQL         string
	Kind        QueryKind
	Supported   bool
	Diagnostics DiagnosticSet
	Fields      []CatalogPlanningFieldTrace
}

// CatalogPlanningFieldTrace records catalog-backed planning traits for one required field.
type CatalogPlanningFieldTrace struct {
	Field                  string
	Table                  string
	Name                   string
	PhysicalName           string
	Type                   DataType
	Index                  IndexKind
	Roles                  FieldRole
	Encoding               EncodingKind
	LegacyEncoding         string
	Multiplicity           ValueMultiplicity
	Granularity            TimeGranularity
	Scale                  int
	PrefixLength           int
	MaxLength              int
	RemainderStore         string
	Rehydration            RehydrationKind
	RehydrationStore       string
	RequiresLookup         bool
	Searchable             bool
	SearchMode             string
	Dictionary             string
	DictionaryVersion      DictionaryVersion
	DictionaryCardinality  uint64
	DictionaryUpdateMode   DictionaryUpdateMode
	DictionaryConsistency  DictionaryConsistencyMode
	DictionaryCapabilities []DictionaryCapability
	PredicateCapabilities  []PredicateCapability
	ProjectionCapabilities []ProjectionCapability
}

// CatalogPlanningTrace returns catalog evidence for fields required by the plan.
func (r PlanResult) CatalogPlanningTrace() CatalogPlanningTrace {
	fields := r.Query.RequiredFields()
	trace := CatalogPlanningTrace{
		SQL:         r.SQL,
		Kind:        r.Query.Kind,
		Supported:   r.Supported && !r.Diagnostics.BlocksNative(),
		Diagnostics: cloneDiagnosticSet(r.Diagnostics),
		Fields:      make([]CatalogPlanningFieldTrace, 0, len(fields)),
	}
	if trace.Kind == "" {
		trace.Kind = r.Unbound.Kind
	}
	for _, field := range fields {
		trace.Fields = append(trace.Fields, catalogPlanningFieldTrace(field))
	}
	return trace
}

func catalogPlanningFieldTrace(field FieldRef) CatalogPlanningFieldTrace {
	encoding := field.Encoding
	dictionary := field.Dictionary
	row := CatalogPlanningFieldTrace{
		Field:                  field.QualifiedName(),
		Table:                  field.Table.DisplayName(),
		Name:                   field.Name,
		PhysicalName:           field.PhysicalName,
		Type:                   field.Type,
		Index:                  field.Index,
		Roles:                  field.Roles,
		Encoding:               encoding.Kind,
		LegacyEncoding:         encoding.LegacyName,
		Multiplicity:           encoding.EffectiveMultiplicity(),
		Granularity:            encoding.Granularity,
		Scale:                  encoding.Scale,
		PrefixLength:           encoding.PrefixLength,
		MaxLength:              encoding.MaxLength,
		RemainderStore:         encoding.RemainderStore,
		Rehydration:            encoding.Rehydration.Kind,
		RehydrationStore:       encoding.Rehydration.Store,
		RequiresLookup:         encoding.RequiresLookup(),
		Searchable:             encoding.Searchable(),
		SearchMode:             encoding.Search.Mode,
		DictionaryVersion:      dictionary.Version,
		DictionaryCardinality:  dictionary.Cardinality,
		DictionaryUpdateMode:   dictionary.EffectiveUpdateMode(),
		DictionaryConsistency:  dictionary.EffectiveConsistency(),
		DictionaryCapabilities: append([]DictionaryCapability(nil), dictionary.Capabilities...),
		PredicateCapabilities:  append([]PredicateCapability(nil), encoding.PredicateCapabilities...),
		ProjectionCapabilities: append([]ProjectionCapability(nil), encoding.ProjectionCapabilities...),
	}
	if dictionary.Ref.Valid() {
		row.Dictionary = dictionary.Ref.QualifiedName()
	}
	return row
}
