package qsruntime

import (
	"context"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// NativeProjectionFieldReadRequest is one storage-neutral native field-vector read.
type NativeProjectionFieldReadRequest struct {
	Index           string
	Field           qsbridge.QuantaProjectionField
	Rownums         []qsbridge.QuantaRownum
	FromEpochMillis int64
	ToEpochMillis   int64
}

// NativeProjectionFieldReadResult is one native field-vector read response.
type NativeProjectionFieldReadResult struct {
	Field      qsbridge.QuantaProjectionField
	Values     []qsbridge.ResultCell
	Encoded    bool
	Dictionary string
	LookupKind NativeProjectionLookupKind
	LookupRef  string
	Probes     []ExecutionProbe
}

// NativeProjectionFieldReader reads one projected field for candidate rownums.
type NativeProjectionFieldReader interface {
	ReadProjectionField(context.Context, NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionFieldReaderFunc adapts a function to NativeProjectionFieldReader.
type NativeProjectionFieldReaderFunc func(context.Context, NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error)

// ReadProjectionField calls f(ctx, request).
func (f NativeProjectionFieldReaderFunc) ReadProjectionField(ctx context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// NativeProjectionLookupKind identifies the side lookup needed to rehydrate encoded values.
type NativeProjectionLookupKind string

const (
	// NativeProjectionLookupUnknown means no side lookup kind has been selected.
	NativeProjectionLookupUnknown NativeProjectionLookupKind = ""
	// NativeProjectionLookupDictionary means encoded values are StringEnum dictionary ids.
	NativeProjectionLookupDictionary NativeProjectionLookupKind = "dictionary"
	// NativeProjectionLookupBackingString means encoded values require a backing string lookup.
	NativeProjectionLookupBackingString NativeProjectionLookupKind = "backing_string"
)

// NativeProjectionValueRehydrationRequest is one encoded-to-visible value conversion request.
type NativeProjectionValueRehydrationRequest struct {
	Index      string
	Field      qsbridge.QuantaProjectionField
	Dictionary string
	LookupKind NativeProjectionLookupKind
	LookupRef  string
	Values     []qsbridge.ResultCell
}

// NativeProjectionValueRehydrationResult is one rehydrated value-vector response.
type NativeProjectionValueRehydrationResult struct {
	Values []qsbridge.ResultCell
	Probes []ExecutionProbe
}

// NativeProjectionValueRehydrator converts encoded values to SQL-visible cells.
type NativeProjectionValueRehydrator interface {
	RehydrateProjectionValues(context.Context, NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionValueRehydratorFunc adapts a function to NativeProjectionValueRehydrator.
type NativeProjectionValueRehydratorFunc func(context.Context, NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error)

// RehydrateProjectionValues calls f(ctx, request).
func (f NativeProjectionValueRehydratorFunc) RehydrateProjectionValues(ctx context.Context, request NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// NativeProjectionCompositeRehydrator routes encoded projection cells to the
// right side lookup implementation.
type NativeProjectionCompositeRehydrator struct {
	Dictionary     NativeProjectionValueRehydrator
	BackingStrings NativeProjectionBackingStringLookupReader
}

// RehydrateProjectionValues converts encoded cells to SQL-visible values.
func (r NativeProjectionCompositeRehydrator) RehydrateProjectionValues(ctx context.Context, request NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
	switch request.LookupKind {
	case NativeProjectionLookupBackingString:
		if r.BackingStrings == nil {
			return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "backing-string rehydrator has no lookup reader for "+request.Index+"."+request.Field.Field),
			}, nil
		}
		result, diagnostics, err := r.BackingStrings.LookupProjectionBackingStrings(ctx, NativeProjectionBackingStringLookupRequest{
			Index:     request.Index,
			Field:     request.Field,
			LookupRef: request.LookupRef,
			Values:    append([]qsbridge.ResultCell(nil), request.Values...),
		})
		return NativeProjectionValueRehydrationResult{Values: result.Values, Probes: result.Probes}, diagnostics, err
	case NativeProjectionLookupDictionary, NativeProjectionLookupUnknown:
		if r.Dictionary == nil {
			return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "dictionary rehydrator has no resolver for "+request.Index+"."+request.Field.Field),
			}, nil
		}
		return r.Dictionary.RehydrateProjectionValues(ctx, request)
	default:
		return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "native projection rehydrator cannot handle lookup kind "+string(request.LookupKind)),
		}, nil
	}
}

// NativeProjectionMaterializationKernel materializes projection fields through native field readers.
type NativeProjectionMaterializationKernel struct {
	Reader     NativeProjectionFieldReader
	Rehydrator NativeProjectionValueRehydrator
}

// MaterializeProjectionBatches executes grouped native field-vector reads.
func (k NativeProjectionMaterializationKernel) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	result := ProjectionMaterializationKernelResult{
		ID: request.ID,
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    request.ProbePrefix + "request_count",
			Value:   strconv.Itoa(request.RequestCount()),
		}},
	}
	if k.Reader == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"native projection materialization has no field reader",
		))
		return result, nil
	}
	for _, materializationRequest := range request.Requests {
		item, diagnostics, err := k.materializeOne(ctx, materializationRequest)
		result.Results = append(result.Results, item)
		result.Probes = append(result.Probes, item.Probes...)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (k NativeProjectionMaterializationKernel) materializeOne(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (ProjectionMaterializationResult, qsbridge.DiagnosticSet, error) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:        request.Index,
		LogicalShard: request.LogicalShard,
		Replica:      request.Replica,
		DependencyID: request.DependencyID,
		Batch:        request.Batch,
		Rownums:      append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	}
	item := ProjectionMaterializationResult{
		ID:      request.DependencyID,
		Request: request,
	}
	var diagnostics qsbridge.DiagnosticSet
	for _, field := range request.ProjectionFields {
		read := NativeProjectionFieldReadRequest{
			Index:           request.Index,
			Field:           field,
			Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			FromEpochMillis: request.FromEpochMillis,
			ToEpochMillis:   request.ToEpochMillis,
		}
		readResult, readDiagnostics, err := k.Reader.ReadProjectionField(ctx, read)
		diagnostics = append(diagnostics, readDiagnostics...)
		item.Probes = append(item.Probes, readResult.Probes...)
		if err != nil || diagnostics.BlocksNative() {
			item.Diagnostics = diagnostics
			return item, diagnostics, err
		}
		values := append([]qsbridge.ResultCell(nil), readResult.Values...)
		if readResult.Encoded {
			if k.Rehydrator == nil {
				diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticUnsupportedSQL,
					qsbridge.PhaseExecute,
					"native projection materialization requires a value rehydrator for "+field.Field,
				))
				item.Diagnostics = diagnostics
				return item, diagnostics, nil
			}
			rehydrated, rehydrateDiagnostics, err := k.Rehydrator.RehydrateProjectionValues(ctx, NativeProjectionValueRehydrationRequest{
				Index:      request.Index,
				Field:      field,
				Dictionary: readResult.Dictionary,
				LookupKind: nativeProjectionLookupKind(readResult),
				LookupRef:  nativeProjectionLookupRef(readResult),
				Values:     values,
			})
			diagnostics = append(diagnostics, rehydrateDiagnostics...)
			item.Probes = append(item.Probes, rehydrated.Probes...)
			if err != nil || diagnostics.BlocksNative() {
				item.Diagnostics = diagnostics
				return item, diagnostics, err
			}
			values = rehydrated.Values
		}
		if len(values) != len(request.Rownums) {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"native projection materialization returned "+strconv.Itoa(len(values))+" values for "+strconv.Itoa(len(request.Rownums))+" rownums",
			))
			item.Diagnostics = diagnostics
			return item, diagnostics, nil
		}
		if readResult.Field.Field != "" || readResult.Field.PhysicalName != "" {
			field = readResult.Field
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field:  field,
			Values: values,
		})
	}
	item.RowSet = rowSet
	item.Diagnostics = diagnostics
	return item, diagnostics, nil
}

func nativeProjectionLookupKind(readResult NativeProjectionFieldReadResult) NativeProjectionLookupKind {
	if readResult.LookupKind != NativeProjectionLookupUnknown {
		return readResult.LookupKind
	}
	if readResult.Dictionary != "" {
		return NativeProjectionLookupDictionary
	}
	return NativeProjectionLookupUnknown
}

func nativeProjectionLookupRef(readResult NativeProjectionFieldReadResult) string {
	if readResult.LookupRef != "" {
		return readResult.LookupRef
	}
	return readResult.Dictionary
}

// NativeProjectionDictionaryLabelRehydrator maps encoded dictionary ids to SQL string labels.
//
// It is intentionally in-memory and deterministic. Runtime adapters can later
// replace it with KVStore-backed or cache-backed implementations without
// changing the native materialization contract.
type NativeProjectionDictionaryLabelRehydrator struct {
	Catalog  qsbridge.QueryCatalogView
	Resolver qsbridge.DictionaryResolver
	Labels   map[string]map[int64]string
}

// NativeProjectionBackingStringLookupRequest asks for original strings by encoded backing-store ids.
type NativeProjectionBackingStringLookupRequest struct {
	Index     string
	Field     qsbridge.QuantaProjectionField
	LookupRef string
	Values    []qsbridge.ResultCell
}

// NativeProjectionBackingStringLookupResult returns original strings for backing-store ids.
type NativeProjectionBackingStringLookupResult struct {
	Values []qsbridge.ResultCell
	Probes []ExecutionProbe
}

// NativeProjectionBackingStringLookupReader resolves StringHashBSI/backing-string ids.
type NativeProjectionBackingStringLookupReader interface {
	LookupProjectionBackingStrings(context.Context, NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionBackingStringLookupReaderFunc adapts a function to NativeProjectionBackingStringLookupReader.
type NativeProjectionBackingStringLookupReaderFunc func(context.Context, NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error)

// LookupProjectionBackingStrings calls f(ctx, request).
func (f NativeProjectionBackingStringLookupReaderFunc) LookupProjectionBackingStrings(ctx context.Context, request NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// RehydrateProjectionValues converts dictionary id cells to string labels.
func (r NativeProjectionDictionaryLabelRehydrator) RehydrateProjectionValues(_ context.Context, request NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
	lookupRef := request.LookupRef
	if lookupRef == "" {
		lookupRef = request.Dictionary
	}
	if request.LookupKind != NativeProjectionLookupDictionary && request.LookupKind != NativeProjectionLookupUnknown {
		return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "dictionary label rehydrator cannot handle lookup kind "+string(request.LookupKind)),
		}, nil
	}
	if r.Resolver != nil {
		ref, ok := r.dictionaryRef(request)
		if !ok {
			return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseExecute, "dictionary reference not available for "+request.Index+"."+request.Field.Field),
			}, nil
		}
		return r.rehydrateProjectionValuesWithResolver(request, ref)
	}
	labels := r.Labels[lookupRef]
	if labels == nil {
		return NativeProjectionValueRehydrationResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "dictionary labels not available for "+lookupRef),
		}, nil
	}
	result := NativeProjectionValueRehydrationResult{
		Values: make([]qsbridge.ResultCell, 0, len(request.Values)),
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "dictionary_rehydration_values",
			Value:   strconv.Itoa(len(request.Values)),
			Detail:  lookupRef,
		}},
	}
	for _, value := range request.Values {
		if value.Kind == qsbridge.ValueNull {
			result.Values = append(result.Values, qsbridge.ResultCell{Kind: qsbridge.ValueNull})
			continue
		}
		id, ok := nativeProjectionDictionaryID(value)
		if !ok {
			return NativeProjectionValueRehydrationResult{Probes: result.Probes}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "dictionary label rehydrator received non-integer id"),
			}, nil
		}
		label, ok := labels[id]
		if !ok {
			return NativeProjectionValueRehydrationResult{Probes: result.Probes}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "dictionary label not found for "+lookupRef+" id="+strconv.FormatInt(id, 10)),
			}, nil
		}
		result.Values = append(result.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: label})
	}
	return result, nil, nil
}

func (r NativeProjectionDictionaryLabelRehydrator) rehydrateProjectionValuesWithResolver(request NativeProjectionValueRehydrationRequest, ref qsbridge.DictionaryRef) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
	result := NativeProjectionValueRehydrationResult{
		Values: make([]qsbridge.ResultCell, 0, len(request.Values)),
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "dictionary_resolver_rehydration_values",
			Value:   strconv.Itoa(len(request.Values)),
			Detail:  ref.QualifiedName(),
		}},
	}
	for _, value := range request.Values {
		if value.Kind == qsbridge.ValueNull {
			result.Values = append(result.Values, qsbridge.ResultCell{Kind: qsbridge.ValueNull})
			continue
		}
		id, ok := nativeProjectionDictionaryID(value)
		if !ok || id < 0 {
			return NativeProjectionValueRehydrationResult{Probes: result.Probes}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "dictionary resolver rehydrator received invalid integer id"),
			}, nil
		}
		entry, diagnostics := r.Resolver.LookupID(ref, qsbridge.StringEnumID(id))
		if diagnostics.BlocksNative() {
			return NativeProjectionValueRehydrationResult{Probes: result.Probes}, diagnostics, nil
		}
		result.Values = append(result.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: entry.Label})
	}
	return result, nil, nil
}

func (r NativeProjectionDictionaryLabelRehydrator) dictionaryRef(request NativeProjectionValueRehydrationRequest) (qsbridge.DictionaryRef, bool) {
	if ref, ok := nativeProjectionDictionaryRefFromCatalog(r.Catalog, request.Index, request.Field); ok {
		return ref, true
	}
	if ref, ok := nativeProjectionDictionaryRefFromLookup(request.LookupRef); ok {
		return ref, true
	}
	return nativeProjectionDictionaryRefFromLookup(request.Dictionary)
}

func nativeProjectionDictionaryRefFromCatalog(catalog qsbridge.QueryCatalogView, defaultIndex string, field qsbridge.QuantaProjectionField) (qsbridge.DictionaryRef, bool) {
	index := field.Index
	if index == "" {
		index = defaultIndex
	}
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	for _, table := range catalog.Tables {
		if !strings.EqualFold(table.Name, index) {
			continue
		}
		for _, definition := range table.Fields {
			if !strings.EqualFold(definition.Name, name) && (definition.PhysicalName == "" || !strings.EqualFold(definition.PhysicalName, name)) {
				continue
			}
			if definition.Dictionary.Ref.Valid() {
				return definition.Dictionary.Ref, true
			}
			return qsbridge.DictionaryRef{Schema: table.Schema, Table: table.Name, Field: definition.Name}, true
		}
	}
	return qsbridge.DictionaryRef{}, false
}

func nativeProjectionDictionaryRefFromLookup(lookupRef string) (qsbridge.DictionaryRef, bool) {
	parts := strings.Split(lookupRef, ".")
	switch len(parts) {
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return qsbridge.DictionaryRef{}, false
		}
		return qsbridge.DictionaryRef{Table: parts[0], Field: parts[1]}, true
	case 3:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return qsbridge.DictionaryRef{}, false
		}
		return qsbridge.DictionaryRef{Schema: parts[0], Table: parts[1], Field: parts[2]}, true
	default:
		return qsbridge.DictionaryRef{}, false
	}
}

func nativeProjectionDictionaryID(value qsbridge.ResultCell) (int64, bool) {
	switch typed := value.Value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(typed), true
	default:
		return 0, false
	}
}

// FallbackProjectionMaterializationKernel prefers a native kernel and falls back to a compatibility kernel.
type FallbackProjectionMaterializationKernel struct {
	Preferred ProjectionMaterializationKernel
	Fallback  ProjectionMaterializationKernel
}

// MaterializeProjectionBatches tries Preferred first and uses Fallback for unsupported native diagnostics.
func (k FallbackProjectionMaterializationKernel) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	if k.Preferred != nil {
		result, err := k.Preferred.MaterializeProjectionBatches(ctx, request)
		if err != nil || !result.Diagnostics.BlocksNative() || !projectionMaterializationDiagnosticsAreUnsupported(result.Diagnostics) {
			return result, err
		}
		if k.Fallback != nil {
			fallbackResult, fallbackErr := k.Fallback.MaterializeProjectionBatches(ctx, request)
			fallbackResult.Probes = append(projectionMaterializationFallbackProbes(request, result.Diagnostics), fallbackResult.Probes...)
			return fallbackResult, fallbackErr
		}
		return result, err
	}
	if k.Fallback == nil {
		return UnsupportedProjectionMaterializationKernel{}.MaterializeProjectionBatches(ctx, request)
	}
	return k.Fallback.MaterializeProjectionBatches(ctx, request)
}

func projectionMaterializationDiagnosticsAreUnsupported(diagnostics qsbridge.DiagnosticSet) bool {
	if len(diagnostics) == 0 {
		return false
	}
	for _, diagnostic := range diagnostics {
		if diagnostic.Code != qsbridge.DiagnosticUnsupportedSQL {
			return false
		}
	}
	return true
}

func projectionMaterializationFallbackProbes(request ProjectionMaterializationKernelRequest, diagnostics qsbridge.DiagnosticSet) []ExecutionProbe {
	return []ExecutionProbe{
		{
			Section: "native_projection_materialization",
			Name:    request.ProbePrefix + "fallback_to_compat",
			Value:   "true",
		},
		{
			Section: "native_projection_materialization",
			Name:    request.ProbePrefix + "fallback_diagnostic_count",
			Value:   strconv.Itoa(len(diagnostics)),
		},
	}
}
