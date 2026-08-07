package qsruntime

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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

// NativeProjectionBatchFieldReader optionally reads several projection fields
// that share a materialization request. Implementations can use this to collapse
// multiple storage calls while the single-field reader remains the baseline
// contract.
type NativeProjectionBatchFieldReader interface {
	ReadProjectionFields(context.Context, []NativeProjectionFieldReadRequest) ([]NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error)
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

// NativeProjectionStringRemainderKey carries the BSI-decoded StringLex prefix
// together with the rownum needed to look up the KV suffix.
type NativeProjectionStringRemainderKey struct {
	RowNum uint64
	Prefix string
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
		return NativeProjectionValueRehydrationResult{
			Values: nativeProjectionJoinStringLexRemainders(request.Values, result.Values),
			Probes: result.Probes,
		}, diagnostics, err
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

func nativeProjectionJoinStringLexRemainders(encoded []qsbridge.ResultCell, suffixes []qsbridge.ResultCell) []qsbridge.ResultCell {
	if len(encoded) != len(suffixes) {
		return suffixes
	}
	values := append([]qsbridge.ResultCell(nil), suffixes...)
	for i, encodedCell := range encoded {
		prefix, ok := nativeProjectionStringLexRemainderPrefix(encodedCell)
		if !ok {
			continue
		}
		if suffixes[i].Kind == qsbridge.ValueNull {
			values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
			continue
		}
		values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: prefix + nativeProjectionStringSuffixValue(suffixes[i])}
	}
	return values
}

func nativeProjectionStringLexRemainderPrefix(cell qsbridge.ResultCell) (string, bool) {
	switch value := cell.Value.(type) {
	case NativeProjectionStringRemainderKey:
		return value.Prefix, true
	case *NativeProjectionStringRemainderKey:
		if value == nil {
			return "", false
		}
		return value.Prefix, true
	default:
		return "", false
	}
}

func nativeProjectionStringSuffixValue(cell qsbridge.ResultCell) string {
	switch value := cell.Value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

// NativeProjectionMaterializationKernel materializes projection fields through native field readers.
type NativeProjectionMaterializationKernel struct {
	Reader     NativeProjectionFieldReader
	Rehydrator NativeProjectionValueRehydrator
}

// MaterializeProjectionBatches executes grouped native field-vector reads.
func (k NativeProjectionMaterializationKernel) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (result ProjectionMaterializationKernelResult, err error) {
	start := time.Now()
	result = ProjectionMaterializationKernelResult{
		ID: request.ID,
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    request.ProbePrefix + "request_count",
			Value:   strconv.Itoa(request.RequestCount()),
		}},
	}
	defer func() {
		result.Probes = append(result.Probes, ExecutionProbe{
			Section: "native_projection_materialization",
			Name:    request.ProbePrefix + "elapsed",
			Value:   time.Since(start).String(),
			Detail:  "requests=" + strconv.Itoa(request.RequestCount()),
		})
	}()
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
	var timingProbes []ExecutionProbe
	states := make([]nativeProjectionMaterializationFieldState, 0, len(request.ProjectionFields))
	readStateIndexes := []int{}
	for _, field := range request.ProjectionFields {
		valueCache := ProjectionValueCacheFromContext(ctx)
		valueCacheKey := ProjectionValueCacheKeyFor(request.Index, field, request.FromEpochMillis, request.ToEpochMillis)
		valueCacheDetail := nativeProjectionValueCacheDetail(valueCacheKey, len(request.Rownums))
		cachedValues, valueCacheMode, valueCacheHit := valueCache.Get(valueCacheKey, request.Rownums)
		recordQueryScratchpadCacheLookup(ctx, "projection_value_cache", valueCacheHit, valueCacheMode, valueCacheDetail)
		item.Probes = append(item.Probes, nativeProjectionValueCacheProbes(request, field, valueCacheHit, valueCacheMode, cachedValues)...)
		if valueCacheHit && cachedValues.MissingCount() == 0 {
			states = append(states, nativeProjectionMaterializationFieldState{
				Field:    field,
				Values:   cachedValues.Values,
				Complete: true,
			})
			continue
		}
		readRownums := append([]qsbridge.QuantaRownum(nil), request.Rownums...)
		if valueCacheHit {
			readRownums = append([]qsbridge.QuantaRownum(nil), cachedValues.MissingRownums...)
		}
		read := NativeProjectionFieldReadRequest{
			Index:           request.Index,
			Field:           field,
			Rownums:         readRownums,
			FromEpochMillis: request.FromEpochMillis,
			ToEpochMillis:   request.ToEpochMillis,
		}
		readProbeRequest := request
		readProbeRequest.Rownums = readRownums
		states = append(states, nativeProjectionMaterializationFieldState{
			Field:            field,
			ValueCache:       valueCache,
			ValueCacheKey:    valueCacheKey,
			CachedValues:     cachedValues,
			ValueCacheHit:    valueCacheHit,
			ReadRequest:      read,
			ReadProbeRequest: readProbeRequest,
		})
		readStateIndexes = append(readStateIndexes, len(states)-1)
	}
	if len(readStateIndexes) > 0 {
		readRequests := make([]NativeProjectionFieldReadRequest, 0, len(readStateIndexes))
		for _, index := range readStateIndexes {
			readRequests = append(readRequests, states[index].ReadRequest)
		}
		readResults, readElapsed, readDiagnostics, err := k.readProjectionFields(ctx, readRequests)
		diagnostics = append(diagnostics, readDiagnostics...)
		for i, readResult := range readResults {
			if i >= len(readStateIndexes) {
				break
			}
			state := &states[readStateIndexes[i]]
			state.ReadResult = readResult
			if i < len(readElapsed) {
				state.ReadElapsed = readElapsed[i]
			}
			item.Probes = append(item.Probes, readResult.Probes...)
		}
		if err != nil || diagnostics.BlocksNative() {
			item.Diagnostics = diagnostics
			return item, diagnostics, err
		}
		if len(readResults) != len(readRequests) {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"native projection materialization returned "+strconv.Itoa(len(readResults))+" field reads for "+strconv.Itoa(len(readRequests))+" requests",
			))
			item.Diagnostics = diagnostics
			return item, diagnostics, nil
		}
	}
	for i := range states {
		state := &states[i]
		field := state.Field
		if state.Complete {
			rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
				Field:  field,
				Values: state.Values,
			})
			continue
		}
		timingProbes = append(timingProbes, nativeProjectionMaterializationFieldProbe("field_read_elapsed", state.ReadElapsed.String(), state.ReadProbeRequest, field))
		readResult := state.ReadResult
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
			rehydrateStart := time.Now()
			rehydrated, rehydrateDiagnostics, err := k.Rehydrator.RehydrateProjectionValues(ctx, NativeProjectionValueRehydrationRequest{
				Index:      request.Index,
				Field:      field,
				Dictionary: readResult.Dictionary,
				LookupKind: nativeProjectionLookupKind(readResult),
				LookupRef:  nativeProjectionLookupRef(readResult),
				Values:     values,
			})
			rehydrateElapsed := time.Since(rehydrateStart)
			timingProbes = append(timingProbes, nativeProjectionMaterializationFieldProbe("field_rehydration_elapsed", rehydrateElapsed.String(), state.ReadProbeRequest, field))
			diagnostics = append(diagnostics, rehydrateDiagnostics...)
			item.Probes = append(item.Probes, rehydrated.Probes...)
			if err != nil || diagnostics.BlocksNative() {
				item.Diagnostics = diagnostics
				return item, diagnostics, err
			}
			values = rehydrated.Values
		}
		if len(values) != len(state.ReadRequest.Rownums) {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"native projection materialization returned "+strconv.Itoa(len(values))+" values for "+strconv.Itoa(len(state.ReadRequest.Rownums))+" rownums",
			))
			item.Diagnostics = diagnostics
			return item, diagnostics, nil
		}
		if readResult.Field.Field != "" || readResult.Field.PhysicalName != "" {
			field = readResult.Field
		}
		state.ValueCache.Set(state.ValueCacheKey, state.ReadRequest.Rownums, values)
		recordQueryScratchpadCacheStore(ctx, "projection_value_cache", nativeProjectionValueCacheDetail(state.ValueCacheKey, len(state.ReadRequest.Rownums)))
		if state.ValueCacheHit {
			merged, ok := nativeProjectionMergeCachedValues(state.CachedValues, values)
			if !ok {
				diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					"native projection value cache could not merge partial values for "+field.Field,
				))
				item.Diagnostics = diagnostics
				return item, diagnostics, nil
			}
			values = merged
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field:  field,
			Values: values,
		})
	}
	item.Probes = append(item.Probes, timingProbes...)
	item.RowSet = rowSet
	item.Diagnostics = diagnostics
	return item, diagnostics, nil
}

type nativeProjectionMaterializationFieldState struct {
	Field            qsbridge.QuantaProjectionField
	ValueCache       *ProjectionValueCache
	ValueCacheKey    ProjectionValueCacheKey
	CachedValues     ProjectionValueCacheLookup
	ValueCacheHit    bool
	Complete         bool
	Values           []qsbridge.ResultCell
	ReadRequest      NativeProjectionFieldReadRequest
	ReadProbeRequest qsbridge.QuantaMaterializationRequest
	ReadResult       NativeProjectionFieldReadResult
	ReadElapsed      time.Duration
}

func (k NativeProjectionMaterializationKernel) readProjectionFields(ctx context.Context, requests []NativeProjectionFieldReadRequest) ([]NativeProjectionFieldReadResult, []time.Duration, qsbridge.DiagnosticSet, error) {
	if len(requests) == 0 {
		return nil, nil, nil, nil
	}
	if batchReader, ok := k.Reader.(NativeProjectionBatchFieldReader); ok && len(requests) > 1 {
		start := time.Now()
		results, diagnostics, err := batchReader.ReadProjectionFields(ctx, requests)
		elapsed := time.Since(start)
		timings := make([]time.Duration, len(requests))
		for i := range timings {
			timings[i] = elapsed
		}
		if err != nil || diagnostics.BlocksNative() || len(results) == len(requests) {
			return results, timings, diagnostics, err
		}
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			"native projection batch reader returned "+strconv.Itoa(len(results))+" field reads for "+strconv.Itoa(len(requests))+" requests",
		))
		return results, timings, diagnostics, nil
	}
	results := make([]NativeProjectionFieldReadResult, 0, len(requests))
	timings := make([]time.Duration, 0, len(requests))
	var diagnostics qsbridge.DiagnosticSet
	for _, request := range requests {
		start := time.Now()
		result, readDiagnostics, err := k.Reader.ReadProjectionField(ctx, request)
		timings = append(timings, time.Since(start))
		results = append(results, result)
		diagnostics = append(diagnostics, readDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return results, timings, diagnostics, err
		}
	}
	return results, timings, diagnostics, nil
}

func nativeProjectionMaterializationFieldProbe(name, value string, request qsbridge.QuantaMaterializationRequest, field qsbridge.QuantaProjectionField) ExecutionProbe {
	fieldName := field.Field
	if fieldName == "" {
		fieldName = field.PhysicalName
	}
	detail := fieldName + " rows=" + strconv.Itoa(len(request.Rownums))
	if request.Index != "" {
		detail = request.Index + "." + detail
	}
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    name,
		Value:   value,
		Detail:  detail,
	}
}

func nativeProjectionValueCacheProbes(request qsbridge.QuantaMaterializationRequest, field qsbridge.QuantaProjectionField, hit bool, mode string, lookup ProjectionValueCacheLookup) []ExecutionProbe {
	value := "false"
	if hit {
		value = "true"
	}
	return []ExecutionProbe{
		nativeProjectionMaterializationFieldProbe("projection_value_cache_hit", value, request, field),
		nativeProjectionMaterializationFieldProbe("projection_value_cache_mode", mode, request, field),
		nativeProjectionMaterializationFieldProbe("projection_value_cache_covered_rows", strconv.Itoa(lookup.CoveredRows), request, field),
		nativeProjectionMaterializationFieldProbe("projection_value_cache_missing_rows", strconv.Itoa(lookup.MissingCount()), request, field),
	}
}

func nativeProjectionValueCacheDetail(key ProjectionValueCacheKey, rows int) string {
	return "index=" + key.Index +
		" field=" + key.Field +
		" rows=" + strconv.Itoa(rows) +
		" from=" + strconv.FormatInt(key.FromEpochMillis, 10) +
		" to=" + strconv.FormatInt(key.ToEpochMillis, 10)
}

func nativeProjectionMergeCachedValues(cached ProjectionValueCacheLookup, fetched []qsbridge.ResultCell) ([]qsbridge.ResultCell, bool) {
	if cached.MissingCount() == 0 {
		return cached.Values, true
	}
	if len(cached.MissingPositions) != len(fetched) {
		return nil, false
	}
	merged := append([]qsbridge.ResultCell(nil), cached.Values...)
	for i, position := range cached.MissingPositions {
		if position < 0 || position >= len(merged) {
			return nil, false
		}
		merged[position] = fetched[i]
	}
	return merged, true
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

// NewNativeProjectionDictionaryLabelRehydrator builds a dictionary-label rehydrator
// with the process-local dictionary cache enabled by default.
func NewNativeProjectionDictionaryLabelRehydrator(catalog qsbridge.QueryCatalogView, resolver qsbridge.DictionaryResolver) NativeProjectionDictionaryLabelRehydrator {
	if resolver != nil {
		if _, ok := resolver.(*qsbridge.CachedDictionaryResolver); !ok {
			resolver = qsbridge.NewCachedDictionaryResolver(resolver)
		}
	}
	return NativeProjectionDictionaryLabelRehydrator{Catalog: catalog, Resolver: resolver}
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

// NativeProjectionBackingStringLookupReader resolves StringLexBSI/backing-string ids.
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
			if definition.Encoding.Kind == qsbridge.EncodingStringEnum {
				return qsbridge.DictionaryRef{Schema: table.Schema, Table: table.Name, Field: definition.Name}, true
			}
			return qsbridge.DictionaryRef{}, false
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
