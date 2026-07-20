package qsruntime

import (
	"context"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// NativeProjectionBSIReadRequest asks storage for one BSI-backed projection field.
type NativeProjectionBSIReadRequest struct {
	Index           string
	Field           qsbridge.QuantaProjectionField
	PhysicalField   string
	Rownums         []qsbridge.QuantaRownum
	FromEpochMillis int64
	ToEpochMillis   int64
}

// NativeProjectionBSIReadResult returns the projected BSI and storage probes.
type NativeProjectionBSIReadResult struct {
	BSI    *roaring64.BSI
	Probes []ExecutionProbe
}

// NativeProjectionDictionaryIDReadRequest asks storage for encoded dictionary ids.
type NativeProjectionDictionaryIDReadRequest struct {
	Index         string
	Field         qsbridge.QuantaProjectionField
	PhysicalField string
	Rownums       []qsbridge.QuantaRownum
}

// NativeProjectionDictionaryIDReadResult returns encoded dictionary ids and probes.
type NativeProjectionDictionaryIDReadResult struct {
	Values []qsbridge.ResultCell
	Probes []ExecutionProbe
}

// NativeProjectionDictionaryIDReader reads encoded dictionary ids for StringEnum projections.
type NativeProjectionDictionaryIDReader interface {
	ReadProjectionDictionaryIDs(context.Context, NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionDictionaryIDReaderFunc adapts a function to NativeProjectionDictionaryIDReader.
type NativeProjectionDictionaryIDReaderFunc func(context.Context, NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error)

// ReadProjectionDictionaryIDs calls f(ctx, request).
func (f NativeProjectionDictionaryIDReaderFunc) ReadProjectionDictionaryIDs(ctx context.Context, request NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// NativeProjectionBSIReader reads raw BSI vectors for native projection materialization.
type NativeProjectionBSIReader interface {
	ReadProjectionBSI(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionBSIReaderFunc adapts a function to NativeProjectionBSIReader.
type NativeProjectionBSIReaderFunc func(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error)

// ReadProjectionBSI calls f(ctx, request).
func (f NativeProjectionBSIReaderFunc) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// NativeProjectionBSIFieldReader materializes simple BSI-backed fields without core.Projector.
type NativeProjectionBSIFieldReader struct {
	TableCache       *core.TableCacheStruct
	Reader           NativeProjectionBSIReader
	DictionaryReader NativeProjectionDictionaryIDReader
}

// ReadProjectionField reads one simple projected field for the requested rownums.
func (r NativeProjectionBSIFieldReader) ReadProjectionField(ctx context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	index := strings.TrimSpace(request.Index)
	if index == "" {
		index = strings.TrimSpace(request.Field.Index)
	}
	fieldName := nativeProjectionPhysicalField(request.Field)
	if index == "" || fieldName == "" {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native BSI projection requires index and field"), nil
	}
	if nativeProjectionRownumPseudoField(fieldName) {
		return nativeProjectionRownumPseudoValues(request), nil, nil
	}
	table := legacyDirectRelationshipCachedTable(r.TableCache, index)
	if table == nil {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native BSI projection has no table metadata for " + index), nil
	}
	attr, diagnostics := nativeProjectionAttribute(table, fieldName)
	if diagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{}, diagnostics, nil
	}
	if nativeProjectionAttributeIsStringEnum(attr) {
		return r.readProjectionDictionaryIDs(ctx, request, index, fieldName)
	}
	if nativeProjectionAttributeIsBackingString(attr) {
		return r.readProjectionBackingStringKeys(request, index, fieldName), nil, nil
	}
	if nativeProjectionAttributeIsDirectBitmap(attr) {
		return r.readProjectionDirectBitmapIDs(ctx, request, index, fieldName, attr)
	}
	if attr == nil || nativeProjectionAttributeRequiresFallback(attr) {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native BSI projection does not yet support " + index + "." + fieldName), nil
	}
	if len(request.Rownums) == 0 {
		return NativeProjectionFieldReadResult{Field: request.Field}, nil, nil
	}
	if r.Reader == nil {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native BSI projection has no BSI reader"), nil
	}
	readResult, readDiagnostics, err := r.Reader.ReadProjectionBSI(ctx, NativeProjectionBSIReadRequest{
		Index:           index,
		Field:           request.Field,
		PhysicalField:   fieldName,
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	})
	if err != nil || readDiagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, readDiagnostics, err
	}
	if readResult.BSI == nil {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, nativeProjectionUnsupported("native BSI projection returned no BSI for " + index + "." + fieldName), nil
	}
	values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
	for _, rownum := range request.Rownums {
		value, ok := readResult.BSI.GetBigValue(uint64(rownum))
		if !ok {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		values = append(values, nativeProjectionBSICell(table, attr, request.Field, value))
	}
	return NativeProjectionFieldReadResult{
		Field:  request.Field,
		Values: values,
		Probes: readResult.Probes,
	}, nil, nil
}

func (r NativeProjectionBSIFieldReader) readProjectionDictionaryIDs(ctx context.Context, request NativeProjectionFieldReadRequest, index string, fieldName string) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	if len(request.Rownums) == 0 {
		lookupRef := index + "." + fieldName
		return NativeProjectionFieldReadResult{Field: request.Field, Encoded: true, Dictionary: lookupRef, LookupKind: NativeProjectionLookupDictionary, LookupRef: lookupRef}, nil, nil
	}
	if r.DictionaryReader == nil {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native StringEnum projection has no dictionary id reader for " + index + "." + fieldName), nil
	}
	readResult, diagnostics, err := r.DictionaryReader.ReadProjectionDictionaryIDs(ctx, NativeProjectionDictionaryIDReadRequest{
		Index:         index,
		Field:         request.Field,
		PhysicalField: fieldName,
		Rownums:       append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	})
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, diagnostics, err
	}
	if len(readResult.Values) != len(request.Rownums) {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native StringEnum projection returned "+strconv.Itoa(len(readResult.Values))+" values for "+strconv.Itoa(len(request.Rownums))+" rownums"),
		}, nil
	}
	return NativeProjectionFieldReadResult{
		Field:      request.Field,
		Values:     append([]qsbridge.ResultCell(nil), readResult.Values...),
		Encoded:    true,
		Dictionary: index + "." + fieldName,
		LookupKind: NativeProjectionLookupDictionary,
		LookupRef:  index + "." + fieldName,
		Probes:     readResult.Probes,
	}, nil, nil
}

func (r NativeProjectionBSIFieldReader) readProjectionBackingStringKeys(request NativeProjectionFieldReadRequest, index string, fieldName string) NativeProjectionFieldReadResult {
	lookupRef := index + "." + fieldName
	values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
	for _, rownum := range request.Rownums {
		values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: uint64(rownum)})
	}
	return NativeProjectionFieldReadResult{
		Field:      request.Field,
		Values:     values,
		Encoded:    true,
		LookupKind: NativeProjectionLookupBackingString,
		LookupRef:  lookupRef,
	}
}

func (r NativeProjectionBSIFieldReader) readProjectionDirectBitmapIDs(ctx context.Context, request NativeProjectionFieldReadRequest, index string, fieldName string, attr *core.Attribute) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	if len(request.Rownums) == 0 {
		return NativeProjectionFieldReadResult{Field: request.Field}, nil, nil
	}
	if r.DictionaryReader == nil {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native direct-bitmap projection has no bitmap id reader for " + index + "." + fieldName), nil
	}
	readResult, diagnostics, err := r.DictionaryReader.ReadProjectionDictionaryIDs(ctx, NativeProjectionDictionaryIDReadRequest{
		Index:         index,
		Field:         request.Field,
		PhysicalField: fieldName,
		Rownums:       append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	})
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, diagnostics, err
	}
	if len(readResult.Values) != len(request.Rownums) {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native direct-bitmap projection returned "+strconv.Itoa(len(readResult.Values))+" values for "+strconv.Itoa(len(request.Rownums))+" rownums"),
		}, nil
	}
	values, valueDiagnostics := nativeProjectionDirectBitmapCells(readResult.Values, attr)
	if valueDiagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, valueDiagnostics, nil
	}
	return NativeProjectionFieldReadResult{
		Field:  request.Field,
		Values: values,
		Probes: readResult.Probes,
	}, nil, nil
}

// LegacyDirectProjectionBSIReader reads native projection BSIs through the inabox-direct BitIndex.
type LegacyDirectProjectionBSIReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// ReadProjectionBSI projects one BSI-backed field through BitIndex.Projection.
func (r LegacyDirectProjectionBSIReader) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	if r.Source == nil {
		return NativeProjectionBSIReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection BSI reader has no source"),
		}, nil
	}
	fromTime, toTime := nativeProjectionWindowNanos(r.TableCache, request)
	foundSet := legacyDirectRelationshipBitmap(request.Rownums)
	cacheKey := directProjectionBSICacheKeyFor(request, fromTime, toTime)
	cache := directProjectionBSICacheFromContext(ctx)
	detail := directProjectionBSICacheInstrumentationDetail(cacheKey, foundSet)
	cachedBSI, mode, ok := cache.Get(cacheKey, foundSet)
	if ok {
		recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", true, mode, detail)
		return NativeProjectionBSIReadResult{
			BSI: cachedBSI,
			Probes: []ExecutionProbe{
				directProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
				directProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
				directProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
			},
		}, nil, nil
	}
	recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", false, mode, detail)
	fetchSet := foundSet
	fetchRownums := request.Rownums
	partial := ProjectionBSICachePartial{}
	partialMode := ""
	partialOK := false
	if mode == "coverage_miss" {
		partial, partialMode, partialOK = cache.GetPartial(cacheKey, foundSet)
		if partialOK {
			recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache_partial", true, partialMode, directProjectionBSIPartialCacheDetail(detail, partial))
			if partial.MissingCardinality() == 0 {
				return NativeProjectionBSIReadResult{
					BSI: partial.BSI,
					Probes: []ExecutionProbe{
						directProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
						directProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
						directProjectionBSICacheModeProbe(request.Index, request.PhysicalField, partialMode),
					},
				}, nil, nil
			}
			fetchSet = partial.MissingRownumSet
			fetchRownums = partial.MissingRownums()
		}
	}
	executionRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     request.Index,
		Field:     request.PhysicalField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	provider := LegacyQuantaSourceSessionProvider{Source: r.Source}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, executionRequest)
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionBSIReadResult{}, diagnostics, err
	}
	if session == nil {
		return NativeProjectionBSIReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection BSI reader received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return NativeProjectionBSIReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection BSI reader has no bitmap index"),
		}, nil
	}
	bsiByField, _, err := legacySession.Session.BitIndex.Projection(request.Index, []string{request.PhysicalField}, fromTime, toTime, fetchSet, false)
	if err != nil {
		return NativeProjectionBSIReadResult{}, nil, err
	}
	bsi := bsiByField[request.PhysicalField]
	if bsi == nil {
		bsi = roaring64.NewDefaultBSI()
	}
	if partialOK {
		bsi = partial.MergeFetchedMissing(bsi)
	}
	cache.Set(cacheKey, foundSet, bsi)
	recordQueryScratchpadCacheStore(ctx, "projection_bsi_cache", detail)
	return NativeProjectionBSIReadResult{
		BSI: bsi,
		Probes: []ExecutionProbe{
			directProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
			directProjectionBSIFetchRowsProbe(request.Index, request.PhysicalField, len(fetchRownums)),
			directProjectionBSICacheProbe(request.Index, request.PhysicalField, false),
			directProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
		},
	}, nil, nil
}

func directProjectionBSICacheInstrumentationDetail(key ProjectionBSICacheKey, rownumSet *roaring64.Bitmap) string {
	rows := uint64(0)
	if rownumSet != nil {
		rows = rownumSet.GetCardinality()
	}
	return "index=" + key.Index +
		" field=" + key.Field +
		" rows=" + strconv.FormatUint(rows, 10) +
		" from=" + strconv.FormatInt(key.FromTimeNanos, 10) +
		" to=" + strconv.FormatInt(key.ToTimeNanos, 10)
}

func directProjectionBSIPartialCacheDetail(detail string, partial ProjectionBSICachePartial) string {
	return detail +
		" covered_rows=" + strconv.FormatUint(partial.CoveredCardinality(), 10) +
		" missing_rows=" + strconv.FormatUint(partial.MissingCardinality(), 10)
}

func directProjectionBSIRowsProbe(index, field string, rows int) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_rows",
		Value:   strconv.Itoa(rows),
		Detail:  index + "." + field,
	}
}

func directProjectionBSIFetchRowsProbe(index, field string, rows int) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_fetch_rows",
		Value:   strconv.Itoa(rows),
		Detail:  index + "." + field,
	}
}

func directProjectionBSICacheProbe(index, field string, hit bool) ExecutionProbe {
	value := "false"
	if hit {
		value = "true"
	}
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_cache_hit",
		Value:   value,
		Detail:  index + "." + field,
	}
}

func directProjectionBSICacheModeProbe(index, field string, mode string) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_cache_mode",
		Value:   mode,
		Detail:  index + "." + field,
	}
}

// LegacyDirectProjectionDictionaryIDReader reads StringEnum dictionary IDs
// through the inabox-direct BitIndex projection path.
type LegacyDirectProjectionDictionaryIDReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// ReadProjectionDictionaryIDs projects standard bitmap rows and converts them
// into encoded dictionary-id cells in the requested rownum order.
func (r LegacyDirectProjectionDictionaryIDReader) ReadProjectionDictionaryIDs(ctx context.Context, request NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error) {
	if len(request.Rownums) == 0 {
		return NativeProjectionDictionaryIDReadResult{}, nil, nil
	}
	if r.Source == nil {
		return NativeProjectionDictionaryIDReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection dictionary reader has no source"),
		}, nil
	}
	executionRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     request.Index,
		Field:     request.PhysicalField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	provider := LegacyQuantaSourceSessionProvider{Source: r.Source}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, executionRequest)
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionDictionaryIDReadResult{}, diagnostics, err
	}
	if session == nil {
		return NativeProjectionDictionaryIDReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection dictionary reader received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return NativeProjectionDictionaryIDReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct projection dictionary reader has no bitmap index"),
		}, nil
	}
	foundSet := legacyDirectRelationshipBitmap(request.Rownums)
	fromTime, toTime := nativeProjectionDictionaryWindowNanos(r.TableCache, request)
	_, bitmapByField, err := legacySession.Session.BitIndex.Projection(request.Index, []string{request.PhysicalField}, fromTime, toTime, foundSet, false)
	if err != nil {
		return NativeProjectionDictionaryIDReadResult{}, nil, err
	}
	values := nativeProjectionDictionaryIDCells(request.Rownums, bitmapByField[request.PhysicalField])
	return NativeProjectionDictionaryIDReadResult{
		Values: values,
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "dictionary_id_projection_rows",
			Value:   strconv.Itoa(len(request.Rownums)),
			Detail:  request.Index + "." + request.PhysicalField,
		}, {
			Section: "native_projection_materialization",
			Name:    "dictionary_id_projection_bitmap_ids",
			Value:   strconv.Itoa(len(bitmapByField[request.PhysicalField])),
			Detail:  request.Index + "." + request.PhysicalField,
		}},
	}, nil, nil
}

func nativeProjectionPhysicalField(field qsbridge.QuantaProjectionField) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Field
}

func nativeProjectionRownumPseudoField(field string) bool {
	return strings.EqualFold(strings.TrimSpace(field), "@rownum")
}

func nativeProjectionRownumPseudoValues(request NativeProjectionFieldReadRequest) NativeProjectionFieldReadResult {
	values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
	for _, rownum := range request.Rownums {
		values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
	}
	return NativeProjectionFieldReadResult{
		Field:  request.Field,
		Values: values,
	}
}

func nativeProjectionAttribute(table *core.Table, field string) (*core.Attribute, qsbridge.DiagnosticSet) {
	if table == nil || field == "" {
		return nil, nativeProjectionUnsupported("native projection attribute lookup requires table and field")
	}
	if attr, err := table.GetAttribute(field); err == nil && attr != nil {
		return attr, nil
	}
	if table.AttributeNameMap != nil {
		for name, attr := range table.AttributeNameMap {
			if strings.EqualFold(name, field) {
				return attr, nil
			}
		}
	}
	for i := range table.Attributes {
		attr := &table.Attributes[i]
		if strings.EqualFold(attr.FieldName, field) || strings.EqualFold(attr.SourceName, field) {
			return attr, nil
		}
	}
	return nil, nativeProjectionUnsupported("native projection could not resolve field metadata for " + field)
}

func nativeProjectionAttributeRequiresFallback(attr *core.Attribute) bool {
	if attr == nil {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(attr.MappingStrategy)) {
	case "stringhashbsi", "stringenum":
		return true
	case "parentrelation":
		return false
	default:
		return !attr.IsBSI()
	}
}

func nativeProjectionAttributeIsStringEnum(attr *core.Attribute) bool {
	if attr == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(attr.MappingStrategy), "StringEnum")
}

func nativeProjectionAttributeIsBackingString(attr *core.Attribute) bool {
	if attr == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(attr.MappingStrategy), "StringHashBSI")
}

func nativeProjectionAttributeIsDirectBitmap(attr *core.Attribute) bool {
	if attr == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(attr.MappingStrategy)) {
	case "booldirect", "intdirect":
		return true
	default:
		return false
	}
}

func nativeProjectionDirectBitmapCells(ids []qsbridge.ResultCell, attr *core.Attribute) ([]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	values := make([]qsbridge.ResultCell, 0, len(ids))
	for _, id := range ids {
		if id.Kind == qsbridge.ValueNull || id.Value == nil {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		encoded, ok := nativeProjectionEncodedBitmapID(id)
		if !ok {
			return nil, nativeProjectionUnsupported("native direct-bitmap projection could not decode bitmap id for " + attr.FieldName)
		}
		switch strings.ToLower(strings.TrimSpace(attr.MappingStrategy)) {
		case "booldirect":
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: encoded == 1})
		default:
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(encoded)})
		}
	}
	return values, nil
}

func nativeProjectionEncodedBitmapID(cell qsbridge.ResultCell) (uint64, bool) {
	switch value := cell.Value.(type) {
	case uint64:
		return value, true
	case uint:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case int64:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int32:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	default:
		return 0, false
	}
}

func nativeProjectionBSICell(table *core.Table, attr *core.Attribute, field qsbridge.QuantaProjectionField, value *big.Int) qsbridge.ResultCell {
	dataType := field.Type
	if dataType == "" {
		dataType = legacyDataType(attr.Type)
	}
	switch dataType {
	case qsbridge.DataTypeFloat:
		scale := attr.Scale
		if scale < 0 {
			scale = 0
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(value.Int64()) / math.Pow10(scale)}
	case qsbridge.DataTypeTime:
		nanos := legacyDirectRelationshipEncodedTimeToNanos(table, attr.FieldName, value.Int64())
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: time.Unix(0, nanos).UTC()}
	case qsbridge.DataTypeBool:
		return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: value.Sign() != 0}
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Int64()}
	}
}

func nativeProjectionWindowNanos(cache *core.TableCacheStruct, request NativeProjectionBSIReadRequest) (int64, int64) {
	if request.FromEpochMillis != 0 || request.ToEpochMillis != 0 {
		return nativeProjectionTimeWindowNanos(request.FromEpochMillis, request.ToEpochMillis)
	}
	table := legacyDirectRelationshipCachedTable(cache, request.Index)
	if !legacyDirectTableHasPhysicalShardWindow(table) {
		return 0, 0
	}
	return legacyDirectRelationshipFullTimeRangeBeginMillis * int64(time.Millisecond),
		legacyDirectRelationshipFullTimeRangeEndMillis * int64(time.Millisecond)
}

func nativeProjectionDictionaryWindowNanos(cache *core.TableCacheStruct, request NativeProjectionDictionaryIDReadRequest) (int64, int64) {
	table := legacyDirectRelationshipCachedTable(cache, request.Index)
	if !legacyDirectTableHasPhysicalShardWindow(table) {
		return 0, 0
	}
	return legacyDirectRelationshipFullTimeRangeBeginMillis * int64(time.Millisecond),
		legacyDirectRelationshipFullTimeRangeEndMillis * int64(time.Millisecond)
}

func nativeProjectionTimeWindowNanos(fromEpochMillis int64, toEpochMillis int64) (int64, int64) {
	if fromEpochMillis == 0 && toEpochMillis == 0 {
		return 0, 0
	}
	return fromEpochMillis * int64(time.Millisecond), toEpochMillis * int64(time.Millisecond)
}

// nativeProjectionDictionaryIDCells converts StringEnum bitmap rows into one
// encoded dictionary-id cell per requested rownum. If a set-valued bitmap field
// contains more than one label for a rownum, the lowest dictionary id wins so
// scalar projection stays deterministic.
func nativeProjectionDictionaryIDCells(rownums []qsbridge.QuantaRownum, bitmaps map[uint64]*roaring64.Bitmap) []qsbridge.ResultCell {
	values := make([]qsbridge.ResultCell, 0, len(rownums))
	if len(rownums) == 0 {
		return values
	}
	ids := make([]uint64, 0, len(bitmaps))
	for id := range bitmaps {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, rownum := range rownums {
		var matched uint64
		found := false
		for _, id := range ids {
			bitmap := bitmaps[id]
			if bitmap != nil && bitmap.Contains(uint64(rownum)) {
				matched = id
				found = true
				break
			}
		}
		if !found {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(matched)})
	}
	return values
}

func nativeProjectionUnsupported(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, message),
	}
}
