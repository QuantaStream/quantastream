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

// NativeProjectionBSIValueReadResult returns BSI values already aligned to the
// requested rownums. It lets local backends avoid constructing retained BSIs
// when the caller only needs materialized values.
type NativeProjectionBSIValueReadResult struct {
	Values []*big.Int
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

// NativeProjectionBSIBatchReader optionally reads several raw BSI vectors that
// share a storage request.
type NativeProjectionBSIBatchReader interface {
	ReadProjectionBSIs(context.Context, []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error)
}

// NativeProjectionBSIValueBatchReader optionally reads BSI-backed values
// directly for late materialization.
type NativeProjectionBSIValueBatchReader interface {
	ReadProjectionBSIValues(context.Context, []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIValueReadResult, qsbridge.DiagnosticSet, error)
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
		return r.readProjectionStringLexRemainderKeys(ctx, request, index, fieldName, attr)
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
	bsiRequest := NativeProjectionBSIReadRequest{
		Index:           index,
		Field:           request.Field,
		PhysicalField:   fieldName,
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
	if valueReader, ok := r.Reader.(NativeProjectionBSIValueBatchReader); ok {
		readResults, readDiagnostics, err := valueReader.ReadProjectionBSIValues(ctx, []NativeProjectionBSIReadRequest{bsiRequest})
		if err != nil || readDiagnostics.BlocksNative() {
			probes := []ExecutionProbe(nil)
			if len(readResults) > 0 {
				probes = readResults[0].Probes
			}
			return NativeProjectionFieldReadResult{Probes: probes}, readDiagnostics, err
		}
		if len(readResults) == 1 {
			result, resultDiagnostics := nativeProjectionBSIFieldResultFromValues(nativeProjectionBSIFieldReadPlan{
				Request:    request,
				BSIRequest: bsiRequest,
				Table:      table,
				Attribute:  attr,
			}, readResults[0])
			return result, resultDiagnostics, nil
		}
	}
	readResult, readDiagnostics, err := r.Reader.ReadProjectionBSI(ctx, bsiRequest)
	if err != nil || readDiagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, readDiagnostics, err
	}
	if readResult.BSI == nil {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, nativeProjectionUnsupported("native BSI projection returned no BSI for " + index + "." + fieldName), nil
	}
	values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
	for _, value := range readResult.BSI.GetBigValues(nativeProjectionRownumColumnIDs(request.Rownums)) {
		if value == nil {
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

// ReadProjectionFields batches simple BSI-backed projection fields when they
// share the same candidate rownums and time window. Unsupported field shapes
// deliberately fall back to ReadProjectionField so the single-field contract
// remains authoritative.
func (r NativeProjectionBSIFieldReader) ReadProjectionFields(ctx context.Context, requests []NativeProjectionFieldReadRequest) ([]NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	results := make([]NativeProjectionFieldReadResult, len(requests))
	pending := make([]nativeProjectionBSIFieldReadPlan, 0, len(requests))
	var diagnostics qsbridge.DiagnosticSet
	for i, request := range requests {
		plan, ok := r.nativeProjectionBSIFieldReadPlan(request, i)
		if ok {
			pending = append(pending, plan)
			continue
		}
		result, readDiagnostics, err := r.ReadProjectionField(ctx, request)
		results[i] = result
		diagnostics = append(diagnostics, readDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return results, diagnostics, err
		}
	}
	if len(pending) == 0 {
		return results, diagnostics, nil
	}
	batchReader, batchOK := r.Reader.(NativeProjectionBSIBatchReader)
	valueReader, valueOK := r.Reader.(NativeProjectionBSIValueBatchReader)
	for _, group := range nativeProjectionBSIFieldReadPlanGroups(pending) {
		if valueOK {
			valueRequests := make([]NativeProjectionBSIReadRequest, 0, len(group))
			for _, plan := range group {
				valueRequests = append(valueRequests, plan.BSIRequest)
			}
			readResults, readDiagnostics, err := valueReader.ReadProjectionBSIValues(ctx, valueRequests)
			diagnostics = append(diagnostics, readDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return results, diagnostics, err
			}
			if len(readResults) != len(group) {
				diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					"native BSI value batch reader returned "+strconv.Itoa(len(readResults))+" field reads for "+strconv.Itoa(len(group))+" requests",
				))
				return results, diagnostics, nil
			}
			for i, readResult := range readResults {
				result, resultDiagnostics := nativeProjectionBSIFieldResultFromValues(group[i], readResult)
				results[group[i].Position] = result
				diagnostics = append(diagnostics, resultDiagnostics...)
				if diagnostics.BlocksNative() {
					return results, diagnostics, nil
				}
			}
			continue
		}
		if batchOK && len(group) > 1 {
			bsiRequests := make([]NativeProjectionBSIReadRequest, 0, len(group))
			for _, plan := range group {
				bsiRequests = append(bsiRequests, plan.BSIRequest)
			}
			readResults, readDiagnostics, err := batchReader.ReadProjectionBSIs(ctx, bsiRequests)
			diagnostics = append(diagnostics, readDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return results, diagnostics, err
			}
			if len(readResults) != len(group) {
				diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					"native BSI batch reader returned "+strconv.Itoa(len(readResults))+" field reads for "+strconv.Itoa(len(group))+" requests",
				))
				return results, diagnostics, nil
			}
			for i, readResult := range readResults {
				result, resultDiagnostics := nativeProjectionBSIFieldResult(group[i], readResult)
				results[group[i].Position] = result
				diagnostics = append(diagnostics, resultDiagnostics...)
				if diagnostics.BlocksNative() {
					return results, diagnostics, nil
				}
			}
			continue
		}
		for _, plan := range group {
			readResult, readDiagnostics, err := r.Reader.ReadProjectionBSI(ctx, plan.BSIRequest)
			diagnostics = append(diagnostics, readDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return results, diagnostics, err
			}
			result, resultDiagnostics := nativeProjectionBSIFieldResult(plan, readResult)
			results[plan.Position] = result
			diagnostics = append(diagnostics, resultDiagnostics...)
			if diagnostics.BlocksNative() {
				return results, diagnostics, nil
			}
		}
	}
	return results, diagnostics, nil
}

// ReadProjectionExpression reads one derived BSI-backed projection expression.
func (r NativeProjectionBSIFieldReader) ReadProjectionExpression(ctx context.Context, request NativeProjectionExpressionReadRequest) (NativeProjectionExpressionReadResult, qsbridge.DiagnosticSet, error) {
	call, sourceField, ok := nativeProjectionYearFieldExpression(request.Expression.Expr)
	if !ok {
		return NativeProjectionExpressionReadResult{}, nativeProjectionUnsupported("native expression projection does not yet support " + nativeProjectionExpressionLabel(request.Expression.Expr)), nil
	}
	index := sourceField.Table.Table
	if index == "" {
		index = request.Index
	}
	fieldReadRequest := NativeProjectionFieldReadRequest{
		Index:           index,
		Field:           nativeProjectionFieldFromRef(index, sourceField),
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
	plan, ok := r.nativeProjectionBSIFieldReadPlan(fieldReadRequest, 0)
	if !ok {
		return NativeProjectionExpressionReadResult{}, nativeProjectionUnsupported("native expression projection cannot read source field " + index + "." + nativeProjectionPhysicalField(fieldReadRequest.Field)), nil
	}
	if !nativeProjectionAttributeIsTimeBSI(plan.Attribute) {
		return NativeProjectionExpressionReadResult{}, nativeProjectionUnsupported("native expression projection year() requires TimestampBSI source field " + index + "." + nativeProjectionPhysicalField(fieldReadRequest.Field)), nil
	}
	values, probes, diagnostics, err := r.readProjectionBSIValues(ctx, plan)
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionExpressionReadResult{Probes: probes}, diagnostics, err
	}
	cells, valueDiagnostics := nativeProjectionYearCells(plan.Table, plan.Attribute, values)
	if valueDiagnostics.BlocksNative() {
		return NativeProjectionExpressionReadResult{Probes: probes}, valueDiagnostics, nil
	}
	output := request.Expression.Output
	if output.Field == "" && output.PhysicalName == "" {
		output = nativeProjectionYearOutputField(index, sourceField)
	}
	return NativeProjectionExpressionReadResult{
		Expression: qsbridge.QuantaProjectionExpression{
			Expr:   request.Expression.Expr,
			Output: output,
		},
		Values: cells,
		Probes: append(probes,
			nativeProjectionExpressionRowsProbe(index, nativeProjectionPhysicalField(fieldReadRequest.Field), call.Name, len(request.Rownums)),
			nativeProjectionExpressionFunctionProbe(index, nativeProjectionPhysicalField(fieldReadRequest.Field), call.Name),
		),
	}, nil, nil
}

type nativeProjectionBSIFieldReadPlan struct {
	Position   int
	Request    NativeProjectionFieldReadRequest
	BSIRequest NativeProjectionBSIReadRequest
	Table      *core.Table
	Attribute  *core.Attribute
}

func (r NativeProjectionBSIFieldReader) nativeProjectionBSIFieldReadPlan(request NativeProjectionFieldReadRequest, position int) (nativeProjectionBSIFieldReadPlan, bool) {
	index := strings.TrimSpace(request.Index)
	if index == "" {
		index = strings.TrimSpace(request.Field.Index)
	}
	fieldName := nativeProjectionPhysicalField(request.Field)
	if index == "" || fieldName == "" || nativeProjectionRownumPseudoField(fieldName) || len(request.Rownums) == 0 || r.Reader == nil {
		return nativeProjectionBSIFieldReadPlan{}, false
	}
	table := legacyDirectRelationshipCachedTable(r.TableCache, index)
	if table == nil {
		return nativeProjectionBSIFieldReadPlan{}, false
	}
	attr, diagnostics := nativeProjectionAttribute(table, fieldName)
	if diagnostics.BlocksNative() ||
		nativeProjectionAttributeIsStringEnum(attr) ||
		nativeProjectionAttributeIsBackingString(attr) ||
		nativeProjectionAttributeIsDirectBitmap(attr) ||
		attr == nil ||
		nativeProjectionAttributeRequiresFallback(attr) {
		return nativeProjectionBSIFieldReadPlan{}, false
	}
	return nativeProjectionBSIFieldReadPlan{
		Position: position,
		Request:  request,
		BSIRequest: NativeProjectionBSIReadRequest{
			Index:           index,
			Field:           request.Field,
			PhysicalField:   fieldName,
			Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			FromEpochMillis: request.FromEpochMillis,
			ToEpochMillis:   request.ToEpochMillis,
		},
		Table:     table,
		Attribute: attr,
	}, true
}

func nativeProjectionBSIFieldReadPlanGroups(plans []nativeProjectionBSIFieldReadPlan) [][]nativeProjectionBSIFieldReadPlan {
	groups := make([][]nativeProjectionBSIFieldReadPlan, 0, len(plans))
	for _, plan := range plans {
		matched := false
		for i := range groups {
			if nativeProjectionBSIFieldReadPlanSameGroup(groups[i][0], plan) {
				groups[i] = append(groups[i], plan)
				matched = true
				break
			}
		}
		if !matched {
			groups = append(groups, []nativeProjectionBSIFieldReadPlan{plan})
		}
	}
	return groups
}

func nativeProjectionBSIFieldReadPlanSameGroup(left, right nativeProjectionBSIFieldReadPlan) bool {
	return strings.EqualFold(left.BSIRequest.Index, right.BSIRequest.Index) &&
		left.BSIRequest.FromEpochMillis == right.BSIRequest.FromEpochMillis &&
		left.BSIRequest.ToEpochMillis == right.BSIRequest.ToEpochMillis &&
		nativeProjectionSameRownums(left.BSIRequest.Rownums, right.BSIRequest.Rownums)
}

func nativeProjectionSameRownums(left, right []qsbridge.QuantaRownum) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func nativeProjectionBSIFieldResult(plan nativeProjectionBSIFieldReadPlan, readResult NativeProjectionBSIReadResult) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet) {
	if readResult.BSI == nil {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, nativeProjectionUnsupported("native BSI projection returned no BSI for " + plan.BSIRequest.Index + "." + plan.BSIRequest.PhysicalField)
	}
	values := make([]qsbridge.ResultCell, 0, len(plan.BSIRequest.Rownums))
	for _, value := range readResult.BSI.GetBigValues(nativeProjectionRownumColumnIDs(plan.BSIRequest.Rownums)) {
		if value == nil {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		values = append(values, nativeProjectionBSICell(plan.Table, plan.Attribute, plan.Request.Field, value))
	}
	return NativeProjectionFieldReadResult{
		Field:  plan.Request.Field,
		Values: values,
		Probes: readResult.Probes,
	}, nil
}

func nativeProjectionBSIFieldResultFromValues(plan nativeProjectionBSIFieldReadPlan, readResult NativeProjectionBSIValueReadResult) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet) {
	if len(readResult.Values) != len(plan.BSIRequest.Rownums) {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native BSI value projection returned "+strconv.Itoa(len(readResult.Values))+" values for "+strconv.Itoa(len(plan.BSIRequest.Rownums))+" rownums"),
		}
	}
	values := make([]qsbridge.ResultCell, 0, len(readResult.Values))
	for _, value := range readResult.Values {
		if value == nil {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		values = append(values, nativeProjectionBSICell(plan.Table, plan.Attribute, plan.Request.Field, value))
	}
	return NativeProjectionFieldReadResult{
		Field:  plan.Request.Field,
		Values: values,
		Probes: readResult.Probes,
	}, nil
}

func (r NativeProjectionBSIFieldReader) readProjectionBSIValues(ctx context.Context, plan nativeProjectionBSIFieldReadPlan) ([]*big.Int, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if valueReader, ok := r.Reader.(NativeProjectionBSIValueBatchReader); ok {
		readResults, diagnostics, err := valueReader.ReadProjectionBSIValues(ctx, []NativeProjectionBSIReadRequest{plan.BSIRequest})
		if err != nil || diagnostics.BlocksNative() {
			probes := []ExecutionProbe(nil)
			if len(readResults) > 0 {
				probes = readResults[0].Probes
			}
			return nil, probes, diagnostics, err
		}
		if len(readResults) != 1 {
			return nil, nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native BSI value projection returned "+strconv.Itoa(len(readResults))+" field reads for expression projection"),
			}, nil
		}
		if len(readResults[0].Values) != len(plan.BSIRequest.Rownums) {
			return nil, readResults[0].Probes, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native BSI value projection returned "+strconv.Itoa(len(readResults[0].Values))+" values for "+strconv.Itoa(len(plan.BSIRequest.Rownums))+" rownums"),
			}, nil
		}
		return append([]*big.Int(nil), readResults[0].Values...), readResults[0].Probes, nil, nil
	}
	readResult, diagnostics, err := r.Reader.ReadProjectionBSI(ctx, plan.BSIRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, readResult.Probes, diagnostics, err
	}
	if readResult.BSI == nil {
		return nil, readResult.Probes, nativeProjectionUnsupported("native BSI projection returned no BSI for " + plan.BSIRequest.Index + "." + plan.BSIRequest.PhysicalField), nil
	}
	values := readResult.BSI.GetBigValues(nativeProjectionRownumColumnIDs(plan.BSIRequest.Rownums))
	return values, readResult.Probes, nil, nil
}

func nativeProjectionYearCells(table *core.Table, attr *core.Attribute, values []*big.Int) ([]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	cells := make([]qsbridge.ResultCell, 0, len(values))
	for _, value := range values {
		if value == nil {
			cells = append(cells, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		nanos := legacyDirectRelationshipEncodedTimeToNanos(table, attr.FieldName, value.Int64())
		cells = append(cells, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(time.Unix(0, nanos).UTC().Year())})
	}
	return cells, nil
}

func nativeProjectionRownumColumnIDs(rownums []qsbridge.QuantaRownum) []uint64 {
	columnIDs := make([]uint64, len(rownums))
	for i, rownum := range rownums {
		columnIDs[i] = uint64(rownum)
	}
	return columnIDs
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

func (r NativeProjectionBSIFieldReader) readProjectionStringLexRemainderKeys(ctx context.Context, request NativeProjectionFieldReadRequest, index string, fieldName string, attr *core.Attribute) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	lookupRef := index + "." + fieldName
	if len(request.Rownums) == 0 {
		return NativeProjectionFieldReadResult{
			Field:      request.Field,
			Encoded:    true,
			LookupKind: NativeProjectionLookupBackingString,
			LookupRef:  lookupRef,
		}, nil, nil
	}
	if r.Reader == nil {
		return NativeProjectionFieldReadResult{}, nativeProjectionUnsupported("native StringLexBSI projection has no BSI reader for " + index + "." + fieldName), nil
	}
	bsiRequest := NativeProjectionBSIReadRequest{
		Index:           index,
		Field:           request.Field,
		PhysicalField:   fieldName,
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Rownums...),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
	if valueReader, ok := r.Reader.(NativeProjectionBSIValueBatchReader); ok {
		readResults, diagnostics, err := valueReader.ReadProjectionBSIValues(ctx, []NativeProjectionBSIReadRequest{bsiRequest})
		if err != nil || diagnostics.BlocksNative() {
			probes := []ExecutionProbe(nil)
			if len(readResults) > 0 {
				probes = readResults[0].Probes
			}
			return NativeProjectionFieldReadResult{Probes: probes}, diagnostics, err
		}
		if len(readResults) == 1 {
			result, resultDiagnostics := nativeProjectionStringLexRemainderResultFromValues(request, bsiRequest, attr, readResults[0])
			return result, resultDiagnostics, nil
		}
	}
	readResult, diagnostics, err := r.Reader.ReadProjectionBSI(ctx, bsiRequest)
	if err != nil || diagnostics.BlocksNative() {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, diagnostics, err
	}
	return nativeProjectionStringLexRemainderResultFromBSI(request, bsiRequest, attr, readResult)
}

func nativeProjectionStringLexRemainderResultFromBSI(request NativeProjectionFieldReadRequest, bsiRequest NativeProjectionBSIReadRequest, attr *core.Attribute, readResult NativeProjectionBSIReadResult) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	if readResult.BSI == nil {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, nativeProjectionUnsupported("native StringLexBSI projection returned no BSI for " + bsiRequest.Index + "." + bsiRequest.PhysicalField), nil
	}
	return nativeProjectionStringLexRemainderResultFromBigValues(request, bsiRequest, attr, readResult.BSI.GetBigValues(nativeProjectionRownumColumnIDs(bsiRequest.Rownums)), readResult.Probes)
}

func nativeProjectionStringLexRemainderResultFromValues(request NativeProjectionFieldReadRequest, bsiRequest NativeProjectionBSIReadRequest, attr *core.Attribute, readResult NativeProjectionBSIValueReadResult) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet) {
	if len(readResult.Values) != len(bsiRequest.Rownums) {
		return NativeProjectionFieldReadResult{Probes: readResult.Probes}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native StringLexBSI value projection returned "+strconv.Itoa(len(readResult.Values))+" values for "+strconv.Itoa(len(bsiRequest.Rownums))+" rownums"),
		}
	}
	result, diagnostics, _ := nativeProjectionStringLexRemainderResultFromBigValues(request, bsiRequest, attr, readResult.Values, readResult.Probes)
	return result, diagnostics
}

func nativeProjectionStringLexRemainderResultFromBigValues(request NativeProjectionFieldReadRequest, bsiRequest NativeProjectionBSIReadRequest, attr *core.Attribute, bigValues []*big.Int, probes []ExecutionProbe) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	if len(bigValues) != len(bsiRequest.Rownums) {
		return NativeProjectionFieldReadResult{Probes: probes}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native StringLexBSI projection returned "+strconv.Itoa(len(bigValues))+" values for "+strconv.Itoa(len(bsiRequest.Rownums))+" rownums"),
		}, nil
	}
	values := make([]qsbridge.ResultCell, 0, len(bigValues))
	for i, value := range bigValues {
		if value == nil {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil})
			continue
		}
		values = append(values, qsbridge.ResultCell{
			Kind: qsbridge.ValueInt,
			Value: NativeProjectionStringRemainderKey{
				RowNum: uint64(bsiRequest.Rownums[i]),
				Prefix: nativeProjectionStringLexRender(attr, value),
			},
		})
	}
	return NativeProjectionFieldReadResult{
		Field:      request.Field,
		Values:     values,
		Encoded:    true,
		LookupKind: NativeProjectionLookupBackingString,
		LookupRef:  bsiRequest.Index + "." + bsiRequest.PhysicalField,
		Probes:     probes,
	}, nil, nil
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

// LegacyDirectProjectionBSIReader reads native projection BSIs through the direct BitIndex.
type LegacyDirectProjectionBSIReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// ReadProjectionBSI projects one BSI-backed field through BitIndex.Projection.
func (r LegacyDirectProjectionBSIReader) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results, diagnostics, err := r.ReadProjectionBSIs(ctx, []NativeProjectionBSIReadRequest{request})
	if len(results) == 0 {
		return NativeProjectionBSIReadResult{}, diagnostics, err
	}
	return results[0], diagnostics, err
}

// ReadProjectionBSIs projects several BSI-backed fields through one or more
// BitIndex.Projection calls when their candidate rownums and time windows align.
func (r LegacyDirectProjectionBSIReader) ReadProjectionBSIs(ctx context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results := make([]NativeProjectionBSIReadResult, len(requests))
	if len(requests) == 0 {
		return results, nil, nil
	}
	if r.Source == nil {
		return results, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection BSI reader has no source"),
		}, nil
	}
	cache := directProjectionBSICacheFromContext(ctx)
	works := make([]nativeProjectionBSIReadWork, 0, len(requests))
	for i, request := range requests {
		fromTime, toTime := nativeProjectionWindowNanos(r.TableCache, request)
		foundSet := legacyDirectRelationshipBitmap(request.Rownums)
		cacheKey := directProjectionBSICacheKeyFor(request, fromTime, toTime)
		detail := directProjectionBSICacheInstrumentationDetail(cacheKey, foundSet)
		cachedBSI, mode, ok := cache.Get(cacheKey, foundSet)
		if ok {
			recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", true, mode, detail)
			results[i] = NativeProjectionBSIReadResult{
				BSI: cachedBSI,
				Probes: []ExecutionProbe{
					directProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
					directProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
					directProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
				},
			}
			continue
		}
		recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", false, mode, detail)
		work := nativeProjectionBSIReadWork{
			Position:     i,
			Request:      request,
			FromTime:     fromTime,
			ToTime:       toTime,
			FoundSet:     foundSet,
			FetchSet:     foundSet,
			FetchRownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			CacheKey:     cacheKey,
			CacheDetail:  detail,
			CacheMode:    mode,
		}
		if mode == "coverage_miss" {
			partial, partialMode, partialOK := cache.GetPartial(cacheKey, foundSet)
			if partialOK {
				recordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache_partial", true, partialMode, directProjectionBSIPartialCacheDetail(detail, partial))
				if partial.MissingCardinality() == 0 {
					results[i] = NativeProjectionBSIReadResult{
						BSI: partial.BSI,
						Probes: []ExecutionProbe{
							directProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
							directProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
							directProjectionBSICacheModeProbe(request.Index, request.PhysicalField, partialMode),
						},
					}
					continue
				}
				work.FetchSet = partial.MissingRownumSet
				work.FetchRownums = partial.MissingRownums()
				work.Partial = partial
				work.PartialOK = true
			}
		}
		works = append(works, work)
	}
	var diagnostics qsbridge.DiagnosticSet
	for _, group := range nativeProjectionBSIReadWorkGroups(works) {
		groupDiagnostics, err := r.readProjectionBSIWorkGroup(ctx, group, cache, results)
		diagnostics = append(diagnostics, groupDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return results, diagnostics, err
		}
	}
	return results, diagnostics, nil
}

type nativeProjectionBSIReadWork struct {
	Position     int
	Request      NativeProjectionBSIReadRequest
	FromTime     int64
	ToTime       int64
	FoundSet     *roaring64.Bitmap
	FetchSet     *roaring64.Bitmap
	FetchRownums []qsbridge.QuantaRownum
	CacheKey     ProjectionBSICacheKey
	CacheDetail  string
	CacheMode    string
	Partial      ProjectionBSICachePartial
	PartialOK    bool
}

func nativeProjectionBSIReadWorkGroups(works []nativeProjectionBSIReadWork) [][]nativeProjectionBSIReadWork {
	groups := make([][]nativeProjectionBSIReadWork, 0, len(works))
	for _, work := range works {
		matched := false
		for i := range groups {
			if nativeProjectionBSIReadWorkSameGroup(groups[i][0], work) {
				groups[i] = append(groups[i], work)
				matched = true
				break
			}
		}
		if !matched {
			groups = append(groups, []nativeProjectionBSIReadWork{work})
		}
	}
	return groups
}

func nativeProjectionBSIReadWorkSameGroup(left, right nativeProjectionBSIReadWork) bool {
	return strings.EqualFold(left.Request.Index, right.Request.Index) &&
		left.FromTime == right.FromTime &&
		left.ToTime == right.ToTime &&
		nativeProjectionSameRownums(left.FetchRownums, right.FetchRownums)
}

func (r LegacyDirectProjectionBSIReader) readProjectionBSIWorkGroup(ctx context.Context, group []nativeProjectionBSIReadWork, cache *DirectProjectionBSICache, results []NativeProjectionBSIReadResult) (qsbridge.DiagnosticSet, error) {
	if len(group) == 0 {
		return nil, nil
	}
	request := group[0].Request
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
		return diagnostics, err
	}
	if session == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection BSI reader received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection BSI reader has no bitmap index"),
		}, nil
	}
	fields := make([]string, 0, len(group))
	for _, work := range group {
		fields = append(fields, work.Request.PhysicalField)
	}
	projectionStart := time.Now()
	bsiByField, _, err := legacySession.Session.BitIndex.Projection(request.Index, fields, group[0].FromTime, group[0].ToTime, group[0].FetchSet, false)
	projectionElapsed := time.Since(projectionStart)
	if err != nil {
		return nil, err
	}
	for _, work := range group {
		bsi := bsiByField[work.Request.PhysicalField]
		if bsi == nil {
			bsi = roaring64.NewDefaultBSI()
		}
		if work.PartialOK {
			bsi = work.Partial.MergeFetchedMissing(bsi)
		}
		cache.Set(work.CacheKey, work.FoundSet, bsi)
		recordQueryScratchpadCacheStore(ctx, "projection_bsi_cache", work.CacheDetail)
		results[work.Position] = NativeProjectionBSIReadResult{
			BSI: bsi,
			Probes: []ExecutionProbe{
				directProjectionBSIRowsProbe(work.Request.Index, work.Request.PhysicalField, len(work.Request.Rownums)),
				directProjectionBSIFetchRowsProbe(work.Request.Index, work.Request.PhysicalField, len(work.FetchRownums)),
				directProjectionBSICacheProbe(work.Request.Index, work.Request.PhysicalField, false),
				directProjectionBSICacheModeProbe(work.Request.Index, work.Request.PhysicalField, work.CacheMode),
				directProjectionBSIBatchFieldsProbe(work.Request.Index, work.Request.PhysicalField, len(group)),
				directProjectionBSIBatchElapsedProbe(work.Request.Index, work.Request.PhysicalField, projectionElapsed),
			},
		}
	}
	return nil, nil
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

func directProjectionBSIBatchFieldsProbe(index, field string, fields int) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_batch_fields",
		Value:   strconv.Itoa(fields),
		Detail:  index + "." + field,
	}
}

func directProjectionBSIBatchElapsedProbe(index, field string, elapsed time.Duration) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "bsi_projection_batch_elapsed",
		Value:   elapsed.String(),
		Detail:  index + "." + field,
	}
}

// LegacyDirectProjectionDictionaryIDReader reads StringEnum dictionary IDs
// through the direct BitIndex projection path.
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
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection dictionary reader has no source"),
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
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection dictionary reader received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return NativeProjectionDictionaryIDReadResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "projection dictionary reader has no bitmap index"),
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
	case "stringenum":
		return true
	case "stringlexbsi":
		return nativeProjectionStringLexNeedsRemainder(attr)
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
	return nativeProjectionStringLexNeedsRemainder(attr)
}

func nativeProjectionStringLexNeedsRemainder(attr *core.Attribute) bool {
	if attr == nil || !strings.EqualFold(strings.TrimSpace(attr.MappingStrategy), "StringLexBSI") {
		return false
	}
	prefixLength := nativeProjectionStringLexPrefixLength(attr.MapperConfig)
	if prefixLength <= 0 {
		return false
	}
	return attr.Size <= 0 || attr.Size > prefixLength
}

func nativeProjectionStringLexPrefixLength(config map[string]string) int {
	for _, key := range []string{"length", "prefixLength", "chars", "characters"} {
		if raw, ok := config[key]; ok {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err == nil {
				return value
			}
			return 0
		}
	}
	return 0
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
	case qsbridge.DataTypeString:
		if strings.EqualFold(strings.TrimSpace(attr.MappingStrategy), "StringLexBSI") {
			return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: nativeProjectionStringLexRender(attr, value)}
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Int64()}
	case qsbridge.DataTypeTime:
		nanos := legacyDirectRelationshipEncodedTimeToNanos(table, attr.FieldName, value.Int64())
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: time.Unix(0, nanos).UTC()}
	case qsbridge.DataTypeBool:
		return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: value.Sign() != 0}
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Int64()}
	}
}

func nativeProjectionStringLexRender(attr *core.Attribute, value *big.Int) string {
	if attr == nil {
		return ""
	}
	mapper, err := core.ResolveMapper(attr)
	if err != nil || mapper == nil {
		return ""
	}
	return mapper.Render(attr, value)
}

func nativeProjectionYearFieldExpression(expr qsbridge.Expr) (qsbridge.CallExpr, qsbridge.FieldRef, bool) {
	call, ok := nativeProjectionCallExpr(expr)
	if !ok || !nativeProjectionFunctionIsYear(call.Name) || len(call.Args) != 1 {
		return qsbridge.CallExpr{}, qsbridge.FieldRef{}, false
	}
	field, ok := nativeProjectionFieldExpr(call.Args[0])
	return call, field, ok
}

func nativeProjectionCallExpr(expr qsbridge.Expr) (qsbridge.CallExpr, bool) {
	switch typed := expr.(type) {
	case qsbridge.CallExpr:
		return typed, true
	case *qsbridge.CallExpr:
		if typed != nil {
			return *typed, true
		}
	}
	return qsbridge.CallExpr{}, false
}

func nativeProjectionFieldExpr(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return typed.Ref, true
	case *qsbridge.FieldExpr:
		if typed != nil {
			return typed.Ref, true
		}
	}
	return qsbridge.FieldRef{}, false
}

func nativeProjectionFunctionIsYear(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "year", "yy":
		return true
	default:
		return false
	}
}

func nativeProjectionFieldFromRef(defaultIndex string, field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	index := field.Table.Table
	if index == "" {
		index = defaultIndex
	}
	name := field.PhysicalName
	if name == "" {
		name = field.Name
	}
	return qsbridge.QuantaProjectionField{
		Index:        index,
		Role:         qsbridge.TableInstanceID(materializationFieldRole(defaultIndex, field)),
		Field:        name,
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        field.Roles,
	}
}

func nativeProjectionYearOutputField(defaultIndex string, field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	source := nativeProjectionFieldFromRef(defaultIndex, field)
	name := source.Field
	if name == "" {
		name = source.PhysicalName
	}
	source.Field = "year_" + name
	source.PhysicalName = source.Field
	source.Type = qsbridge.DataTypeInt
	source.Visible = false
	return source
}

func nativeProjectionAttributeIsTimeBSI(attr *core.Attribute) bool {
	if attr == nil {
		return false
	}
	profile := qsbridge.LegacyEncodingProfile(attr.MappingStrategy, qsbridge.LegacyEncodingOptions{
		Granularity: LegacyTimeBSIGranularity(attr.MapperConfig),
	})
	return profile.Kind == qsbridge.EncodingTimeBSI
}

func nativeProjectionExpressionLabel(expr qsbridge.Expr) string {
	if call, ok := nativeProjectionCallExpr(expr); ok {
		parts := make([]string, 0, len(call.Args))
		for _, arg := range call.Args {
			parts = append(parts, nativeProjectionExpressionLabel(arg))
		}
		return strings.ToLower(call.Name) + "(" + strings.Join(parts, ",") + ")"
	}
	if field, ok := nativeProjectionFieldExpr(expr); ok {
		name := field.PhysicalName
		if name == "" {
			name = field.Name
		}
		if ref := field.Table.RefName(); ref != "" {
			return ref + "." + name
		}
		return name
	}
	if expr == nil {
		return "<nil>"
	}
	return string(expr.ExpressionKind())
}

func nativeProjectionExpressionRowsProbe(index, field, function string, rows int) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "expression_projection_rows",
		Value:   strconv.Itoa(rows),
		Detail:  index + "." + strings.ToLower(function) + "(" + field + ")",
	}
}

func nativeProjectionExpressionFunctionProbe(index, field, function string) ExecutionProbe {
	return ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "expression_projection_function",
		Value:   strings.ToLower(function),
		Detail:  index + "." + field,
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
