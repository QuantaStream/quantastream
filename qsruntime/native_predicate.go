package qsruntime

import (
	"fmt"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// NativePredicateSet carries executor-owned predicates that are not represented
// as SQL residual expression trees.
type NativePredicateSet struct {
	CorrelatedAggregate []CorrelatedAggregatePredicate
}

// Empty reports whether no native predicates are attached.
func (s NativePredicateSet) Empty() bool {
	return len(s.CorrelatedAggregate) == 0
}

// CorrelatedAggregatePredicate applies a per-key aggregate threshold to child
// rows after relationship-vector reduction has aligned the correlated domains.
type CorrelatedAggregatePredicate struct {
	ID         string
	KeyField   qsbridge.FieldRef
	ValueField qsbridge.FieldRef
	Operator   qsbridge.BinaryOp
	Thresholds []CorrelatedAggregateThreshold
}

// CorrelatedAggregateThreshold is one correlated key and computed aggregate threshold.
type CorrelatedAggregateThreshold struct {
	Key       int64
	Threshold float64
}

func appendNativePredicateProjectionFields(fields []qsbridge.QuantaProjectionField, predicates NativePredicateSet) []qsbridge.QuantaProjectionField {
	for _, predicate := range predicates.CorrelatedAggregate {
		fields = appendNativePredicateProjectionField(fields, predicate.KeyField)
		fields = appendNativePredicateProjectionField(fields, predicate.ValueField)
	}
	return fields
}

func appendNativePredicateProjectionField(fields []qsbridge.QuantaProjectionField, ref qsbridge.FieldRef) []qsbridge.QuantaProjectionField {
	field := nativePredicateProjectionField(ref)
	if field.Field == "" {
		return fields
	}
	for _, existing := range fields {
		if existing.Index == field.Index && existing.Role == field.Role && existing.Field == field.Field {
			return fields
		}
	}
	return append(fields, field)
}

func nativePredicateProjectionField(ref qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	index := ref.Table.Table
	role := materializationFieldRole(index, ref)
	name := directBitmapFieldPhysicalName(ref)
	return qsbridge.QuantaProjectionField{
		Index:        index,
		Role:         qsbridge.TableInstanceID(role),
		Field:        name,
		PhysicalName: ref.PhysicalName,
		Type:         ref.Type,
		Roles:        ref.Roles | qsbridge.FieldRoleResidualInput,
	}
}

func filterRowSetByNativePredicates(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, []ExecutionProbe, qsbridge.DiagnosticSet) {
	if request.NativePredicates.Empty() {
		return rowSet, nil, nil
	}
	keep := make([]int, 0, rowSet.CandidateCount())
	for i := 0; i < rowSet.CandidateCount(); i++ {
		matched, diagnostics := evaluateNativePredicatesForRow(request.NativePredicates, rowSet, i)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, nil, diagnostics
		}
		if matched {
			keep = append(keep, i)
		}
	}
	filtered := directBitmapFilterRowSetByIndexes(rowSet, keep)
	probes := []ExecutionProbe{{
		Section: "native_predicate",
		Name:    "correlated_aggregate_count",
		Value:   strconv.Itoa(len(request.NativePredicates.CorrelatedAggregate)),
	}, {
		Section: "native_predicate",
		Name:    "rows_before",
		Value:   strconv.Itoa(rowSet.CandidateCount()),
	}, {
		Section: "native_predicate",
		Name:    "rows_after",
		Value:   strconv.Itoa(filtered.CandidateCount()),
	}}
	return filtered, probes, nil
}

func evaluateNativePredicatesForRow(predicates NativePredicateSet, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	for _, predicate := range predicates.CorrelatedAggregate {
		matched, diagnostics := evaluateCorrelatedAggregatePredicateForRow(predicate, rowSet, index)
		if diagnostics.BlocksNative() || !matched {
			return matched, diagnostics
		}
	}
	return true, nil
}

func evaluateCorrelatedAggregatePredicateForRow(predicate CorrelatedAggregatePredicate, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	keyVector, ok := nativePredicateProjectionVector(rowSet, predicate.KeyField)
	if !ok {
		return false, nativePredicateDiagnostics("correlated aggregate predicate missing key field " + predicate.KeyField.QualifiedName())
	}
	valueVector, ok := nativePredicateProjectionVector(rowSet, predicate.ValueField)
	if !ok {
		return false, nativePredicateDiagnostics("correlated aggregate predicate missing value field " + predicate.ValueField.QualifiedName())
	}
	if index >= len(keyVector.Values) || index >= len(valueVector.Values) {
		return false, nativePredicateDiagnostics("correlated aggregate predicate projection vector is shorter than rowset")
	}
	key, ok := nativePredicateIntCell(keyVector.Values[index])
	if !ok {
		return false, nativePredicateDiagnostics("correlated aggregate predicate key value is not numeric")
	}
	threshold, ok := predicate.thresholdForKey(key)
	if !ok {
		return false, nil
	}
	value, ok := directBitmapNumericCellValue(valueVector.Values[index])
	if !ok {
		return false, nativePredicateDiagnostics("correlated aggregate predicate value is not numeric")
	}
	return directBitmapResidualCompareFloat(predicate.Operator, value, threshold), nil
}

func nativePredicateProjectionVector(rowSet qsbridge.QuantaProjectedRowSet, field qsbridge.FieldRef) (qsbridge.QuantaProjectionVector, bool) {
	for _, vector := range rowSet.ProjectionVectors {
		if directBitmapProjectionVectorMatchesField(vector, field) {
			return vector, true
		}
	}
	return qsbridge.QuantaProjectionVector{}, false
}

func nativePredicateIntCell(cell qsbridge.ResultCell) (int64, bool) {
	value, ok := directBitmapNumericCellValue(cell)
	if !ok {
		return 0, false
	}
	return int64(value), true
}

func (p CorrelatedAggregatePredicate) thresholdForKey(key int64) (float64, bool) {
	for _, threshold := range p.Thresholds {
		if threshold.Key == key {
			return threshold.Threshold, true
		}
	}
	return 0, false
}

func nativePredicateDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("native predicate: %s", message))}
}
