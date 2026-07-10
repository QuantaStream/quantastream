package qsruntime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RelationshipTupleRow carries role-qualified rownums for one logical relationship graph row.
type RelationshipTupleRow = qsbridge.RelationshipTupleRow

// RelationshipTupleRowSet is a role-qualified row stream for relationship-vector graph execution.
type RelationshipTupleRowSet = qsbridge.RelationshipTupleRowSet

// RelationshipTupleExpansion describes one parent-to-child relationship-vector expansion.
type RelationshipTupleExpansion = qsbridge.RelationshipTupleExpansion

// NewRelationshipTupleRow aliases qsbridge's tuple-row constructor during the package split.
var NewRelationshipTupleRow = qsbridge.NewRelationshipTupleRow

// NewRelationshipTupleRowSet aliases qsbridge's tuple-rowset constructor during the package split.
var NewRelationshipTupleRowSet = qsbridge.NewRelationshipTupleRowSet

// NewRelationshipTupleRowSetFromRootExpansions aliases qsbridge's root-expansion constructor during the package split.
var NewRelationshipTupleRowSetFromRootExpansions = qsbridge.NewRelationshipTupleRowSetFromRootExpansions

// NewRelationshipTupleRowSetFromAlignedRownums aliases qsbridge's aligned-rownum constructor during the package split.
var NewRelationshipTupleRowSetFromAlignedRownums = qsbridge.NewRelationshipTupleRowSetFromAlignedRownums

// RelationshipTupleProbeSnapshot captures the tuple-rowset observations exposed during inspection.
type RelationshipTupleProbeSnapshot struct {
	Section             string
	Expanded            RelationshipTupleRowSet
	Filtered            RelationshipTupleRowSet
	MaterializedFields  []qsbridge.QuantaProjectionField
	AggregateExpression string
	AggregateAlias      string
}

// RelationshipTupleProbes returns stable diagnostic probes for tuple-rowset execution planning.
func RelationshipTupleProbes(snapshot RelationshipTupleProbeSnapshot) []ExecutionProbe {
	section := snapshot.Section
	if section == "" {
		section = "relationship_tuple"
	}
	roles := snapshot.Expanded.Roles()
	if len(roles) == 0 {
		roles = snapshot.Filtered.Roles()
	}
	probes := []ExecutionProbe{
		{Section: section, Name: "roles", Value: relationshipTupleRolesDebug(roles)},
		{Section: section, Name: "expanded_rows", Value: fmt.Sprintf("%d", snapshot.Expanded.CandidateCount())},
		{Section: section, Name: "filtered_rows", Value: fmt.Sprintf("%d", snapshot.Filtered.CandidateCount())},
	}
	if len(snapshot.MaterializedFields) > 0 {
		probes = append(probes, ExecutionProbe{Section: section, Name: "materialized_fields", Value: relationshipTupleProjectionFieldsDebug(snapshot.MaterializedFields)})
	}
	if snapshot.AggregateExpression != "" {
		probes = append(probes, ExecutionProbe{Section: section, Name: "aggregate_expression", Value: snapshot.AggregateExpression})
	}
	if snapshot.AggregateAlias != "" {
		probes = append(probes, ExecutionProbe{Section: section, Name: "aggregate_alias", Value: snapshot.AggregateAlias})
	}
	return probes
}

// RelationshipTupleValueKey identifies a materialized tuple value by role and field.
type RelationshipTupleValueKey = qsbridge.RelationshipTupleValueKey

// RelationshipTupleValueStore stores materialized field values by role, field, and rownum.
type RelationshipTupleValueStore = qsbridge.RelationshipTupleValueStore

// RelationshipTupleValueKeyForField aliases qsbridge's tuple value-key builder during the package split.
var RelationshipTupleValueKeyForField = qsbridge.RelationshipTupleValueKeyForField

// AggregateRelationshipTupleProjected evaluates SQL aggregates over projected values aligned with tuple rows.
func AggregateRelationshipTupleProjected(s RelationshipTupleRowSet, request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet, result ExecutionResult) ExecutionResult {
	if rowSet.CandidateCount() != s.CandidateCount() {
		result.Diagnostics = append(result.Diagnostics, relationshipTupleDiagnostics(fmt.Sprintf("projected row count %d does not match tuple row count %d", rowSet.CandidateCount(), s.CandidateCount()))...)
		return result
	}
	result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded:           s,
		Filtered:           s,
		MaterializedFields: relationshipTupleProjectedFields(rowSet),
		AggregateAlias:     relationshipTupleAggregateAlias(request),
	})...)
	return directBitmapMaterializedAggregateResult(request, rowSet, result)
}

// FilterRelationshipTupleProjectedResiduals filters projected and tuple rows with the same residual keep indexes.
func FilterRelationshipTupleProjectedResiduals(s RelationshipTupleRowSet, request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, RelationshipTupleRowSet, qsbridge.DiagnosticSet) {
	if rowSet.CandidateCount() != s.CandidateCount() {
		return qsbridge.QuantaProjectedRowSet{}, RelationshipTupleRowSet{}, relationshipTupleDiagnostics(fmt.Sprintf("projected row count %d does not match tuple row count %d", rowSet.CandidateCount(), s.CandidateCount()))
	}
	residuals := directBitmapResidualScanPredicates(request)
	if len(residuals) == 0 {
		return rowSet, s, nil
	}
	keep := make([]int, 0, rowSet.CandidateCount())
	for i := 0; i < rowSet.CandidateCount(); i++ {
		matched, diagnostics := directBitmapEvaluateResidualPredicates(residuals, rowSet, i)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, RelationshipTupleRowSet{}, diagnostics
		}
		if matched {
			keep = append(keep, i)
		}
	}
	return directBitmapFilterRowSetByIndexes(rowSet, keep), s.FilterByIndexes(keep), nil
}

// FilterRelationshipTupleResidualPredicates applies residual predicates to tuple rows through projected values.
func FilterRelationshipTupleResidualPredicates(s RelationshipTupleRowSet, index string, fields []qsbridge.QuantaProjectionField, values RelationshipTupleValueStore, predicates []qsbridge.Predicate) (RelationshipTupleRowSet, qsbridge.DiagnosticSet) {
	if len(predicates) == 0 || len(s.Rows) == 0 {
		return s, nil
	}
	rowSet, diagnostics := s.ToProjectedRowSet(index, fields, values)
	if diagnostics.BlocksNative() {
		return RelationshipTupleRowSet{}, diagnostics
	}
	kept := make([]RelationshipTupleRow, 0, len(s.Rows))
	for i, row := range s.Rows {
		matched, diagnostics := directBitmapEvaluateResidualPredicates(predicates, rowSet, i)
		if diagnostics.BlocksNative() {
			return RelationshipTupleRowSet{}, diagnostics
		}
		if matched {
			kept = append(kept, row)
		}
	}
	return RelationshipTupleRowSet{Rows: kept}, nil
}

func relationshipTupleDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, message)}
}

func relationshipTupleValueKeyDebug(key RelationshipTupleValueKey) string {
	parts := []string{string(key.Role)}
	if key.Field != "" {
		parts = append(parts, key.Field)
	}
	return strings.Join(parts, ".")
}

func relationshipTupleProjectedFields(rowSet qsbridge.QuantaProjectedRowSet) []qsbridge.QuantaProjectionField {
	fields := make([]qsbridge.QuantaProjectionField, 0, len(rowSet.ProjectionVectors))
	for _, vector := range rowSet.ProjectionVectors {
		fields = append(fields, vector.Field)
	}
	return fields
}

func relationshipTupleProjectionFieldsDebug(fields []qsbridge.QuantaProjectionField) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		key := RelationshipTupleValueKeyForField(field)
		parts = append(parts, relationshipTupleValueKeyDebug(key))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func relationshipTupleRolesDebug(roles []qsbridge.TableInstanceID) string {
	parts := make([]string, 0, len(roles))
	for _, role := range roles {
		parts = append(parts, string(role))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
