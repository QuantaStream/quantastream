package qsbridge

import "strings"

// Diagnostics reports mutation-specific legality blockers that are independent
// of the concrete storage adapter. Execution layers may add stricter runtime
// checks, but broad writes and protected identity updates are planner-level
// policy decisions.
func (m MutationShape) Diagnostics() DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	switch m.Kind {
	case MutationUpdate:
		diagnostics = append(diagnostics, mutationPredicateDiagnostics(m.Kind, m.Predicates)...)
		for _, assignment := range m.Assignments {
			if mutationProtectedField(assignment.Field) {
				diagnostics = append(diagnostics, ErrorDiagnostic(
					DiagnosticMutationProtectedField,
					PhasePlan,
					"update cannot assign protected identity field: "+assignment.Field.QualifiedName(),
				))
			}
			for _, field := range FieldRefs(assignment.Value) {
				if !sameTableInstance(field.Table, m.Target) {
					diagnostics = append(diagnostics, ErrorDiagnostic(
						DiagnosticUnsupportedMutation,
						PhasePlan,
						"update assignment expression must be target-row-local: "+assignment.Field.QualifiedName(),
					))
					break
				}
			}
		}
	case MutationDelete:
		diagnostics = append(diagnostics, mutationPredicateDiagnostics(m.Kind, m.Predicates)...)
	}
	return diagnostics
}

func mutationPredicateDiagnostics(kind MutationKind, predicates []Predicate) DiagnosticSet {
	if kind != MutationUpdate && kind != MutationDelete {
		return nil
	}
	if len(predicates) > 0 {
		return nil
	}
	return DiagnosticSet{ErrorDiagnostic(
		DiagnosticMutationMissingPredicate,
		PhasePlan,
		string(kind)+" requires a predicate before native execution",
	)}
}

func mutationProtectedField(field FieldRef) bool {
	if field.PrimaryKey {
		return true
	}
	name := strings.ToLower(field.Name)
	physical := strings.ToLower(field.PhysicalName)
	return name == "rownum" || physical == "rownum"
}

func sameTableInstance(left TableInstance, right TableInstance) bool {
	if left.ID != "" || right.ID != "" {
		return left.ID == right.ID
	}
	return strings.EqualFold(left.Schema, right.Schema) && strings.EqualFold(left.Table, right.Table) && strings.EqualFold(left.RefName(), right.RefName())
}
