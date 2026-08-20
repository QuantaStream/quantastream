package qsbridge

import "testing"

func TestMutationShapeDiagnosticsRejectsMissingUpdatePredicate(t *testing.T) {
	orders := TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders", Alias: "o"}
	mutation := MutationShape{
		Kind:   MutationUpdate,
		Target: orders,
		Assignments: []MutationAssignment{{
			Field: FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat},
			Value: Literal(ValueFloat, 10.0),
		}},
	}

	diagnostics := mutation.Diagnostics()
	if !diagnostics.BlocksNative() || !containsDiagnosticCode(diagnostics.Codes(), DiagnosticMutationMissingPredicate) {
		t.Fatalf("diagnostics = %#v, want missing predicate blocker", diagnostics)
	}
}

func TestMutationShapeDiagnosticsRejectsMissingDeletePredicate(t *testing.T) {
	mutation := MutationShape{
		Kind:   MutationDelete,
		Target: TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders"},
	}

	diagnostics := mutation.Diagnostics()
	if !diagnostics.BlocksNative() || !containsDiagnosticCode(diagnostics.Codes(), DiagnosticMutationMissingPredicate) {
		t.Fatalf("diagnostics = %#v, want missing predicate blocker", diagnostics)
	}
}

func TestMutationShapeDiagnosticsRejectsProtectedUpdateField(t *testing.T) {
	orders := TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt, PrimaryKey: true}
	mutation := MutationShape{
		Kind:   MutationUpdate,
		Target: orders,
		Assignments: []MutationAssignment{{
			Field: orderKey,
			Value: Literal(ValueInt, int64(1)),
		}},
		Predicates: []Predicate{{Expr: BinaryExpr{Op: BinaryOpEqual, Left: Field(orderKey), Right: Literal(ValueInt, int64(1))}}},
	}

	diagnostics := mutation.Diagnostics()
	if !diagnostics.BlocksNative() || !containsDiagnosticCode(diagnostics.Codes(), DiagnosticMutationProtectedField) {
		t.Fatalf("diagnostics = %#v, want protected field blocker", diagnostics)
	}
}

func TestQueryIRDiagnosticsIncludesMutationPolicy(t *testing.T) {
	orders := TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders"}
	query := QueryIR{
		Kind: QueryKindDelete,
		Mutation: MutationShape{
			Kind:   MutationDelete,
			Target: orders,
		},
	}

	if query.Supported() {
		t.Fatalf("unqualified delete should not be supported")
	}
	if !containsDiagnosticCode(query.Diagnostics().Codes(), DiagnosticMutationMissingPredicate) {
		t.Fatalf("diagnostics = %#v, want mutation missing predicate", query.Diagnostics())
	}
}

func TestMutationValidationStepFailureDiagnosticCarriesFields(t *testing.T) {
	orders := TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt, PrimaryKey: true}
	step := MutationValidationStep{
		Kind:           MutationValidationPrimaryKeyDuplicateScan,
		Target:         orders,
		Columns:        []FieldRef{orderKey},
		FailureCode:    DiagnosticMutationPrimaryKeyDuplicate,
		FailureMessage: "primary key duplicates exist",
	}

	diagnostic := step.FailureDiagnostic(PhaseExecute)
	if diagnostic.Code != DiagnosticMutationPrimaryKeyDuplicate || diagnostic.Phase != PhaseExecute || diagnostic.Message != "primary key duplicates exist" {
		t.Fatalf("diagnostic = %#v, want duplicate failure diagnostic", diagnostic)
	}
	if len(diagnostic.Fields) != 1 || diagnostic.Fields[0].QualifiedName() != "o.o_orderkey" {
		t.Fatalf("diagnostic fields = %#v, want o.o_orderkey", diagnostic.Fields)
	}
}

func TestFieldDefinitionRefCarriesPrimaryKey(t *testing.T) {
	field := FieldDefinition{Name: "o_orderkey", Type: DataTypeInt, PrimaryKey: true}
	ref := field.Ref(TableInstance{ID: "orders_1", Table: "orders", Alias: "o"}, FieldRoleMutationTarget)
	if !ref.PrimaryKey {
		t.Fatalf("PrimaryKey = false, want true")
	}
}
