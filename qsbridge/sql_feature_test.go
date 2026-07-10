package qsbridge

import "testing"

func TestDefaultSQLFeatureMatrixDescribesCompatibilityMilestones(t *testing.T) {
	matrix := DefaultSQLFeatureMatrix()
	for _, name := range []string{
		"select_projection",
		"predicate_pushdown",
		"semi_anti_membership",
		"outer_join",
		"scalar_subquery",
		"custom_functions",
		"prepared_and_batch",
		"explain_and_management_metadata",
	} {
		if !sqlFeatureMatrixHas(matrix, name) {
			t.Fatalf("matrix = %#v, missing %q", matrix.Features, name)
		}
	}
	if feature, ok := sqlFeatureMatrixFeature(matrix, "outer_join"); !ok || feature.Status != CompatibilityStatusDeferred || !sqlFeatureHasDiagnostic(feature, DiagnosticOuterJoin) {
		t.Fatalf("outer join feature = %#v/%v, want deferred diagnostic feature", feature, ok)
	}
	if feature, ok := sqlFeatureMatrixFeature(matrix, "semi_anti_membership"); !ok || feature.Status != CompatibilityStatusNativePlanning || !sqlFeatureHasCapability(feature, CapabilityBitmapDifference) {
		t.Fatalf("semi/anti feature = %#v/%v, want native bitmap difference evidence", feature, ok)
	}
	if feature, ok := sqlFeatureMatrixFeature(matrix, "custom_functions"); !ok || feature.Category != SQLFeatureFunction || feature.Status != CompatibilityStatusMetadataOnly {
		t.Fatalf("custom functions feature = %#v/%v, want metadata-only function feature", feature, ok)
	}
	if feature, ok := sqlFeatureMatrixFeature(matrix, "explain_and_management_metadata"); !ok || feature.Category != SQLFeatureProtocol || feature.Status != CompatibilityStatusMetadataOnly {
		t.Fatalf("explain/management feature = %#v/%v, want metadata-only protocol feature", feature, ok)
	}
}

func TestSQLFeatureMatrixCloneCopiesMutableState(t *testing.T) {
	matrix := DefaultSQLFeatureMatrix()
	matrix.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInternalInvariant, PhasePlan, "original"),
	}

	cloned := matrix.Clone()
	cloned.Features[0].Name = "mutated"
	cloned.Features[0].Capabilities = append(cloned.Features[0].Capabilities, CapabilityScalarSubquery)
	cloned.Features[0].Diagnostics = append(cloned.Features[0].Diagnostics, DiagnosticScalarSubquery)
	cloned.Diagnostics[0].Message = "mutated"

	if matrix.Features[0].Name == "mutated" || sqlFeatureHasCapability(matrix.Features[0], CapabilityScalarSubquery) || sqlFeatureHasDiagnostic(matrix.Features[0], DiagnosticScalarSubquery) {
		t.Fatalf("features leaked mutation: %#v", matrix.Features[0])
	}
	if matrix.Diagnostics[0].Message != "original" {
		t.Fatalf("diagnostics leaked mutation: %#v", matrix.Diagnostics)
	}
}

func sqlFeatureMatrixHas(matrix SQLFeatureMatrix, name string) bool {
	_, ok := sqlFeatureMatrixFeature(matrix, name)
	return ok
}

func sqlFeatureMatrixFeature(matrix SQLFeatureMatrix, name string) (SQLFeature, bool) {
	for _, feature := range matrix.Features {
		if feature.Name == name {
			return feature, true
		}
	}
	return SQLFeature{}, false
}

func sqlFeatureHasCapability(feature SQLFeature, capability PlanCapability) bool {
	for _, current := range feature.Capabilities {
		if current == capability {
			return true
		}
	}
	return false
}

func sqlFeatureHasDiagnostic(feature SQLFeature, code DiagnosticCode) bool {
	for _, current := range feature.Diagnostics {
		if current == code {
			return true
		}
	}
	return false
}
