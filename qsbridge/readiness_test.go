package qsbridge

import "testing"

func TestPlanningServiceReadinessReportSummarizesManifestStatus(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	report := service.ReadinessReport()
	if !report.Supported() {
		t.Fatalf("report = %#v, want supported readiness report", report)
	}
	if len(report.Rows) != 10 {
		t.Fatalf("rows = %#v, want five status rows for compatibility and SQL features", report.Rows)
	}
	if readinessCount(report.Rows, "compatibility", CompatibilityStatusNativePlanning) == 0 {
		t.Fatalf("rows = %#v, want native-planning compatibility count", report.Rows)
	}
	if readinessCount(report.Rows, "compatibility", CompatibilityStatusMetadataOnly) < 4 {
		t.Fatalf("rows = %#v, want client metadata compatibility surfaces in readiness", report.Rows)
	}
	if readinessCount(report.Rows, "sql_feature", CompatibilityStatusDeferred) == 0 {
		t.Fatalf("rows = %#v, want deferred SQL feature count", report.Rows)
	}
	if report.Deferred != readinessCount(report.Rows, "compatibility", CompatibilityStatusDeferred)+readinessCount(report.Rows, "sql_feature", CompatibilityStatusDeferred) {
		t.Fatalf("deferred = %d rows=%#v, want aggregate deferred count", report.Deferred, report.Rows)
	}
	if len(report.Details) != len(report.Compatibility.Capabilities)+len(report.SQLFeatures.Features) {
		t.Fatalf("details = %#v, want one detail per compatibility capability and SQL feature", report.Details)
	}
	if !readinessDetailsContain(report.Details, "compatibility", "structured_explain", string(CompatibilityLayerClient), CompatibilityStatusMetadataOnly) {
		t.Fatalf("details = %#v, want structured explain detail", report.Details)
	}
	if !readinessDetailsContain(report.Details, "sql_feature", "explain_and_management_metadata", string(SQLFeatureProtocol), CompatibilityStatusMetadataOnly) {
		t.Fatalf("details = %#v, want explain/management feature detail", report.Details)
	}
}

func TestReadinessReportCloneCopiesMutableState(t *testing.T) {
	report := NewPlanningService(Planner{}, nil).ReadinessReport()
	cloned := report.Clone()
	cloned.Compatibility.Capabilities[0].Name = "mutated"
	cloned.SQLFeatures.Features[0].Name = "mutated"
	cloned.Rows[0].Count = 999
	cloned.Details[0].Name = "mutated"

	again := NewPlanningService(Planner{}, nil).ReadinessReport()
	if again.Compatibility.Capabilities[0].Name == "mutated" || again.SQLFeatures.Features[0].Name == "mutated" {
		t.Fatalf("manifests leaked mutation: %#v/%#v", again.Compatibility.Capabilities[0], again.SQLFeatures.Features[0])
	}
	if again.Rows[0].Count == 999 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Details[0].Name == "mutated" {
		t.Fatalf("details leaked mutation: %#v", again.Details[0])
	}
}

func readinessCount(rows []ReadinessSummaryRow, scope string, status CompatibilityStatus) int {
	for _, row := range rows {
		if row.Scope == scope && row.Status == status {
			return row.Count
		}
	}
	return 0
}
