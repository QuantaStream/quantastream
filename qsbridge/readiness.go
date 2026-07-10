package qsbridge

// ReadinessSummaryRow is one aggregate count in a scaffold readiness report.
type ReadinessSummaryRow struct {
	Scope  string
	Name   string
	Status CompatibilityStatus
	Count  int
}

// ReadinessDetailRow is one manifest item normalized for readiness reporting.
type ReadinessDetailRow struct {
	Scope        string
	Name         string
	Category     string
	Status       CompatibilityStatus
	RuntimeOwned bool
	AdapterOwned bool
	Description  string
}

// ReadinessReport summarizes qsbridge scaffold readiness from current manifests.
type ReadinessReport struct {
	Compatibility CompatibilityProfile
	SQLFeatures   SQLFeatureMatrix
	Rows          []ReadinessSummaryRow
	Details       []ReadinessDetailRow
	Deferred      int
	Blocking      int
	Diagnostics   DiagnosticSet
}

// ReadinessReport returns a derived scaffold readiness report.
func (s PlanningService) ReadinessReport() ReadinessReport {
	compatibility := s.CompatibilityProfile()
	features := s.SQLFeatureMatrix()
	report := ReadinessReport{
		Compatibility: compatibility.Clone(),
		SQLFeatures:   features.Clone(),
		Diagnostics:   mergeDiagnosticSets(compatibility.Diagnostics, features.Diagnostics),
	}
	report.Rows = readinessRows(compatibility, features)
	report.Details = readinessDetailRows(compatibility, features)
	for _, row := range report.Rows {
		if row.Status == CompatibilityStatusDeferred {
			report.Deferred += row.Count
		}
	}
	if report.Diagnostics.BlocksNative() {
		report.Blocking = len(report.Diagnostics)
	}
	return report
}

// Supported reports whether the readiness manifests themselves can be returned.
func (r ReadinessReport) Supported() bool {
	return !r.Diagnostics.BlocksNative()
}

// Clone returns a deep copy of report.
func (r ReadinessReport) Clone() ReadinessReport {
	return ReadinessReport{
		Compatibility: r.Compatibility.Clone(),
		SQLFeatures:   r.SQLFeatures.Clone(),
		Rows:          cloneReadinessRows(r.Rows),
		Details:       cloneReadinessDetailRows(r.Details),
		Deferred:      r.Deferred,
		Blocking:      r.Blocking,
		Diagnostics:   cloneDiagnosticSet(r.Diagnostics),
	}
}

func readinessRows(compatibility CompatibilityProfile, features SQLFeatureMatrix) []ReadinessSummaryRow {
	rows := make([]ReadinessSummaryRow, 0)
	rows = append(rows, readinessStatusRows("compatibility", compatibilityStatusCounts(compatibility.Capabilities))...)
	rows = append(rows, readinessStatusRows("sql_feature", sqlFeatureStatusCounts(features.Features))...)
	return rows
}

func readinessDetailRows(compatibility CompatibilityProfile, features SQLFeatureMatrix) []ReadinessDetailRow {
	rows := make([]ReadinessDetailRow, 0, len(compatibility.Capabilities)+len(features.Features))
	for _, capability := range compatibility.Capabilities {
		rows = append(rows, ReadinessDetailRow{
			Scope:        "compatibility",
			Name:         capability.Name,
			Category:     string(capability.Layer),
			Status:       capability.Status,
			RuntimeOwned: capability.RuntimeOwned,
			AdapterOwned: capability.AdapterOwned,
			Description:  capability.Description,
		})
	}
	for _, feature := range features.Features {
		rows = append(rows, ReadinessDetailRow{
			Scope:       "sql_feature",
			Name:        feature.Name,
			Category:    string(feature.Category),
			Status:      feature.Status,
			Description: feature.Description,
		})
	}
	return rows
}

func readinessStatusRows(scope string, counts map[CompatibilityStatus]int) []ReadinessSummaryRow {
	statuses := []CompatibilityStatus{
		CompatibilityStatusNativePlanning,
		CompatibilityStatusMetadataOnly,
		CompatibilityStatusBoundaryOnly,
		CompatibilityStatusAuditOnly,
		CompatibilityStatusDeferred,
	}
	rows := make([]ReadinessSummaryRow, 0, len(statuses))
	for _, status := range statuses {
		rows = append(rows, ReadinessSummaryRow{
			Scope:  scope,
			Name:   "status",
			Status: status,
			Count:  counts[status],
		})
	}
	return rows
}

func compatibilityStatusCounts(capabilities []CompatibilityCapability) map[CompatibilityStatus]int {
	counts := make(map[CompatibilityStatus]int)
	for _, capability := range capabilities {
		counts[capability.Status]++
	}
	return counts
}

func sqlFeatureStatusCounts(features []SQLFeature) map[CompatibilityStatus]int {
	counts := make(map[CompatibilityStatus]int)
	for _, feature := range features {
		counts[feature.Status]++
	}
	return counts
}

func cloneReadinessRows(rows []ReadinessSummaryRow) []ReadinessSummaryRow {
	if len(rows) == 0 {
		return nil
	}
	return append([]ReadinessSummaryRow(nil), rows...)
}

func cloneReadinessDetailRows(rows []ReadinessDetailRow) []ReadinessDetailRow {
	if len(rows) == 0 {
		return nil
	}
	return append([]ReadinessDetailRow(nil), rows...)
}
