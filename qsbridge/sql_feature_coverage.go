package qsbridge

import "strconv"

// SQLFeatureCoverage describes whether one SQL feature appears in a prepared plan.
type SQLFeatureCoverage struct {
	Feature      SQLFeature
	Present      bool
	Supported    bool
	Capabilities []PlanCapability
	Diagnostics  []DiagnosticCode
	Detail       string
}

// SQLFeatureCoverageReport summarizes a prepared plan against the SQL feature matrix.
type SQLFeatureCoverageReport struct {
	Prepared    PreparedPlan
	Matrix      SQLFeatureMatrix
	Coverage    []SQLFeatureCoverage
	Diagnostics DiagnosticSet
}

// SQLFeatureCoverage returns coverage rows for this prepared plan.
func (p PreparedPlan) SQLFeatureCoverage(matrix SQLFeatureMatrix) SQLFeatureCoverageReport {
	matrix = matrix.Clone()
	report := SQLFeatureCoverageReport{
		Prepared:    clonePreparedPlan(p),
		Matrix:      matrix,
		Diagnostics: cloneDiagnosticSet(p.Diagnostics),
	}
	if len(report.Matrix.Features) == 0 {
		report.Matrix = DefaultSQLFeatureMatrix()
	}
	report.Coverage = preparedSQLFeatureCoverage(p, report.Matrix.Features)
	return report
}

func preparedSQLFeatureCoverage(prepared PreparedPlan, features []SQLFeature) []SQLFeatureCoverage {
	coverage := make([]SQLFeatureCoverage, 0, len(features))
	capabilities := preparedFeatureCapabilities(prepared)
	diagnostics := prepared.Diagnostics.Codes()
	for _, feature := range features {
		evidenceCapabilities := matchingPlanCapabilities(capabilities, feature.Capabilities)
		evidenceDiagnostics := matchingDiagnosticCodes(diagnostics, feature.Diagnostics)
		present := preparedSQLFeaturePresent(prepared, feature, evidenceCapabilities, evidenceDiagnostics)
		supported := !present || preparedSQLFeatureSupported(feature, evidenceDiagnostics)
		coverage = append(coverage, SQLFeatureCoverage{
			Feature:      cloneSQLFeature(feature),
			Present:      present,
			Supported:    supported,
			Capabilities: evidenceCapabilities,
			Diagnostics:  evidenceDiagnostics,
			Detail:       preparedSQLFeatureDetail(prepared, feature, present),
		})
	}
	return coverage
}

func preparedFeatureCapabilities(prepared PreparedPlan) []PlanCapability {
	if len(prepared.Inspection.Capabilities) > 0 {
		return append([]PlanCapability(nil), prepared.Inspection.Capabilities...)
	}
	return append([]PlanCapability(nil), prepared.Logical.Classification.Capabilities...)
}

func preparedSQLFeaturePresent(prepared PreparedPlan, feature SQLFeature, capabilities []PlanCapability, diagnostics []DiagnosticCode) bool {
	if len(capabilities) > 0 || len(diagnostics) > 0 {
		return true
	}
	query := prepared.Query
	switch feature.Name {
	case "select_projection":
		return query.Kind == QueryKindSelect && len(query.Projection) > 0
	case "order_by":
		return len(query.OrderBy) > 0
	case "predicate_pushdown":
		return anyPredicatePlacement(query, PredicatePushdown)
	case "string_predicates":
		return false
	case "residual_scan":
		return anyPredicatePlacement(query, PredicateResidualScan)
	case "mixed_table_residual":
		return anyPredicatePlacement(query, PredicateResidualJoin)
	case "inner_join":
		return anyJoinKind(query, JoinKindInner) || anyJoinKind(query, JoinKindUnknown)
	case "semi_anti_membership":
		return len(query.Memberships) > 0
	case "outer_join":
		return anyJoinKind(query, JoinKindLeftOuter) || anyJoinKind(query, JoinKindRightOuter)
	case "grouped_aggregate":
		return len(query.GroupBy) > 0 || len(query.Aggregates) > 0
	case "grouped_join":
		return len(query.Joins) > 0 && (len(query.GroupBy) > 0 || len(query.Aggregates) > 0)
	case "scalar_subquery":
		return false
	case "mutations":
		return query.Kind == QueryKindInsert || query.Kind == QueryKindUpdate || query.Kind == QueryKindDelete || query.Kind == QueryKindDDL || query.Kind == QueryKindSession
	case "custom_functions":
		return len(queryFunctionNamesByOrigin(query, FunctionOriginQuantaCustom, FunctionOriginLegacyCustom)) > 0
	case "prepared_and_batch":
		return len(prepared.Parameters) > 0
	case "explain_and_management_metadata":
		return false
	case "cancellation_and_cursors":
		return false
	default:
		return false
	}
}

func preparedSQLFeatureSupported(feature SQLFeature, diagnostics []DiagnosticCode) bool {
	if len(diagnostics) > 0 {
		return false
	}
	return feature.Status != CompatibilityStatusDeferred
}

func preparedSQLFeatureDetail(prepared PreparedPlan, feature SQLFeature, present bool) string {
	if !present {
		return ""
	}
	query := prepared.Query
	switch feature.Name {
	case "select_projection":
		return countDetail("columns", len(query.Projection))
	case "order_by":
		return countDetail("sorts", len(query.OrderBy))
	case "predicate_pushdown", "residual_scan", "mixed_table_residual":
		return countDetail("predicates", len(query.Predicates)+len(query.Having))
	case "inner_join", "outer_join":
		return countDetail("joins", len(query.Joins))
	case "semi_anti_membership":
		return countDetail("memberships", len(query.Memberships))
	case "grouped_aggregate":
		return joinStringValues([]string{
			countDetail("group_by", len(query.GroupBy)),
			countDetail("aggregates", len(query.Aggregates)),
		})
	case "grouped_join":
		return joinStringValues([]string{
			countDetail("joins", len(query.Joins)),
			countDetail("group_by", len(query.GroupBy)),
			countDetail("aggregates", len(query.Aggregates)),
		})
	case "mutations":
		return string(query.Kind)
	case "custom_functions":
		names := queryFunctionNamesByOrigin(query, FunctionOriginQuantaCustom, FunctionOriginLegacyCustom)
		predicateNames := queryFunctionNamesByPlacement(query, FunctionPlacementPredicate)
		return joinStringValues([]string{
			countDetail("functions", len(names)),
			countDetail("predicate", len(predicateNames)),
			"names=" + joinStringValues(names),
		})
	case "prepared_and_batch":
		return countDetail("parameters", len(prepared.Parameters))
	default:
		return ""
	}
}

func queryFunctionNamesByOrigin(query QueryIR, origins ...FunctionOrigin) []string {
	allowed := make(map[FunctionOrigin]struct{}, len(origins))
	for _, origin := range origins {
		allowed[origin] = struct{}{}
	}
	names := make([]string, 0)
	seen := make(map[string]struct{})
	for _, usage := range query.FunctionUsages() {
		if _, ok := allowed[usage.Origin]; !ok {
			continue
		}
		appendFunctionUsageName(usage.Name, seen, &names)
	}
	return names
}

func queryFunctionNamesByPlacement(query QueryIR, placements ...FunctionPlacement) []string {
	allowed := make(map[FunctionPlacement]struct{}, len(placements))
	for _, placement := range placements {
		allowed[placement] = struct{}{}
	}
	names := make([]string, 0)
	seen := make(map[string]struct{})
	for _, usage := range query.FunctionUsages() {
		if _, ok := allowed[usage.Placement]; !ok {
			continue
		}
		appendFunctionUsageName(usage.Name, seen, &names)
	}
	return names
}

func appendFunctionUsageName(name string, seen map[string]struct{}, names *[]string) {
	if _, ok := seen[name]; ok {
		return
	}
	seen[name] = struct{}{}
	*names = append(*names, name)
}

func anyPredicatePlacement(query QueryIR, placement PredicatePlacement) bool {
	for _, predicate := range query.Predicates {
		if predicate.Placement == placement {
			return true
		}
	}
	for _, predicate := range query.Having {
		if predicate.Placement == placement {
			return true
		}
	}
	return false
}

func anyJoinKind(query QueryIR, kind JoinKind) bool {
	for _, edge := range query.Joins {
		if joinKindOrInner(edge.Kind) == kind {
			return true
		}
	}
	return false
}

func matchingPlanCapabilities(haystack, needles []PlanCapability) []PlanCapability {
	if len(haystack) == 0 || len(needles) == 0 {
		return nil
	}
	matches := make([]PlanCapability, 0)
	for _, needle := range needles {
		for _, current := range haystack {
			if current == needle {
				matches = append(matches, needle)
				break
			}
		}
	}
	return matches
}

func matchingDiagnosticCodes(haystack, needles []DiagnosticCode) []DiagnosticCode {
	if len(haystack) == 0 || len(needles) == 0 {
		return nil
	}
	matches := make([]DiagnosticCode, 0)
	for _, needle := range needles {
		for _, current := range haystack {
			if current == needle {
				matches = append(matches, needle)
				break
			}
		}
	}
	return matches
}

func countDetail(name string, count int) string {
	return name + "=" + strconv.Itoa(count)
}

func cloneSQLFeature(feature SQLFeature) SQLFeature {
	feature.Capabilities = append([]PlanCapability(nil), feature.Capabilities...)
	feature.Diagnostics = append([]DiagnosticCode(nil), feature.Diagnostics...)
	return feature
}
