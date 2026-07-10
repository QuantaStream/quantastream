package qsbridge

// PlanInvariantStatus describes one prepared-plan invariant check result.
type PlanInvariantStatus string

const (
	// PlanInvariantOK means the invariant is satisfied.
	PlanInvariantOK PlanInvariantStatus = "ok"
	// PlanInvariantWarning means the invariant is advisory but not blocking.
	PlanInvariantWarning PlanInvariantStatus = "warning"
	// PlanInvariantError means the invariant indicates an internally inconsistent snapshot.
	PlanInvariantError PlanInvariantStatus = "error"
)

// PlanInvariantCheck is one prepared-plan consistency check.
type PlanInvariantCheck struct {
	Name       string
	Status     PlanInvariantStatus
	Diagnostic DiagnosticCode
	Detail     string
}

// PlanInvariantReport summarizes prepared-plan consistency without executing it.
type PlanInvariantReport struct {
	Prepared    PreparedPlan
	Checks      []PlanInvariantCheck
	Diagnostics DiagnosticSet
}

// PlanInvariants returns consistency checks for this prepared plan.
func (p PreparedPlan) PlanInvariants() PlanInvariantReport {
	report := PlanInvariantReport{
		Prepared: clonePreparedPlan(p),
		Checks:   preparedPlanInvariantChecks(p),
	}
	for _, check := range report.Checks {
		if check.Status == PlanInvariantError {
			report.Diagnostics = append(report.Diagnostics, ErrorDiagnostic(
				DiagnosticInternalInvariant,
				PhasePlan,
				check.Name+": "+check.Detail,
			))
		}
	}
	return report
}

// Supported reports whether all prepared-plan invariants pass without errors.
func (r PlanInvariantReport) Supported() bool {
	return !r.Diagnostics.BlocksNative()
}

// Clone returns a deep copy of report.
func (r PlanInvariantReport) Clone() PlanInvariantReport {
	return PlanInvariantReport{
		Prepared:    clonePreparedPlan(r.Prepared),
		Checks:      clonePlanInvariantChecks(r.Checks),
		Diagnostics: cloneDiagnosticSet(r.Diagnostics),
	}
}

func preparedPlanInvariantChecks(prepared PreparedPlan) []PlanInvariantCheck {
	return []PlanInvariantCheck{
		preparedSupportedInvariant(prepared),
		preparedKindInvariant(prepared),
		preparedParametersInvariant(prepared),
		preparedAccessInvariant(prepared),
		preparedResultColumnsInvariant(prepared),
		preparedScalarSubqueryPlaceholderInvariant(prepared),
		preparedCorrelatedAggregatePlaceholderInvariant(prepared),
		preparedCacheKeyInvariant(prepared),
	}
}

func preparedSupportedInvariant(prepared PreparedPlan) PlanInvariantCheck {
	wantSupported := !prepared.Diagnostics.BlocksNative()
	if prepared.Supported == wantSupported {
		return okInvariant("supported_matches_diagnostics", boolDetail("supported", prepared.Supported))
	}
	return errorInvariant("supported_matches_diagnostics", "supported flag disagrees with blocking diagnostics")
}

func preparedKindInvariant(prepared PreparedPlan) PlanInvariantCheck {
	if prepared.Query.Kind == "" || prepared.Kind == prepared.Query.Kind {
		return okInvariant("kind_matches_query", string(prepared.Kind))
	}
	return errorInvariant("kind_matches_query", "prepared kind "+string(prepared.Kind)+" differs from query kind "+string(prepared.Query.Kind))
}

func preparedParametersInvariant(prepared PreparedPlan) PlanInvariantCheck {
	required := prepared.Query.RequiredParameters()
	if len(prepared.Parameters) == len(required) {
		return okInvariant("parameters_match_query", countDetail("parameters", len(prepared.Parameters)))
	}
	return errorInvariant("parameters_match_query", "prepared parameters do not match query-required parameters")
}

func preparedAccessInvariant(prepared PreparedPlan) PlanInvariantCheck {
	required := prepared.Query.RequiredAccess()
	if len(prepared.Access) == len(required) {
		return okInvariant("access_matches_query", countDetail("requirements", len(prepared.Access)))
	}
	return errorInvariant("access_matches_query", "prepared access requirements do not match query-required access")
}

func preparedResultColumnsInvariant(prepared PreparedPlan) PlanInvariantCheck {
	required := prepared.Query.ResultColumns()
	if len(prepared.ResultColumns) == len(required) {
		return okInvariant("result_columns_match_query", countDetail("columns", len(prepared.ResultColumns)))
	}
	return errorInvariant("result_columns_match_query", "prepared result columns do not match query result columns")
}

func preparedScalarSubqueryPlaceholderInvariant(prepared PreparedPlan) PlanInvariantCheck {
	total, missing := scalarSubqueryPlaceholderCounts(prepared.Logical.Root)
	if missing > 0 {
		return errorInvariant("scalar_subquery_placeholders", countDetail("missing_outputs", missing))
	}
	return okInvariant("scalar_subquery_placeholders", countDetail("placeholders", total))
}

func scalarSubqueryPlaceholderCounts(root LogicalNode) (int, int) {
	total := 0
	missing := 0
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		scalar, ok := node.(ScalarSubqueryNode)
		if !ok {
			return true
		}
		if len(scalar.Intents) == 0 {
			missing++
			return true
		}
		for _, intent := range scalar.Intents {
			total++
			if intent.Scalar == nil || intent.Scalar.OutputName == "" {
				missing++
			}
		}
		return true
	})
	return total, missing
}

func preparedCorrelatedAggregatePlaceholderInvariant(prepared PreparedPlan) PlanInvariantCheck {
	total, invalid := correlatedAggregatePlaceholderCounts(prepared.Logical.Root)
	if invalid > 0 {
		return errorInvariant("correlated_aggregate_placeholders", countDetail("invalid_placeholders", invalid))
	}
	return okInvariant("correlated_aggregate_placeholders", countDetail("placeholders", total))
}

func correlatedAggregatePlaceholderCounts(root LogicalNode) (int, int) {
	total := 0
	invalid := 0
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		correlated, ok := node.(CorrelatedAggregateSubqueryNode)
		if !ok {
			return true
		}
		if len(correlated.Intents) == 0 {
			invalid++
			return true
		}
		for _, intent := range correlated.Intents {
			total++
			if !intent.Valid() {
				invalid++
			}
		}
		return true
	})
	return total, invalid
}

func preparedCacheKeyInvariant(prepared PreparedPlan) PlanInvariantCheck {
	key := prepared.CacheKey()
	if key.Digest != "" {
		return okInvariant("cache_key_available", "digest="+key.Digest)
	}
	return errorInvariant("cache_key_available", "prepared plan did not produce a cache key digest")
}

func okInvariant(name, detail string) PlanInvariantCheck {
	return PlanInvariantCheck{Name: name, Status: PlanInvariantOK, Detail: detail}
}

func errorInvariant(name, detail string) PlanInvariantCheck {
	return PlanInvariantCheck{
		Name:       name,
		Status:     PlanInvariantError,
		Diagnostic: DiagnosticInternalInvariant,
		Detail:     detail,
	}
}

func boolDetail(name string, value bool) string {
	if value {
		return name + "=true"
	}
	return name + "=false"
}

func clonePlanInvariantChecks(checks []PlanInvariantCheck) []PlanInvariantCheck {
	if len(checks) == 0 {
		return nil
	}
	return append([]PlanInvariantCheck(nil), checks...)
}
