package qsbridge

import "testing"

func TestPreparedPlanSQLFeatureCoverageReportsPresentFeatures(t *testing.T) {
	prepared := PreparedPlan{
		Query: QueryIR{
			Kind:       QueryKindSelect,
			Projection: []ProjectionColumn{{Alias: "order_id", Type: DataTypeInt}},
			Predicates: []Predicate{{
				Placement:    PredicatePushdown,
				Capabilities: []PlanCapability{CapabilityBitmapPushdown},
			}},
			OrderBy: []SortSpec{{Direction: SortAscending}},
		},
		Inspection: InspectionReport{
			Capabilities: []PlanCapability{CapabilityBitmapPushdown},
		},
		Supported: true,
	}

	report := prepared.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	projection := sqlFeatureCoverageByName(t, report.Coverage, "select_projection")
	if !projection.Present || !projection.Supported || projection.Detail != "columns=1" {
		t.Fatalf("projection coverage = %#v, want present supported projection detail", projection)
	}
	predicate := sqlFeatureCoverageByName(t, report.Coverage, "predicate_pushdown")
	if !predicate.Present || !predicate.Supported || !coverageHasCapability(predicate, CapabilityBitmapPushdown) {
		t.Fatalf("predicate coverage = %#v, want bitmap pushdown evidence", predicate)
	}
	orderBy := sqlFeatureCoverageByName(t, report.Coverage, "order_by")
	if !orderBy.Present || orderBy.Detail != "sorts=1" {
		t.Fatalf("order coverage = %#v, want order-by detail", orderBy)
	}
	scalar := sqlFeatureCoverageByName(t, report.Coverage, "scalar_subquery")
	if scalar.Present || !scalar.Supported {
		t.Fatalf("scalar coverage = %#v, absent feature should not block plan coverage", scalar)
	}
	management := sqlFeatureCoverageByName(t, report.Coverage, "explain_and_management_metadata")
	if management.Present || !management.Supported {
		t.Fatalf("management metadata coverage = %#v, absent metadata surface should not be query-present", management)
	}
}

func TestPreparedPlanSQLFeatureCoverageReportsDeferredDiagnostics(t *testing.T) {
	prepared := PreparedPlan{
		Query: QueryIR{Kind: QueryKindSelect},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticScalarSubquery, PhasePlan, "scalar subquery"),
		},
		Supported: false,
	}

	report := prepared.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	scalar := sqlFeatureCoverageByName(t, report.Coverage, "scalar_subquery")
	if !scalar.Present || scalar.Supported || !coverageHasDiagnostic(scalar, DiagnosticScalarSubquery) {
		t.Fatalf("scalar coverage = %#v, want unsupported scalar diagnostic coverage", scalar)
	}
}

func TestPreparedPlanSQLFeatureCoverageReportsCustomFunctions(t *testing.T) {
	prepared := PreparedPlan{
		Query: QueryIR{
			Kind: QueryKindSelect,
			Projection: []ProjectionColumn{{
				Alias: "shipmode_topn",
				Expr: FunctionCall(FunctionDefinition{
					Name:       "topn",
					Origin:     FunctionOriginQuantaCustom,
					Placement:  FunctionPlacementAggregate,
					ReturnType: DataTypeString,
				}),
			}},
			Predicates: []Predicate{{
				Expr:      FunctionCall(FunctionDefinition{Name: "sample_stratified", Origin: FunctionOriginLegacyCustom, Placement: FunctionPlacementPredicate, ReturnType: DataTypeBool}),
				Placement: PredicateResidualScan,
			}},
		},
		Supported: true,
	}

	report := prepared.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	custom := sqlFeatureCoverageByName(t, report.Coverage, "custom_functions")
	if !custom.Present || !custom.Supported || custom.Detail != "functions=2,predicate=1,names=topn,sample_stratified" {
		t.Fatalf("custom coverage = %#v, want custom and predicate function detail", custom)
	}

	aggregateOnly := prepared
	aggregateOnly.Query.Projection = nil
	aggregateOnly.Query.Predicates = nil
	aggregateOnly.Query.Aggregates = []Aggregate{{
		Function:  "topn",
		Origin:    FunctionOriginQuantaCustom,
		Placement: FunctionPlacementAggregate,
		Type:      DataTypeString,
	}}
	aggregateReport := aggregateOnly.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	aggregateCustom := sqlFeatureCoverageByName(t, aggregateReport.Coverage, "custom_functions")
	if !aggregateCustom.Present || aggregateCustom.Detail != "functions=1,predicate=0,names=topn" {
		t.Fatalf("aggregate custom coverage = %#v, want aggregate custom function detail", aggregateCustom)
	}

	mysqlOnly := prepared
	mysqlOnly.Query.Projection[0].Expr = FunctionCall(FunctionDefinition{Name: "lower", Origin: FunctionOriginMySQLCompatible, ReturnType: DataTypeString})
	mysqlOnly.Query.Predicates = nil
	mysqlOnly.Query.Having = nil
	mysqlReport := mysqlOnly.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	mysqlCustom := sqlFeatureCoverageByName(t, mysqlReport.Coverage, "custom_functions")
	if mysqlCustom.Present {
		t.Fatalf("mysql custom coverage = %#v, want absent custom feature", mysqlCustom)
	}
}

func TestPreparedPlanSQLFeatureCoverageCopiesMutableState(t *testing.T) {
	prepared := PreparedPlan{
		Query: QueryIR{
			Kind:       QueryKindSelect,
			Projection: []ProjectionColumn{{Alias: "order_id", Type: DataTypeInt}},
		},
	}

	report := prepared.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	report.Prepared.Query.Projection[0].Alias = "mutated"
	report.Matrix.Features[0].Name = "mutated"
	report.Coverage[0].Feature.Name = "mutated"

	again := prepared.SQLFeatureCoverage(DefaultSQLFeatureMatrix())
	if again.Prepared.Query.Projection[0].Alias == "mutated" {
		t.Fatalf("prepared plan leaked mutation: %#v", again.Prepared.Query.Projection)
	}
	if again.Matrix.Features[0].Name == "mutated" || again.Coverage[0].Feature.Name == "mutated" {
		t.Fatalf("matrix/coverage leaked mutation: %#v/%#v", again.Matrix.Features[0], again.Coverage[0])
	}
}

func sqlFeatureCoverageByName(t *testing.T, coverage []SQLFeatureCoverage, name string) SQLFeatureCoverage {
	t.Helper()
	for _, item := range coverage {
		if item.Feature.Name == name {
			return item
		}
	}
	t.Fatalf("coverage = %#v, missing %q", coverage, name)
	return SQLFeatureCoverage{}
}

func coverageHasCapability(coverage SQLFeatureCoverage, capability PlanCapability) bool {
	for _, current := range coverage.Capabilities {
		if current == capability {
			return true
		}
	}
	return false
}

func coverageHasDiagnostic(coverage SQLFeatureCoverage, code DiagnosticCode) bool {
	for _, current := range coverage.Diagnostics {
		if current == code {
			return true
		}
	}
	return false
}
