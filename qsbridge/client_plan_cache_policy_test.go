package qsbridge

import "testing"

func TestPlanningServiceListClientPlanCachePolicyBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientPlanCachePolicy(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported plan-cache policy metadata", exchange)
	}
	if len(exchange.Rows) != len(DefaultPlanCacheKeyPolicy()) {
		t.Fatalf("rows = %#v, want one row per default policy", exchange.Rows)
	}
	sqlRow, ok := clientPlanCachePolicyRowByFactor(exchange.Rows, PlanCacheFactorSQL)
	if !ok || !sqlRow.Included || sqlRow.Participation != PlanCacheParticipationIncluded {
		t.Fatalf("sql row = %#v/%v, want included policy", sqlRow, ok)
	}
	explainRow, ok := clientPlanCachePolicyRowByFactor(exchange.Rows, PlanCacheFactorExplainOptions)
	if !ok || explainRow.Included || explainRow.Participation != PlanCacheParticipationDisplayOnly {
		t.Fatalf("explain row = %#v/%v, want display-only exclusion", explainRow, ok)
	}
	parameterRow, ok := clientPlanCachePolicyRowByFactor(exchange.Rows, PlanCacheFactorParameterValues)
	if !ok || parameterRow.Included || parameterRow.Participation != PlanCacheParticipationExecuteOnly {
		t.Fatalf("parameter row = %#v/%v, want execute-only exclusion", parameterRow, ok)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Factor" {
		t.Fatalf("schema = %#v, want plan-cache policy columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per policy", exchange.Result)
	}
}

func TestPlanningServiceListClientPlanCachePolicyReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "blocked")}

	exchange := service.ListClientPlanCachePolicy(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 4 {
		t.Fatalf("result/schema = %#v/%#v, want failed policy envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientPlanCachePolicyCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientPlanCachePolicy(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Policies[0].Reason = "mutated"
	exchange.Rows[0].Reason = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientPlanCachePolicy(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Policies[0].Reason == "mutated" || again.Rows[0].Reason == "mutated" {
		t.Fatalf("policy metadata leaked mutation: %#v/%#v", again.Policies[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Factor" || again.ResultSchema.Columns[0].Name != "Factor" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][3].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func clientPlanCachePolicyRowByFactor(rows []ClientPlanCachePolicyRow, factor PlanCacheFactor) (ClientPlanCachePolicyRow, bool) {
	for _, row := range rows {
		if row.Factor == factor {
			return row, true
		}
	}
	return ClientPlanCachePolicyRow{}, false
}
