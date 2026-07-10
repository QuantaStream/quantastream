package qsfixture

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
)

func TestRuntimeFixtureMaterializesSelectedCandidates(t *testing.T) {
	candidates, err := runtimeFixtureSelectCandidates("orders", nil, runtimeFixtureOrders)
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if got := candidates.Set.CandidateCount(); got != 3 {
		t.Fatalf("candidate count = %d, want 3", got)
	}

	candidates = runtimeFixtureLimitCandidates(candidates, 1, 1)
	materialization := candidates.Set.MaterializationRequest([]qsbridge.QuantaProjectionField{{
		Index:   "orders",
		Field:   "o_orderkey",
		Type:    qsbridge.DataTypeInt,
		Visible: true,
	}})
	rowSet, err := runtimeFixtureMaterializeRows(materialization, candidates.Rows)
	if err != nil {
		t.Fatalf("materialize rows: %v", err)
	}
	if got := rowSet.CandidateCount(); got != 1 {
		t.Fatalf("materialized candidate count = %d, want 1", got)
	}
	if got := rowSet.Rownums[0]; got != qsbridge.QuantaRownum(2) {
		t.Fatalf("rownum = %d, want 2", got)
	}
	if got := rowSet.ProjectionVectors[0].Values[0].Value; got != int64(1002) {
		t.Fatalf("projected order key = %v, want 1002", got)
	}
}

func TestRuntimeFixtureInsertsAndProjectsCustomersQA(t *testing.T) {
	store := newRuntimeFixtureStore()
	request := qsruntime.ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationInsert,
			Target: qsbridge.TableInstance{Table: "customers_qa"},
			Columns: []qsbridge.FieldRef{
				{Name: "cust_id", Type: qsbridge.DataTypeString},
				{Name: "first_name", Type: qsbridge.DataTypeString},
				{Name: "city", Type: qsbridge.DataTypeString},
			},
			Rows: []qsbridge.MutationRow{
				{Values: []qsbridge.Expr{
					qsbridge.Literal(qsbridge.ValueString, "101"),
					qsbridge.Literal(qsbridge.ValueString, "Abe"),
					qsbridge.Literal(qsbridge.ValueString, "Seattle"),
				}},
				{Values: []qsbridge.Expr{
					qsbridge.Literal(qsbridge.ValueString, "102"),
					qsbridge.Literal(qsbridge.ValueString, "Abby"),
					qsbridge.Literal(qsbridge.ValueString, "Tacoma"),
				}},
			},
		},
	}

	result, err := store.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("insert diagnostics: %#v", result.Diagnostics)
	}
	if result.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.Statement.AffectedRows)
	}

	queryResult, err := store.ExecuteDirect(context.Background(), qsruntime.ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "customers_qa", Field: "first_name", Type: qsbridge.DataTypeString, Visible: true},
				{Index: "customers_qa", Field: "city", Type: qsbridge.DataTypeString, Visible: true},
			},
		},
		SourceIndexes: []string{"customers_qa"},
		Result:        qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if queryResult.Diagnostics.BlocksNative() {
		t.Fatalf("query diagnostics: %#v", queryResult.Diagnostics)
	}
	if queryResult.RowSet.CandidateCount() != 2 {
		t.Fatalf("candidate count = %d, want 2", queryResult.RowSet.CandidateCount())
	}
	if got := queryResult.RowSet.ProjectionVectors[0].Values[1].Value; got != "Abby" {
		t.Fatalf("second first_name = %v, want Abby", got)
	}

	filteredResult, err := store.ExecuteDirect(context.Background(), qsruntime.ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			Fragments: []qsbridge.QuantaQueryFragment{{
				Index:      "customers_qa",
				Field:      "first_name",
				Operation:  qsbridge.QuantaOperationIntersect,
				Literal:    qsbridge.Literal(qsbridge.ValueString, "Abby"),
				HasLiteral: true,
			}},
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "customers_qa", Field: "city", Type: qsbridge.DataTypeString, Visible: true},
			},
		},
		SourceIndexes: []string{"customers_qa"},
		Result:        qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	})
	if err != nil {
		t.Fatalf("filtered query: %v", err)
	}
	if filteredResult.Diagnostics.BlocksNative() {
		t.Fatalf("filtered query diagnostics: %#v", filteredResult.Diagnostics)
	}
	if filteredResult.RowSet.CandidateCount() != 1 {
		t.Fatalf("filtered candidate count = %d, want 1", filteredResult.RowSet.CandidateCount())
	}
	if got := filteredResult.RowSet.ProjectionVectors[0].Values[0].Value; got != "Tacoma" {
		t.Fatalf("filtered city = %v, want Tacoma", got)
	}

	inResult, err := store.ExecuteDirect(context.Background(), qsruntime.ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			Fragments: []qsbridge.QuantaQueryFragment{{
				Index:     "customers_qa",
				Field:     "city",
				Operation: qsbridge.QuantaOperationIntersect,
				Literals: []qsbridge.LiteralExpr{
					qsbridge.Literal(qsbridge.ValueString, "Seattle"),
					qsbridge.Literal(qsbridge.ValueString, "Tacoma"),
				},
			}},
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "customers_qa", Field: "first_name", Type: qsbridge.DataTypeString, Visible: true},
			},
		},
		SourceIndexes: []string{"customers_qa"},
		Result:        qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	})
	if err != nil {
		t.Fatalf("in query: %v", err)
	}
	if inResult.Diagnostics.BlocksNative() {
		t.Fatalf("in query diagnostics: %#v", inResult.Diagnostics)
	}
	if inResult.RowSet.CandidateCount() != 2 {
		t.Fatalf("in candidate count = %d, want 2", inResult.RowSet.CandidateCount())
	}

	generatedNotNullResult, err := store.ExecuteDirect(context.Background(), qsruntime.ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			Fragments: []qsbridge.QuantaQueryFragment{{
				Index:     "customers_qa",
				Field:     "createdAtTimestamp",
				Operation: qsbridge.QuantaOperationIntersect,
				NullCheck: true,
				Negate:    true,
			}},
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "customers_qa", Field: "first_name", Type: qsbridge.DataTypeString, Visible: true},
			},
		},
		SourceIndexes: []string{"customers_qa"},
		Result:        qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	})
	if err != nil {
		t.Fatalf("generated not null query: %v", err)
	}
	if generatedNotNullResult.Diagnostics.BlocksNative() {
		t.Fatalf("generated not null query diagnostics: %#v", generatedNotNullResult.Diagnostics)
	}
	if generatedNotNullResult.RowSet.CandidateCount() != 2 {
		t.Fatalf("generated not null candidate count = %d, want 2", generatedNotNullResult.RowSet.CandidateCount())
	}
}

func TestRuntimeFixtureInsertsAndProjectsOrdersQA(t *testing.T) {
	store := newRuntimeFixtureStore()
	request := qsruntime.ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationInsert,
			Target: qsbridge.TableInstance{Table: "orders_qa"},
			Columns: []qsbridge.FieldRef{
				{Name: "cust_id", Type: qsbridge.DataTypeString},
				{Name: "order_id", Type: qsbridge.DataTypeString},
				{Name: "order_date", Type: qsbridge.DataTypeTime},
				{Name: "ship_date", Type: qsbridge.DataTypeTime},
				{Name: "ship_via", Type: qsbridge.DataTypeString},
			},
			Rows: []qsbridge.MutationRow{{
				Values: []qsbridge.Expr{
					qsbridge.Literal(qsbridge.ValueString, "101"),
					qsbridge.Literal(qsbridge.ValueString, "1001"),
					qsbridge.Literal(qsbridge.ValueString, "2023-06-01 01:00:00"),
					qsbridge.Literal(qsbridge.ValueString, "2023-06-04 05:00:00"),
					qsbridge.Literal(qsbridge.ValueString, "truck"),
				},
			}},
		},
	}

	result, err := store.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("insert orders_qa: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("insert diagnostics: %#v", result.Diagnostics)
	}
	if result.Statement.AffectedRows != 1 {
		t.Fatalf("affected rows = %d, want 1", result.Statement.AffectedRows)
	}

	queryResult, err := store.ExecuteDirect(context.Background(), qsruntime.ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "orders_qa", Field: "order_id", Type: qsbridge.DataTypeString, Visible: true},
				{Index: "orders_qa", Field: "order_date", Type: qsbridge.DataTypeTime, Visible: true},
				{Index: "orders_qa", Field: "ship_via", Type: qsbridge.DataTypeString, Visible: true},
			},
		},
		SourceIndexes: []string{"orders_qa"},
		Result:        qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	})
	if err != nil {
		t.Fatalf("query orders_qa: %v", err)
	}
	if queryResult.Diagnostics.BlocksNative() {
		t.Fatalf("query diagnostics: %#v", queryResult.Diagnostics)
	}
	if queryResult.RowSet.CandidateCount() != 1 {
		t.Fatalf("candidate count = %d, want 1", queryResult.RowSet.CandidateCount())
	}
	if got := queryResult.RowSet.ProjectionVectors[0].Values[0].Value; got != "1001" {
		t.Fatalf("order_id = %v, want 1001", got)
	}
	if queryResult.RowSet.ProjectionVectors[1].Values[0].Kind != qsbridge.ValueInt {
		t.Fatalf("order_date kind = %s, want int epoch millis", queryResult.RowSet.ProjectionVectors[1].Values[0].Kind)
	}
	if got := queryResult.RowSet.ProjectionVectors[2].Values[0].Value; got != "truck" {
		t.Fatalf("ship_via = %v, want truck", got)
	}
}

func TestRuntimeFixtureDistinctRowsKeepsFirstVisibleTuple(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "customers_qa",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "customers_qa", Field: "first_name", Type: qsbridge.DataTypeString, Visible: true},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "Abe"},
					{Kind: qsbridge.ValueString, Value: "Abe"},
					{Kind: qsbridge.ValueString, Value: "Abby"},
				},
			},
		},
	}

	distinct := runtimeFixtureDistinctRows(rowSet)
	if distinct.CandidateCount() != 2 {
		t.Fatalf("distinct candidate count = %d, want 2", distinct.CandidateCount())
	}
	if distinct.Rownums[0] != 1 || distinct.Rownums[1] != 3 {
		t.Fatalf("distinct rownums = %#v, want first instances 1 and 3", distinct.Rownums)
	}
	if got := distinct.ProjectionVectors[0].Values[1].Value; got != "Abby" {
		t.Fatalf("second distinct value = %v, want Abby", got)
	}
}

func TestRuntimeFixtureCellEqualComparesBoolAndNumericValues(t *testing.T) {
	if !runtimeFixtureCellEqual(
		qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: true},
		qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(1)},
	) {
		t.Fatalf("true should compare equal to 1")
	}
	if !runtimeFixtureCellEqual(
		qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: false},
		qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)},
	) {
		t.Fatalf("false should compare equal to 0")
	}
	if runtimeFixtureCellEqual(
		qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: true},
		qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)},
	) {
		t.Fatalf("true should not compare equal to 0")
	}
}

func TestRuntimeFixtureMatchesNullCheckFragments(t *testing.T) {
	row := runtimeFixtureRow{
		"city": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Seattle"},
	}
	isNull := qsbridge.QuantaQueryFragment{
		Index:     "customers_qa",
		Field:     "missing",
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
	}
	matched, err := runtimeFixtureMatchFragment(isNull, row)
	if err != nil {
		t.Fatalf("is null match: %v", err)
	}
	if !matched {
		t.Fatalf("missing field IS NULL did not match")
	}

	isNotNull := qsbridge.QuantaQueryFragment{
		Index:     "customers_qa",
		Field:     "city",
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}
	matched, err = runtimeFixtureMatchFragment(isNotNull, row)
	if err != nil {
		t.Fatalf("is not null match: %v", err)
	}
	if !matched {
		t.Fatalf("city IS NOT NULL did not match")
	}
}

func TestRuntimeFixtureNormalizesTimeCells(t *testing.T) {
	cell := runtimeFixtureNormalizeTimeCell(qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "2011-03-01 00:00:00.000Z"})
	if cell.Kind != qsbridge.ValueInt {
		t.Fatalf("normalized kind = %s, want int", cell.Kind)
	}
	if got, ok := cell.Value.(int64); !ok || got <= 0 {
		t.Fatalf("normalized value = %#v, want epoch millis", cell.Value)
	}

	empty := runtimeFixtureNormalizeTimeCell(qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""})
	if empty.Kind != qsbridge.ValueNull {
		t.Fatalf("empty normalized kind = %s, want null", empty.Kind)
	}
}

func TestRuntimeFixtureGeneratedTimestampDefaultIsRecent(t *testing.T) {
	row := runtimeFixtureRow{}
	runtimeFixtureApplyGeneratedDefaults("customers_qa", row)

	fragment := qsbridge.QuantaQueryFragment{
		Index:     "customers_qa",
		Field:     "createdAtTimestamp",
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpGT,
		Value:     big.NewInt(runtimeFixtureReferenceNow().Add(-time.Minute).UnixMilli()),
	}
	matched, err := runtimeFixtureMatchFragment(fragment, row)
	if err != nil {
		t.Fatalf("match generated timestamp: %v", err)
	}
	if !matched {
		t.Fatalf("generated timestamp did not match recent-window predicate")
	}
}

func TestRuntimeFixtureSelectCandidatesAppliesUnionFragments(t *testing.T) {
	rows := []runtimeFixtureRow{
		{
			"first_name": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Carl"},
			"city":       qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Spokane"},
		},
		{
			"first_name": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Abe"},
			"city":       qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Seattle"},
		},
		{
			"first_name": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Bob"},
			"city":       qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Tacoma"},
		},
	}
	fragments := []qsbridge.QuantaQueryFragment{
		{
			Index:      "customers_qa",
			Field:      "first_name",
			Operation:  qsbridge.QuantaOperationUnion,
			Literal:    qsbridge.Literal(qsbridge.ValueString, "Carl"),
			HasLiteral: true,
		},
		{
			Index:      "customers_qa",
			Field:      "city",
			Operation:  qsbridge.QuantaOperationUnion,
			Literal:    qsbridge.Literal(qsbridge.ValueString, "Seattle"),
			HasLiteral: true,
		},
	}

	candidates, err := runtimeFixtureSelectCandidates("customers_qa", fragments, rows)
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if candidates.Set.CandidateCount() != 2 {
		t.Fatalf("candidate count = %d, want 2", candidates.Set.CandidateCount())
	}
}

func TestRuntimeFixtureMatchesSemicolonSeparatedStringSets(t *testing.T) {
	if !runtimeFixtureCellEqual(
		qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "cell;home"},
		qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "home"},
	) {
		t.Fatalf("cell;home did not match home token")
	}
	if runtimeFixtureCellEqual(
		qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "cell;home"},
		qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "landline"},
	) {
		t.Fatalf("cell;home matched unrelated landline token")
	}
}

func TestRuntimeFixtureSelectCandidatesAppliesUnionFragmentsOverStringSets(t *testing.T) {
	rows := []runtimeFixtureRow{
		{"phoneType": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "cell;home"}},
		{"phoneType": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "landline;business"}},
		{"phoneType": qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "business"}},
	}
	fragments := []qsbridge.QuantaQueryFragment{
		{
			Index:      "customers_qa",
			Field:      "phoneType",
			Operation:  qsbridge.QuantaOperationUnion,
			Literal:    qsbridge.Literal(qsbridge.ValueString, "home"),
			HasLiteral: true,
		},
		{
			Index:      "customers_qa",
			Field:      "phoneType",
			Operation:  qsbridge.QuantaOperationUnion,
			Literal:    qsbridge.Literal(qsbridge.ValueString, "landline"),
			HasLiteral: true,
		},
	}

	candidates, err := runtimeFixtureSelectCandidates("customers_qa", fragments, rows)
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if candidates.Set.CandidateCount() != 2 {
		t.Fatalf("candidate count = %d, want 2", candidates.Set.CandidateCount())
	}
}
