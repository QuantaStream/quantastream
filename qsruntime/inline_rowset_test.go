package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSQLRuntimeInlineConstantUnionDerivedTableOrderLimitOffset(t *testing.T) {
	runtime, executed := newInlineRowSetTestRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), `
select n
from (
  select 3 as n
  union all select 1
  union all select 2
) as numbers
order by n
limit 1 offset 1`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if *executed {
		t.Fatalf("inline rowset query should not dispatch to direct executor")
	}
	rows := inlineRowSetTestRows(t, result)
	if len(rows) != 1 || rows[0][0].Value != int64(2) {
		t.Fatalf("rows = %#v, want n=2", rows)
	}
}

func TestSQLRuntimeInlineConstantUnionDerivedTableOrderByAlias(t *testing.T) {
	runtime, _ := newInlineRowSetTestRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), `
select score * 2 as doubled_score
from (
  select 7 as score
  union all select 4
  union all select 11
) as ranked
order by doubled_score desc
limit 2`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	rows := inlineRowSetTestRows(t, result)
	if len(rows) != 2 || rows[0][0].Value != float64(22) || rows[1][0].Value != float64(14) {
		t.Fatalf("rows = %#v, want doubled scores 22 and 14", rows)
	}
}

func TestSQLRuntimeInlineConstantUnionDerivedTableGroupedHaving(t *testing.T) {
	runtime, _ := newInlineRowSetTestRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), `
select region, sum(amount) as total_amount
from (
  select 'east' as region, 12 as amount
  union all select 'east', 5
  union all select 'west', 2
) as sales
group by region
having total_amount > 10
order by total_amount desc`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	rows := inlineRowSetTestRows(t, result)
	if len(rows) != 1 || rows[0][0].Value != "east" || rows[0][1].Value != float64(17) {
		t.Fatalf("rows = %#v, want east total 17", rows)
	}
}

func TestSQLRuntimeInlineConstantUnionDerivedTableInnerJoin(t *testing.T) {
	runtime, _ := newInlineRowSetTestRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), `
select c.name, o.order_id
from (
  select 1 as cust_id, 'Abe' as name
  union all select 2, 'Abby'
) as c
inner join (
  select 1001 as order_id, 1 as cust_id
  union all select 1002, 1
) as o on o.cust_id = c.cust_id
order by o.order_id`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	rows := inlineRowSetTestRows(t, result)
	if len(rows) != 2 ||
		rows[0][0].Value != "Abe" || rows[0][1].Value != int64(1001) ||
		rows[1][0].Value != "Abe" || rows[1][1].Value != int64(1002) {
		t.Fatalf("rows = %#v, want two Abe orders", rows)
	}
}

func TestSQLRuntimeInlineConstantUnionDerivedTableLeftJoinNullExtension(t *testing.T) {
	runtime, _ := newInlineRowSetTestRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), `
select c.name, o.order_id
from (
  select 1 as cust_id, 'Abe' as name
  union all select 2, 'Abby'
) as c
left join (
  select 1001 as order_id, 1 as cust_id
) as o on c.cust_id = o.cust_id
order by c.cust_id, o.order_id`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	rows := inlineRowSetTestRows(t, result)
	if len(rows) != 2 ||
		rows[0][0].Value != "Abe" || rows[0][1].Value != int64(1001) ||
		rows[1][0].Value != "Abby" || rows[1][1].Kind != qsbridge.ValueNull {
		t.Fatalf("rows = %#v, want Abe order and Abby null extension", rows)
	}
}

func newInlineRowSetTestRuntime(t *testing.T) (SQLRuntime, *bool) {
	t.Helper()
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	return runtime, &executed
}

func inlineRowSetTestRows(t *testing.T, result SQLExecutionResult) []qsbridge.ResultRow {
	t.Helper()
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	return chunk.Rows
}
