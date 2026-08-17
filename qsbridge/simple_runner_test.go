package qsbridge

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

type simpleRunnerCase struct {
	name            string
	querySQL        string
	args            []any
	prepared        bool
	tables          []InMemoryNativeTable
	expectedColumns []string
	expectedRows    [][]string
}

func TestSimpleRunnerRunsProjectedFilterOrderLimitCase(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_filter_order_limit",
		querySQL: "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice >= 101 and o.o_orderkey <= 8 order by o.o_totalprice desc limit 2",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
			"total_price",
		},
		expectedRows: [][]string{
			{"8", "101.5"},
		},
	})
}

func TestSimpleRunnerReturnsEmptyRowsForValidSelect(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_valid_select_empty_result",
		querySQL: "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice < 100 order by o.o_totalprice desc limit 2",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
			"total_price",
		},
		expectedRows: nil,
	})
}

func TestSimpleRunnerRunsStringEqualityFilter(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "customer_string_filter",
		querySQL: "select c.c_custkey as customer_id, c.c_name as customer_name from customer as c where c.c_name = 'Annie' order by c.c_custkey limit 2",
		tables:   simpleRunnerCustomerFixture(),
		expectedColumns: []string{
			"customer_id",
			"customer_name",
		},
		expectedRows: [][]string{
			{"2", "Annie"},
		},
	})
}

func TestSimpleRunnerRunsBetweenFilter(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_between_filter",
		querySQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey between ? and ? order by o.o_orderkey",
		args: []any{
			int64(8),
			int64(9),
		},
		tables: simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
		},
		expectedRows: [][]string{
			{"8"},
			{"9"},
		},
	})
}

func TestSimpleRunnerRunsInFilter(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_in_filter",
		querySQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey in (?, ?) order by o.o_orderkey",
		args: []any{
			int64(7),
			int64(9),
		},
		tables: simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
		},
		expectedRows: [][]string{
			{"7"},
			{"9"},
		},
	})
}

func TestSimpleRunnerRunsLimitOffset(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_limit_offset",
		querySQL: "select o.o_orderkey as order_id from orders as o order by o.o_orderkey limit 1 offset 1",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
		},
		expectedRows: [][]string{
			{"8"},
		},
	})
}

func TestSimpleRunnerRunsCountStar(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_count_star",
		querySQL: "select count(*) as order_count from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_count",
		},
		expectedRows: [][]string{
			{"2"},
		},
	})
}

func TestSimpleRunnerRunsGlobalSum(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_global_sum",
		querySQL: "select sum(o.o_totalprice) as revenue from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"revenue",
		},
		expectedRows: [][]string{
			{"204.25"},
		},
	})
}

func TestSimpleRunnerRunsGlobalAvg(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_global_avg",
		querySQL: "select avg(o.o_totalprice) as avg_total from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"avg_total",
		},
		expectedRows: [][]string{
			{"102.125"},
		},
	})
}

func TestSimpleRunnerRunsGlobalMin(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_global_min",
		querySQL: "select min(o.o_totalprice) as min_total from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"min_total",
		},
		expectedRows: [][]string{
			{"101.5"},
		},
	})
}

func TestSimpleRunnerRunsGlobalMax(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_global_max",
		querySQL: "select max(o.o_totalprice) as max_total from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"max_total",
		},
		expectedRows: [][]string{
			{"102.75"},
		},
	})
}

func TestSimpleRunnerRunsMultipleGlobalAggregates(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_multiple_global_aggregates",
		querySQL: "select count(*) as order_count, sum(o.o_totalprice) as revenue, avg(o.o_totalprice) as avg_total from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_count",
			"revenue",
			"avg_total",
		},
		expectedRows: [][]string{
			{"2", "204.25", "102.125"},
		},
	})
}

func TestSimpleRunnerReturnsSQLAggregateValuesForEmptyInput(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_empty_global_aggregates",
		querySQL: "select count(*) as order_count, sum(o.o_totalprice) as revenue, avg(o.o_totalprice) as avg_total, min(o.o_totalprice) as min_total, max(o.o_totalprice) as max_total from orders as o where o.o_totalprice < 0",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_count",
			"revenue",
			"avg_total",
			"min_total",
			"max_total",
		},
		expectedRows: [][]string{
			{"0", "NULL", "NULL", "NULL", "NULL"},
		},
	})
}

func TestSimpleRunnerRunsArithmeticProjection(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_arithmetic_projection",
		querySQL: "select o.o_totalprice * 2 as doubled_total from orders as o where o.o_orderkey = 8",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"doubled_total",
		},
		expectedRows: [][]string{
			{"203"},
		},
	})
}

func TestSimpleRunnerRunsAggregateExpressionInput(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_aggregate_expression_input",
		querySQL: "select sum(o.o_totalprice * 2) as doubled_revenue from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"doubled_revenue",
		},
		expectedRows: [][]string{
			{"408.5"},
		},
	})
}

func TestSimpleRunnerRunsNestedAggregateExpressionInput(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_nested_aggregate_expression_input",
		querySQL: "select sum(o.o_totalprice * (1 - o.o_discount)) as discounted_revenue from orders as o where o.o_totalprice >= 101",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"discounted_revenue",
		},
		expectedRows: [][]string{
			{"153.5"},
		},
	})
}

func TestSimpleRunnerRunsProjectionOnlyWhere(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "projection_only_where",
		querySQL: "select 1 as matched where 3 > 2 and 2 <= 2",
		expectedColumns: []string{
			"matched",
		},
		expectedRows: [][]string{
			{"1"},
		},
	})
}

func TestSimpleRunnerReturnsEmptyRowsForProjectionOnlyWhereMiss(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "projection_only_where_miss",
		querySQL: "select 1 as matched where 3 < 2",
		expectedColumns: []string{
			"matched",
		},
		expectedRows: nil,
	})
}

func TestSimpleRunnerRunsGroupedCount(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_count",
		querySQL: "select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"order_count",
		},
		expectedRows: [][]string{
			{"1", "2"},
			{"2", "1"},
		},
	})
}

func TestSimpleRunnerRunsGroupedSum(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_sum",
		querySQL: "select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"total_revenue",
		},
		expectedRows: [][]string{
			{"1", "201.75"},
			{"2", "102.75"},
		},
	})
}

func TestSimpleRunnerRunsGroupedAvgMinMax(t *testing.T) {
	cases := []simpleRunnerCase{
		{
			name:     "orders_grouped_avg",
			querySQL: "select o.o_custkey as customer_id, avg(o.o_totalprice) as avg_revenue from orders as o group by o.o_custkey",
			tables:   simpleRunnerOrdersFixture(),
			expectedColumns: []string{
				"customer_id",
				"avg_revenue",
			},
			expectedRows: [][]string{
				{"1", "100.875"},
				{"2", "102.75"},
			},
		},
		{
			name:     "orders_grouped_min",
			querySQL: "select o.o_custkey as customer_id, min(o.o_totalprice) as min_revenue from orders as o group by o.o_custkey",
			tables:   simpleRunnerOrdersFixture(),
			expectedColumns: []string{
				"customer_id",
				"min_revenue",
			},
			expectedRows: [][]string{
				{"1", "100.25"},
				{"2", "102.75"},
			},
		},
		{
			name:     "orders_grouped_max",
			querySQL: "select o.o_custkey as customer_id, max(o.o_totalprice) as max_revenue from orders as o group by o.o_custkey",
			tables:   simpleRunnerOrdersFixture(),
			expectedColumns: []string{
				"customer_id",
				"max_revenue",
			},
			expectedRows: [][]string{
				{"1", "101.5"},
				{"2", "102.75"},
			},
		},
	}
	for _, runnerCase := range cases {
		runSimpleRunnerCase(t, runnerCase)
	}
}

func TestSimpleRunnerRunsMultipleGroupedAggregates(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_multiple_grouped_aggregates",
		querySQL: "select o.o_custkey as customer_id, count(*) as order_count, sum(o.o_totalprice) as total_revenue, avg(o.o_totalprice) as avg_revenue from orders as o group by o.o_custkey",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"order_count",
			"total_revenue",
			"avg_revenue",
		},
		expectedRows: [][]string{
			{"1", "2", "201.75", "100.875"},
			{"2", "1", "102.75", "102.75"},
		},
	})
}

func TestSimpleRunnerOrdersGroupedAggregatesByAggregateAlias(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_order_by_aggregate_alias",
		querySQL: "select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey order by order_count asc limit 1",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"order_count",
		},
		expectedRows: [][]string{
			{"2", "1"},
		},
	})
}

func TestSimpleRunnerOrdersGroupedAggregatesByAggregateCall(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_order_by_aggregate_call",
		querySQL: "select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey order by count(*) asc limit 1",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"order_count",
		},
		expectedRows: [][]string{
			{"2", "1"},
		},
	})
}

func TestSimpleRunnerOrdersGroupedAggregatesByAggregateInputCall(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_order_by_aggregate_input_call",
		querySQL: "select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey order by sum(o.o_totalprice) desc limit 1",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"total_revenue",
		},
		expectedRows: [][]string{
			{"1", "201.75"},
		},
	})
}

func TestSimpleRunnerRunsGroupedHavingAggregateAlias(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_having_aggregate_alias",
		querySQL: "select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey having total_revenue > 150 order by total_revenue desc",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"total_revenue",
		},
		expectedRows: [][]string{
			{"1", "201.75"},
		},
	})
}

func TestSimpleRunnerRunsGroupedHavingAggregateCall(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_grouped_having_aggregate_call",
		querySQL: "select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey having sum(o.o_totalprice) > 150 order by sum(o.o_totalprice) desc",
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"customer_id",
			"total_revenue",
		},
		expectedRows: [][]string{
			{"1", "201.75"},
		},
	})
}

func TestSimpleRunnerScansBatchSizedChunks(t *testing.T) {
	connector := NewSQLDriverConnector(simpleRunnerPlanningService(), ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(simpleRunnerOrdersFixture()...),
	})
	connector.Options = ExecutionOptions{RequestID: "chunked-1", BatchSize: 1}
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "select o.o_orderkey as order_id from orders as o order by o.o_orderkey")
	if err != nil {
		t.Fatalf("query through database/sql: %v", err)
	}
	defer rows.Close()

	scanned := make([]string, 0)
	for rows.Next() {
		var orderID int64
		if err := rows.Scan(&orderID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		scanned = append(scanned, strconv.FormatInt(orderID, 10))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if got, want := strings.Join(scanned, ","), "7,8,9"; got != want {
		t.Fatalf("rows = %q, want %q", got, want)
	}
}

func TestSimpleRunnerHonorsExecutionMaxRows(t *testing.T) {
	connector := NewSQLDriverConnector(simpleRunnerPlanningService(), ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(simpleRunnerOrdersFixture()...),
	})
	connector.Options = ExecutionOptions{RequestID: "maxrows-1", MaxRows: 2}
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "select o.o_orderkey as order_id from orders as o order by o.o_orderkey")
	if err != nil {
		t.Fatalf("query through database/sql: %v", err)
	}
	defer rows.Close()

	scanned := make([]string, 0)
	for rows.Next() {
		var orderID int64
		if err := rows.Scan(&orderID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		scanned = append(scanned, strconv.FormatInt(orderID, 10))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if got, want := strings.Join(scanned, ","), "7,8"; got != want {
		t.Fatalf("rows = %q, want %q", got, want)
	}
}

func TestSimpleRunnerBindsDatabaseSQLParameters(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_parameterized_filter",
		querySQL: "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice >= ? and o.o_orderkey <= ? order by o.o_totalprice desc limit 2",
		args: []any{
			float64(101),
			int64(8),
		},
		tables: simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
			"total_price",
		},
		expectedRows: [][]string{
			{"8", "101.5"},
		},
	})
}

func TestSimpleRunnerQueriesPreparedStatement(t *testing.T) {
	runSimpleRunnerCase(t, simpleRunnerCase{
		name:     "orders_prepared_statement_filter",
		querySQL: "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice >= ? and o.o_orderkey <= ? order by o.o_totalprice desc limit 2",
		args: []any{
			float64(101),
			int64(8),
		},
		prepared: true,
		tables:   simpleRunnerOrdersFixture(),
		expectedColumns: []string{
			"order_id",
			"total_price",
		},
		expectedRows: [][]string{
			{"8", "101.5"},
		},
	})
}

func TestSimpleRunnerReportsPreparedArgumentArity(t *testing.T) {
	db := sql.OpenDB(NewSQLDriverConnector(simpleRunnerPlanningService(), ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(simpleRunnerOrdersFixture()...),
	}))
	defer db.Close()

	stmt, err := db.PrepareContext(context.Background(), "select o.o_orderkey as order_id from orders as o where o.o_totalprice >= ?")
	if err != nil {
		t.Fatalf("prepare through database/sql: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background())
	if rows != nil {
		rows.Close()
	}
	if err == nil {
		t.Fatalf("expected prepared argument arity error")
	}
	if !strings.Contains(err.Error(), "expected 1 arguments") {
		t.Fatalf("error = %q, want prepared argument arity error", err.Error())
	}
}

func runSimpleRunnerCase(t *testing.T, runnerCase simpleRunnerCase) {
	t.Helper()

	t.Run(runnerCase.name, func(t *testing.T) {
		columns, rows := simpleRunnerRowsFromSQL(t, runnerCase.querySQL, runnerCase.args, runnerCase.prepared, runnerCase.tables)
		if got, want := strings.Join(columns, ","), strings.Join(runnerCase.expectedColumns, ","); got != want {
			t.Fatalf("columns = %q, want %q", got, want)
		}
		if got, want := simpleRunnerJoinRows(rows), simpleRunnerJoinRows(runnerCase.expectedRows); got != want {
			t.Fatalf("rows = %q, want %q", got, want)
		}
	})
}

func simpleRunnerRowsFromSQL(t *testing.T, querySQL string, args []any, prepared bool, tables []InMemoryNativeTable) ([]string, [][]string) {
	t.Helper()

	db := sql.OpenDB(NewSQLDriverConnector(simpleRunnerPlanningService(), ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(tables...),
	}))
	defer db.Close()

	rows, err := simpleRunnerQueryContext(t, db, querySQL, args, prepared)
	if err != nil {
		t.Fatalf("query through database/sql: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	scannedRows := make([][]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range destinations {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		scanned := make([]string, len(values))
		for i, value := range values {
			scanned[i] = simpleRunnerValueString(value)
		}
		scannedRows = append(scannedRows, scanned)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return columns, scannedRows
}

func simpleRunnerQueryContext(t *testing.T, db *sql.DB, querySQL string, args []any, prepared bool) (*sql.Rows, error) {
	t.Helper()

	if prepared {
		stmt, err := db.PrepareContext(context.Background(), querySQL)
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() {
			_ = stmt.Close()
		})
		return stmt.QueryContext(context.Background(), args...)
	}
	return db.QueryContext(context.Background(), querySQL, args...)
}

func simpleRunnerPlanningService() PlanningService {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	return NewPlanningService(planner, nil)
}

func simpleRunnerOrdersFixture() []InMemoryNativeTable {
	return []InMemoryNativeTable{{
		Name: "orders",
		Rows: []InMemoryNativeRow{
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(7)},
				"o_custkey":    {Kind: ValueInt, Value: int64(1)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(100.25)},
				"o_discount":   {Kind: ValueFloat, Value: float64(0)},
			},
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(8)},
				"o_custkey":    {Kind: ValueInt, Value: int64(1)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(101.5)},
				"o_discount":   {Kind: ValueFloat, Value: float64(0.5)},
			},
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(9)},
				"o_custkey":    {Kind: ValueInt, Value: int64(2)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(102.75)},
				"o_discount":   {Kind: ValueFloat, Value: float64(0)},
			},
		},
	}}
}

func simpleRunnerCustomerFixture() []InMemoryNativeTable {
	return []InMemoryNativeTable{{
		Name: "customer",
		Rows: []InMemoryNativeRow{
			{
				"c_custkey": {Kind: ValueInt, Value: int64(1)},
				"c_name":    {Kind: ValueString, Value: "Abe"},
			},
			{
				"c_custkey": {Kind: ValueInt, Value: int64(2)},
				"c_name":    {Kind: ValueString, Value: "Annie"},
			},
			{
				"c_custkey": {Kind: ValueInt, Value: int64(3)},
				"c_name":    {Kind: ValueString, Value: "Abby"},
			},
		},
	}}
}

func simpleRunnerJoinRows(rows [][]string) string {
	joined := make([]string, len(rows))
	for i, row := range rows {
		joined[i] = strings.Join(row, ":")
	}
	return strings.Join(joined, ",")
}

func simpleRunnerValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(typed)
	case string:
		return typed
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprint(typed)
	}
}
