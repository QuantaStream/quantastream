package qsbridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestSQLDriverConnectorQueriesThroughDatabaseSQL(t *testing.T) {
	db := sql.OpenDB(NewSQLDriverConnector(testSQLDriverPlanningService(), ExecutionDispatcher{Native: PlanOnlyNativeExecutor{}}))
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "select o_orderkey as order_id from orders where o_totalprice >= ?", 100.5)
	if err != nil {
		t.Fatalf("query through database/sql: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if len(columns) != 1 || columns[0] != "order_id" {
		t.Fatalf("columns = %#v, want order_id", columns)
	}
	if rows.Next() {
		t.Fatalf("plan-only driver returned a row, want schema-only result")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
}

func TestSQLDriverConnectorScansProjectedRowsThroughDatabaseSQL(t *testing.T) {
	executor := NewInMemoryNativeExecutor(InMemoryNativeTable{
		Name: "orders",
		Rows: []InMemoryNativeRow{
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(7)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(100.25)},
			},
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(8)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(101.5)},
			},
			{
				"o_orderkey":   {Kind: ValueInt, Value: int64(9)},
				"o_totalprice": {Kind: ValueFloat, Value: float64(102.75)},
			},
		},
	})
	db := sql.OpenDB(NewSQLDriverConnector(testSQLDriverProjectedRowsPlanningService(), ExecutionDispatcher{Native: executor}))
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice >= 101 and o.o_orderkey <= 8 order by o.o_totalprice desc limit 2")
	if err != nil {
		t.Fatalf("query through database/sql: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if got, want := strings.Join(columns, ","), "order_id,total_price"; got != want {
		t.Fatalf("columns = %q, want %q", got, want)
	}
	var scanned []string
	for rows.Next() {
		var orderID int64
		var totalPrice float64
		if err := rows.Scan(&orderID, &totalPrice); err != nil {
			t.Fatalf("scan: %v", err)
		}
		scanned = append(scanned, strconv.FormatInt(orderID, 10)+":"+strconv.FormatFloat(totalPrice, 'f', 2, 64))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if got, want := strings.Join(scanned, ","), "8:101.50"; got != want {
		t.Fatalf("rows = %q, want %q", got, want)
	}
}

func TestSQLDriverRowsIteratesResultChunksWithoutFlattening(t *testing.T) {
	rows := newSQLDriverRows(ExecutionResult{
		Columns: []ResultColumn{
			{Name: "order_id", Type: DataTypeInt},
		},
		Chunks: []ResultChunk{
			{
				Sequence: 1,
				Rows: []ResultRow{
					{{Kind: ValueInt, Value: int64(7)}},
				},
			},
			{
				Sequence: 2,
				Rows: []ResultRow{
					nil,
					{{Kind: ValueInt, Value: int64(8)}},
				},
				Final: true,
			},
		},
	})
	defer rows.Close()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("first next: %v", err)
	}
	if dest[0] != int64(7) {
		t.Fatalf("first row = %#v, want 7", dest[0])
	}
	if err := rows.Next(dest); err != nil {
		t.Fatalf("second next: %v", err)
	}
	if dest[0] != int64(8) {
		t.Fatalf("second row = %#v, want 8", dest[0])
	}
	if err := rows.Next(dest); err != io.EOF {
		t.Fatalf("third next = %v, want EOF", err)
	}
}

func TestSQLDriverConnectorExecsThroughDatabaseSQL(t *testing.T) {
	db := sql.OpenDB(NewSQLDriverConnector(testSQLDriverStatementPlanningService(), ExecutionDispatcher{Native: PlanOnlyNativeExecutor{}}))
	defer db.Close()

	result, err := db.ExecContext(context.Background(), "delete from orders where o_orderkey = ?", int64(7))
	if err != nil {
		t.Fatalf("exec through database/sql: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}
	if affected != 3 {
		t.Fatalf("rows affected = %d, want 3", affected)
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if lastInsertID != 11 {
		t.Fatalf("last insert id = %d, want 11", lastInsertID)
	}
}

func TestSQLDriverConnectorExecsPreparedStatementThroughDatabaseSQL(t *testing.T) {
	db := sql.OpenDB(NewSQLDriverConnector(testSQLDriverStatementPlanningService(), ExecutionDispatcher{Native: PlanOnlyNativeExecutor{}}))
	defer db.Close()

	stmt, err := db.PrepareContext(context.Background(), "delete from orders where o_orderkey = ?")
	if err != nil {
		t.Fatalf("prepare through database/sql: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(context.Background(), int64(7))
	if err != nil {
		t.Fatalf("prepared exec through database/sql: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}
	if affected != 3 {
		t.Fatalf("rows affected = %d, want 3", affected)
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if lastInsertID != 11 {
		t.Fatalf("last insert id = %d, want 11", lastInsertID)
	}
}

func TestSQLDriverConnectorReturnsPlanningDiagnosticsAsErrors(t *testing.T) {
	db := sql.OpenDB(NewSQLDriverConnector(testSQLDriverPlanningService(), ExecutionDispatcher{Native: PlanOnlyNativeExecutor{}}))
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "select o_orderkey as order_id from orders where o_totalprice >= ?", "bad")
	if rows != nil {
		rows.Close()
	}
	if err == nil {
		t.Fatalf("expected parameter diagnostic error")
	}
	if !strings.Contains(err.Error(), "parameter") {
		t.Fatalf("error = %q, want parameter diagnostic", err.Error())
	}
}

func testSQLDriverPlanningService() PlanningService {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_orderkey as order_id from orders where o_totalprice >= ?",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_orderkey"),
				Alias: "order_id",
				Type:  DataTypeInt,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpGreaterEqual,
					UnboundField("o", "o_totalprice"),
					UnboundParameter(1, DataTypeFloat),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	return NewPlanningService(planner, nil)
}

func testSQLDriverProjectedRowsPlanningService() PlanningService {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	return NewPlanningService(planner, nil)
}

func testSQLDriverStatementPlanningService() PlanningService {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "delete from orders where o_orderkey = ?",
		Kind: QueryKindDelete,
		Delete: UnboundDelete{
			Table: UnboundTable{Name: "orders"},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("", "o_orderkey"),
					UnboundParameter(1, DataTypeInt),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{
				Kind: ResultStatement,
				Statement: StatementResult{
					AffectedRows: 3,
					LastInsertID: 11,
					Status:       "deleted",
				},
			},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	return NewPlanningService(planner, nil)
}
