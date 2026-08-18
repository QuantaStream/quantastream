package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSQLRuntimeCreateTemporaryTableUpdatesSessionCatalogMetadata(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	create, err := runtime.ExecuteSQL(context.Background(), "create temporary table if not exists scratch_keys (customer_key bigint not null, market_segment varchar(16), primary key (customer_key))", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE failed: %v", err)
	}
	if create.Diagnostics.BlocksNative() || create.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("create diagnostics = %#v runtime=%#v", create.Diagnostics, create.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("CREATE TEMPORARY TABLE should not dispatch to the direct executor")
	}
	actions := create.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionCreateTemporaryTable || actions[0].Table.Name != "scratch_keys" {
		t.Fatalf("create session actions = %#v", actions)
	}
	transition := runtime.Session.PreviewSessionTransition(actions)
	if transition.Diagnostics.BlocksNative() {
		t.Fatalf("session transition diagnostics = %#v", transition.Diagnostics)
	}
	runtime.Session = transition.After

	describe, err := runtime.ExecuteSQL(context.Background(), "show columns from scratch_keys", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COLUMNS failed: %v", err)
	}
	if describe.Diagnostics.BlocksNative() || describe.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("describe diagnostics = %#v runtime=%#v", describe.Diagnostics, describe.Runtime.Diagnostics)
	}
	chunk, diagnostics := describe.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("columns = %#v, want two temp columns", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "customer_key" || chunk.Rows[0][3].Value != "PRI" {
		t.Fatalf("first column row = %#v, want customer_key primary key", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "market_segment" {
		t.Fatalf("second column row = %#v, want market_segment", chunk.Rows[1])
	}

	drop, err := runtime.ExecuteSQL(context.Background(), "drop temporary table scratch_keys", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("DROP TEMPORARY TABLE failed: %v", err)
	}
	if drop.Diagnostics.BlocksNative() || drop.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("drop diagnostics = %#v runtime=%#v", drop.Diagnostics, drop.Runtime.Diagnostics)
	}
	transition = runtime.Session.PreviewSessionTransition(drop.Runtime.Statement.SessionActions)
	runtime.Session = transition.After

	missing, err := runtime.ExecuteSQL(context.Background(), "show columns from scratch_keys", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COLUMNS missing failed: %v", err)
	}
	if !missing.Diagnostics.BlocksNative() {
		t.Fatalf("missing diagnostics = %#v, want blocking table not found", missing.Diagnostics)
	}
}

func TestSQLRuntimeTemporaryTableRowsStayInSession(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	applyActions := func(result SQLExecutionResult) {
		t.Helper()
		transition := runtime.Session.PreviewSessionTransition(result.Runtime.Statement.SessionActions)
		if transition.Diagnostics.BlocksNative() {
			t.Fatalf("session transition diagnostics = %#v", transition.Diagnostics)
		}
		runtime.Session = transition.After
	}

	create, err := runtime.ExecuteSQL(context.Background(), "create temporary table scratch_keys (customer_key int not null, market_segment varchar(16), primary key (customer_key))", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE failed: %v", err)
	}
	if !create.Supported() {
		t.Fatalf("create diagnostics = %#v runtime=%#v", create.Diagnostics, create.Runtime.Diagnostics)
	}
	applyActions(create)

	insert, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys (customer_key, market_segment) values (2, 'BUILDING'), (1, 'AUTOMOBILE')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT temporary table failed: %v", err)
	}
	if !insert.Supported() {
		t.Fatalf("insert diagnostics = %#v runtime=%#v", insert.Diagnostics, insert.Runtime.Diagnostics)
	}
	if insert.Runtime.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", insert.Runtime.Statement.AffectedRows)
	}
	if executed {
		t.Fatalf("temporary table DML should not dispatch to the direct executor")
	}
	applyActions(insert)

	selectResult, err := runtime.ExecuteSQL(context.Background(), "select customer_key, market_segment from scratch_keys where customer_key in (1, 2) order by customer_key limit 2", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT temporary table failed: %v", err)
	}
	if !selectResult.Supported() {
		t.Fatalf("select diagnostics = %#v runtime=%#v", selectResult.Diagnostics, selectResult.Runtime.Diagnostics)
	}
	chunk, diagnostics := selectResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("result chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, int64(1); got != want {
		t.Fatalf("first customer_key = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "AUTOMOBILE"; got != want {
		t.Fatalf("first market_segment = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][0].Value, int64(2); got != want {
		t.Fatalf("second customer_key = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "BUILDING"; got != want {
		t.Fatalf("second market_segment = %#v, want %#v", got, want)
	}

	duplicate, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys (customer_key, market_segment) values (1, 'MACHINERY')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("duplicate INSERT temporary table failed: %v", err)
	}
	if !duplicate.Diagnostics.BlocksNative() {
		t.Fatalf("duplicate diagnostics = %#v, want duplicate primary key blocker", duplicate.Diagnostics)
	}
}
