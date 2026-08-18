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
