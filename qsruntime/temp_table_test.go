package qsruntime

import (
	"context"
	"math/big"
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

func TestSQLRuntimeCreateTemporaryTableLikeClonesCatalogMetadata(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas: []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI, PrimaryKey: true, Nullable: false, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "c_name", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringEnum", qsbridge.LegacyEncodingOptions{})},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	create, err := runtime.ExecuteSQL(context.Background(), "create temporary table scratch_customer like customer", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE LIKE failed: %v", err)
	}
	if create.Diagnostics.BlocksNative() || create.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("create diagnostics = %#v runtime=%#v", create.Diagnostics, create.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("CREATE TEMPORARY TABLE LIKE should not dispatch to the direct executor")
	}
	actions := create.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionCreateTemporaryTable || actions[0].Table.Name != "scratch_customer" {
		t.Fatalf("create session actions = %#v", actions)
	}
	transition := runtime.Session.PreviewSessionTransition(actions)
	if transition.Diagnostics.BlocksNative() {
		t.Fatalf("session transition diagnostics = %#v", transition.Diagnostics)
	}
	runtime.Session = transition.After

	describe, err := runtime.ExecuteSQL(context.Background(), "show columns from scratch_customer", qsbridge.ExecutionOptions{})
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
		t.Fatalf("columns = %#v, want two cloned columns", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "c_custkey" || chunk.Rows[0][3].Value != "PRI" || chunk.Rows[0][5].Value != "mapper=IntBSI" {
		t.Fatalf("first cloned column row = %#v, want primary-key IntBSI metadata", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "c_name" || chunk.Rows[1][2].Value != "YES" || chunk.Rows[1][5].Value != "mapper=StringEnum" {
		t.Fatalf("second cloned column row = %#v, want nullable StringEnum metadata", chunk.Rows[1])
	}
}

func TestTemporaryTableIntegerCoercionAcceptsBigIntCells(t *testing.T) {
	field := qsbridge.FieldDefinition{Name: "region_key", Type: qsbridge.DataTypeInt}
	cell, diagnostics := temporaryTableCoerceCell(qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: big.NewInt(7)}, field)
	if diagnostics.BlocksNative() {
		t.Fatalf("coerce diagnostics = %#v", diagnostics)
	}
	if cell.Kind != qsbridge.ValueInt || cell.Value != int64(7) {
		t.Fatalf("coerced cell = %#v, want int64 7", cell)
	}

	tooLarge := new(big.Int).Lsh(big.NewInt(1), 80)
	_, diagnostics = temporaryTableCoerceCell(qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: tooLarge}, field)
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want overflow blocker", diagnostics)
	}
}

func TestSQLRuntimeInsertSelectTemporaryTableLikeOrdersDirectSourceVectors(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas: []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "region",
			Fields: []qsbridge.FieldDefinition{
				{Name: "r_regionkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI, PrimaryKey: true, Nullable: false, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "r_name", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum, Nullable: false, Encoding: qsbridge.LegacyEncodingProfile("StringEnum", qsbridge.LegacyEncodingOptions{})},
				{Name: "r_comment", Type: qsbridge.DataTypeString, Index: qsbridge.IndexBackingString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 256})},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{
			RowSet: qsbridge.QuantaProjectedRowSet{
				Index:   "region",
				Rownums: []qsbridge.QuantaRownum{1},
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field:  qsbridge.QuantaProjectionField{Index: "region", PhysicalName: "r_name", Type: qsbridge.DataTypeString, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "AFRICA"}},
					},
					{
						Field:  qsbridge.QuantaProjectionField{Index: "region", PhysicalName: "r_comment", Type: qsbridge.DataTypeString, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "comment"}},
					},
					{
						Field:  qsbridge.QuantaProjectionField{Index: "region", PhysicalName: "r_regionkey", Type: qsbridge.DataTypeInt, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: big.NewInt(0)}},
					},
				},
			},
			Count: 1,
		}, nil
	})

	applyActions := func(result SQLExecutionResult) {
		t.Helper()
		transition := runtime.Session.PreviewSessionTransition(result.Runtime.Statement.SessionActions)
		if transition.Diagnostics.BlocksNative() {
			t.Fatalf("session transition diagnostics = %#v", transition.Diagnostics)
		}
		runtime.Session = transition.After
	}

	create, err := runtime.ExecuteSQL(context.Background(), "create temporary table scratch_region like region", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE LIKE failed: %v", err)
	}
	if !create.Supported() {
		t.Fatalf("create diagnostics = %#v runtime=%#v", create.Diagnostics, create.Runtime.Diagnostics)
	}
	applyActions(create)

	insert, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_region select r_regionkey, r_name, r_comment from region", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT SELECT temporary table LIKE failed: %v", err)
	}
	if !insert.Supported() {
		t.Fatalf("insert diagnostics = %#v runtime=%#v", insert.Diagnostics, insert.Runtime.Diagnostics)
	}
	if !executed {
		t.Fatalf("INSERT SELECT source should dispatch to the direct executor")
	}
	applyActions(insert)

	selectResult, err := runtime.ExecuteSQL(context.Background(), "select r_regionkey, r_name, r_comment from scratch_region", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT temporary table LIKE failed: %v", err)
	}
	if !selectResult.Supported() {
		t.Fatalf("select diagnostics = %#v runtime=%#v", selectResult.Diagnostics, selectResult.Runtime.Diagnostics)
	}
	chunk, diagnostics := selectResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("result chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, int64(0); got != want {
		t.Fatalf("r_regionkey = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "AFRICA"; got != want {
		t.Fatalf("r_name = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "comment"; got != want {
		t.Fatalf("r_comment = %#v, want %#v", got, want)
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

	insert, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys values (2, 'BUILDING'), (1, 'AUTOMOBILE')", qsbridge.ExecutionOptions{})
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

func TestSQLRuntimeInsertSelectTemporaryTableMaterializesRows(t *testing.T) {
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

	insert, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys select 2 as customer_key, 'BUILDING' as market_segment union all select 1 as customer_key, 'AUTOMOBILE' as market_segment", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT SELECT temporary table failed: %v", err)
	}
	if !insert.Supported() {
		t.Fatalf("insert diagnostics = %#v runtime=%#v", insert.Diagnostics, insert.Runtime.Diagnostics)
	}
	if insert.Runtime.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", insert.Runtime.Statement.AffectedRows)
	}
	if executed {
		t.Fatalf("projection-only INSERT SELECT source should not dispatch to the direct executor")
	}
	applyActions(insert)

	selectResult, err := runtime.ExecuteSQL(context.Background(), "select customer_key, market_segment from scratch_keys order by customer_key limit 2", qsbridge.ExecutionOptions{})
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
	if got, want := len(chunk.Rows), 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if got, want := chunk.Rows[0][0].Value, int64(1); got != want {
		t.Fatalf("first customer_key = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "BUILDING"; got != want {
		t.Fatalf("second market_segment = %#v, want %#v", got, want)
	}
}

func TestSQLRuntimeCreateTemporaryTableAsSelectMaterializesRows(t *testing.T) {
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

	create, err := runtime.ExecuteSQL(context.Background(), "create temporary table scratch_building_customers as select 2 as customer_key, 'BUILDING' as market_segment union all select 1 as customer_key, 'AUTOMOBILE' as market_segment", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE AS SELECT failed: %v", err)
	}
	if !create.Supported() {
		t.Fatalf("create diagnostics = %#v runtime=%#v", create.Diagnostics, create.Runtime.Diagnostics)
	}
	if create.Runtime.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", create.Runtime.Statement.AffectedRows)
	}
	actions := create.Runtime.Statement.SessionActions
	if len(actions) != 2 || actions[0].Kind != qsbridge.SessionActionCreateTemporaryTable || actions[1].Kind != qsbridge.SessionActionInsertTemporaryRows {
		t.Fatalf("session actions = %#v, want create + insert rows", actions)
	}
	if got, want := len(actions[0].Table.Fields), 2; got != want {
		t.Fatalf("temporary CTAS fields = %d, want %d", got, want)
	}
	for _, field := range actions[0].Table.Fields {
		if field.PrimaryKey {
			t.Fatalf("temporary CTAS field = %#v, projection-only CTAS should not expose a synthetic primary key", field)
		}
	}
	if executed {
		t.Fatalf("projection-only CTAS source should not dispatch to the direct executor")
	}
	applyActions(create)

	selectResult, err := runtime.ExecuteSQL(context.Background(), "select customer_key, market_segment from scratch_building_customers order by customer_key limit 2", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT CTAS temporary table failed: %v", err)
	}
	if !selectResult.Supported() {
		t.Fatalf("select diagnostics = %#v runtime=%#v", selectResult.Diagnostics, selectResult.Runtime.Diagnostics)
	}
	chunk, diagnostics := selectResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("result chunk diagnostics = %#v", diagnostics)
	}
	if got, want := len(chunk.Rows), 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if got, want := chunk.Rows[0][0].Value, int64(1); got != want {
		t.Fatalf("first customer_key = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "BUILDING"; got != want {
		t.Fatalf("second market_segment = %#v, want %#v", got, want)
	}
}

func TestSQLRuntimeSelectTemporaryTableAggregatesRows(t *testing.T) {
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

	insert, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys values (2, 'BUILDING'), (1, 'AUTOMOBILE'), (3, 'BUILDING')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT temporary table failed: %v", err)
	}
	if !insert.Supported() {
		t.Fatalf("insert diagnostics = %#v runtime=%#v", insert.Diagnostics, insert.Runtime.Diagnostics)
	}
	applyActions(insert)

	countResult, err := runtime.ExecuteSQL(context.Background(), "select count(*) as row_count, sum(customer_key) as key_sum from scratch_keys where customer_key >= 1", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT aggregate temporary table failed: %v", err)
	}
	if !countResult.Supported() {
		t.Fatalf("count diagnostics = %#v runtime=%#v", countResult.Diagnostics, countResult.Runtime.Diagnostics)
	}
	countChunk, diagnostics := countResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("count chunk diagnostics = %#v", diagnostics)
	}
	if len(countChunk.Rows) != 1 {
		t.Fatalf("count rows = %#v, want one", countChunk.Rows)
	}
	if got, want := countChunk.Rows[0][0].Value, int64(3); got != want {
		t.Fatalf("row_count = %#v, want %#v", got, want)
	}
	if got, want := countChunk.Rows[0][1].Value, float64(6); got != want {
		t.Fatalf("key_sum = %#v, want %#v", got, want)
	}

	groupResult, err := runtime.ExecuteSQL(context.Background(), "select market_segment, count(*) as segment_count from scratch_keys group by market_segment order by market_segment", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT grouped aggregate temporary table failed: %v", err)
	}
	if !groupResult.Supported() {
		t.Fatalf("group diagnostics = %#v runtime=%#v", groupResult.Diagnostics, groupResult.Runtime.Diagnostics)
	}
	groupChunk, diagnostics := groupResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("group chunk diagnostics = %#v", diagnostics)
	}
	if len(groupChunk.Rows) != 2 {
		t.Fatalf("group rows = %#v, want two", groupChunk.Rows)
	}
	if got, want := groupChunk.Rows[0][0].Value, "AUTOMOBILE"; got != want {
		t.Fatalf("first market_segment = %#v, want %#v", got, want)
	}
	if got, want := groupChunk.Rows[0][1].Value, int64(1); got != want {
		t.Fatalf("first segment_count = %#v, want %#v", got, want)
	}
	if got, want := groupChunk.Rows[1][0].Value, "BUILDING"; got != want {
		t.Fatalf("second market_segment = %#v, want %#v", got, want)
	}
	if got, want := groupChunk.Rows[1][1].Value, int64(2); got != want {
		t.Fatalf("second segment_count = %#v, want %#v", got, want)
	}
	if executed {
		t.Fatalf("temporary table aggregate reads should not dispatch to the direct executor")
	}
}
