package qsruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/version"
)

func TestSQLRuntimeWrapsExecutionContext(t *testing.T) {
	type contextKey struct{}
	const contextValue = "direct-cache"
	var wrapperCalled bool
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if got := ctx.Value(contextKey{}); got != contextValue {
			t.Fatalf("wrapped context value = %#v, want %q", got, contextValue)
		}
		return ExecutionResult{Count: 1}, nil
	})
	runtime.ContextWrapper = func(ctx context.Context) context.Context {
		wrapperCalled = true
		return context.WithValue(ctx, contextKey{}, contextValue)
	}

	result, err := runtime.ExecuteSQL(context.Background(), "select count(*) from orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if !wrapperCalled {
		t.Fatal("ContextWrapper was not called")
	}
}

func TestSQLRuntimeExecuteSQLAuthorizesBeforeExecution(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{Count: 1}, nil
	})
	runtime.Session = qsbridge.SessionContext{User: "bench", CurrentSchema: "quanta"}
	runtime.Authorizer = qsbridge.NewAccessPolicy()

	result, err := runtime.ExecuteSQL(context.Background(), "select o_orderkey from orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if executed {
		t.Fatalf("direct executor should not run when authorization denies access")
	}
	if result.Supported() {
		t.Fatalf("result should not be supported when authorization denies access")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticAccessDenied {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticAccessDenied)
	}
}

func TestSQLRuntimeExecuteSQLUnionAllAppendsBranchRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select 'orders' as source_table, 2 as row_count
		union all
		select 'lineitem' as source_table, 3 as row_count
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("constant UNION ALL should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 2 || len(chunk.Rows[1]) != 2 {
		t.Fatalf("rows = %#v, want two two-column rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "orders" || chunk.Rows[0][1].Value != int64(2) {
		t.Fatalf("first row = %#v, want orders/2", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "lineitem" || chunk.Rows[1][1].Value != int64(3) {
		t.Fatalf("second row = %#v, want lineitem/3", chunk.Rows[1])
	}
}

func TestSQLRuntimeExecuteSQLShowCreateViewReturnsCatalogRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Views: []qsbridge.SQLViewDefinition{{
			Schema: "quanta",
			Name:   "customer_names",
			SQL:    "select c_custkey, c_name from customer",
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show create view customer_names", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW CREATE VIEW should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 4 {
		t.Fatalf("rows = %#v, want one four-column row", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "customer_names" {
		t.Fatalf("view name = %#v", chunk.Rows[0][0].Value)
	}
	if got, want := chunk.Rows[0][1].Value, "CREATE VIEW quanta.customer_names AS select c_custkey, c_name from customer"; got != want {
		t.Fatalf("create view SQL = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "utf8mb4"; got != want {
		t.Fatalf("character_set_client = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowCreateTableReturnsCatalogRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
				{Name: "notes", Type: qsbridge.DataTypeString, Nullable: true},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show create table customer", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW CREATE TABLE should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want one two-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "customer"; got != want {
		t.Fatalf("table name = %#v, want %q", got, want)
	}
	wantSQL := "CREATE TABLE `customer` (\n  `c_custkey` int NOT NULL,\n  `c_name` varchar(25) DEFAULT NULL,\n  `notes` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`c_custkey`)\n)"
	if got := chunk.Rows[0][1].Value; got != wantSQL {
		t.Fatalf("create table SQL = %#v, want %q", got, wantSQL)
	}
}

func TestSQLRuntimeExecuteSQLShowCreateTableIncludesForeignKeys(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_since", Type: qsbridge.DataTypeTime, PrimaryKey: true},
				},
			},
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []qsbridge.FieldDefinition{
					{Name: "o_orderkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "o_custkey", Type: qsbridge.DataTypeInt, Nullable: true},
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:      "orders_customer",
					FromTable: "orders",
					FromField: "o_custkey",
					ToTable:   "customer",
					Direction: qsbridge.JoinChildToParent,
				}},
			},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show create table orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW CREATE TABLE should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	createSQL, _ := chunk.Rows[0][1].Value.(string)
	want := "CONSTRAINT `orders_customer` FOREIGN KEY (`o_custkey`) REFERENCES `customer` (`c_custkey`)"
	if !strings.Contains(createSQL, want) {
		t.Fatalf("create table SQL = %q, want foreign key clause %q", createSQL, want)
	}
}

func TestSQLRuntimeExecuteSQLShowCreateDatabaseReturnsCatalogRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show create database quanta", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW CREATE DATABASE should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want one two-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "quanta"; got != want {
		t.Fatalf("database = %#v, want %q", got, want)
	}
	if got := fmt.Sprint(chunk.Rows[0][1].Value); got != "CREATE DATABASE `quanta` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */" {
		t.Fatalf("create database SQL = %#v", got)
	}
}

func TestSQLRuntimeExecuteSQLUseReturnsSessionAction(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "use quanta", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("USE should not dispatch to the direct executor")
	}
	if got, want := result.Runtime.Statement.Status, "Database changed"; got != want {
		t.Fatalf("status = %#v, want %q", got, want)
	}
	actions := result.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionUseSchema || actions[0].Value != "quanta" {
		t.Fatalf("session actions = %#v, want use_schema quanta", actions)
	}

	missing, err := runtime.ExecuteSQL(context.Background(), "use missing_schema", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL missing schema failed: %v", err)
	}
	if !missing.Diagnostics.BlocksNative() {
		t.Fatalf("missing schema diagnostics = %#v, want blocking", missing.Diagnostics)
	}
}

func TestSQLRuntimeExecuteSQLSetNamesAndAutocommitReturnSessionActions(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	names, err := runtime.ExecuteSQL(context.Background(), "set names utf8mb4 collate utf8mb4_0900_ai_ci", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET NAMES failed: %v", err)
	}
	if names.Diagnostics.BlocksNative() || names.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("names diagnostics = %#v runtime=%#v", names.Diagnostics, names.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SET NAMES should not dispatch to the direct executor")
	}
	actions := names.Runtime.Statement.SessionActions
	if len(actions) != 4 || actions[0].Name != "character_set_client" || actions[0].Value != "utf8mb4" || actions[3].Name != "collation_connection" {
		t.Fatalf("SET NAMES actions = %#v", actions)
	}

	characterSet, err := runtime.ExecuteSQL(context.Background(), "set character set utf8mb4", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET CHARACTER SET failed: %v", err)
	}
	actions = characterSet.Runtime.Statement.SessionActions
	if len(actions) != 3 || actions[0].Name != "character_set_client" || actions[0].Value != "utf8mb4" {
		t.Fatalf("SET CHARACTER SET actions = %#v", actions)
	}

	charset, err := runtime.ExecuteSQL(context.Background(), "set charset 'utf8mb4'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET CHARSET failed: %v", err)
	}
	actions = charset.Runtime.Statement.SessionActions
	if len(actions) != 3 || actions[0].Name != "character_set_client" || actions[0].Value != "utf8mb4" {
		t.Fatalf("SET CHARSET actions = %#v", actions)
	}

	characterSetDefault, err := runtime.ExecuteSQL(context.Background(), "set character set default", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET CHARACTER SET DEFAULT failed: %v", err)
	}
	actions = characterSetDefault.Runtime.Statement.SessionActions
	if len(actions) != 3 || actions[0].Name != "character_set_client" || actions[0].Value != "default" {
		t.Fatalf("SET CHARACTER SET DEFAULT actions = %#v", actions)
	}

	characterSetResultsNull, err := runtime.ExecuteSQL(context.Background(), "set character_set_results = NULL", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET character_set_results NULL failed: %v", err)
	}
	actions = characterSetResultsNull.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetVariable || actions[0].Name != "character_set_results" || actions[0].Value != "NULL" {
		t.Fatalf("SET character_set_results NULL actions = %#v", actions)
	}

	autocommit, err := runtime.ExecuteSQL(context.Background(), "set autocommit = 1", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET autocommit failed: %v", err)
	}
	actions = autocommit.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetVariable || actions[0].Name != "autocommit" || actions[0].Value != "1" {
		t.Fatalf("SET autocommit actions = %#v", actions)
	}

	autocommitOn, err := runtime.ExecuteSQL(context.Background(), "set autocommit = ON", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET autocommit ON failed: %v", err)
	}
	actions = autocommitOn.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetVariable || actions[0].Name != "autocommit" || actions[0].Value != "ON" {
		t.Fatalf("SET autocommit ON actions = %#v", actions)
	}

	sqlMode, err := runtime.ExecuteSQL(context.Background(), "set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET sql_mode failed: %v", err)
	}
	actions = sqlMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Fatalf("SET sql_mode actions = %#v", actions)
	}

	doubleQuotedSQLMode, err := runtime.ExecuteSQL(context.Background(), `set sql_mode = "STRICT_TRANS_TABLES,NO_ZERO_DATE"`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET double-quoted sql_mode failed: %v", err)
	}
	actions = doubleQuotedSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Fatalf("SET double-quoted sql_mode actions = %#v", actions)
	}

	unquotedCommaSQLMode, err := runtime.ExecuteSQL(context.Background(), "set sql_mode = STRICT_TRANS_TABLES,NO_ZERO_DATE", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET unquoted comma sql_mode failed: %v", err)
	}
	actions = unquotedCommaSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Fatalf("SET unquoted comma sql_mode actions = %#v", actions)
	}

	functionSQLMode, err := runtime.ExecuteSQL(context.Background(), "set sql_mode = replace(@@sql_mode, 'ONLY_FULL_GROUP_BY', '')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET function sql_mode failed: %v", err)
	}
	actions = functionSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "replace(@@sql_mode, 'ONLY_FULL_GROUP_BY', '')" {
		t.Fatalf("SET function sql_mode actions = %#v", actions)
	}

	colonEqualsSQLMode, err := runtime.ExecuteSQL(context.Background(), "set @saved_sql_mode := @@sql_mode", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET := session variable failed: %v", err)
	}
	actions = colonEqualsSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetVariable || actions[0].Name != "@saved_sql_mode" || actions[0].Value != "@@sql_mode" {
		t.Fatalf("SET := session variable actions = %#v", actions)
	}

	timeZone, err := runtime.ExecuteSQL(context.Background(), "set time_zone = '+00:00'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET time_zone failed: %v", err)
	}
	actions = timeZone.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetTimeZone || actions[0].Name != "time_zone" || actions[0].Value != "+00:00" {
		t.Fatalf("SET time_zone actions = %#v", actions)
	}

	scopedSQLMode, err := runtime.ExecuteSQL(context.Background(), "set session sql_mode = 'ANSI_QUOTES'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET SESSION sql_mode failed: %v", err)
	}
	actions = scopedSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "ANSI_QUOTES" {
		t.Fatalf("SET SESSION sql_mode actions = %#v", actions)
	}

	sessionVarSQLMode, err := runtime.ExecuteSQL(context.Background(), "set @@session.sql_mode = 'NO_ENGINE_SUBSTITUTION'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET @@session.sql_mode failed: %v", err)
	}
	actions = sessionVarSQLMode.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionSetSQLMode || actions[0].Name != "sql_mode" || actions[0].Value != "NO_ENGINE_SUBSTITUTION" {
		t.Fatalf("SET @@session.sql_mode actions = %#v", actions)
	}

	mixedScoped, err := runtime.ExecuteSQL(context.Background(), "set character_set_results = NULL, session sql_mode = 'ANSI_QUOTES', session sql_select_limit = DEFAULT", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET mixed scoped assignments failed: %v", err)
	}
	actions = mixedScoped.Runtime.Statement.SessionActions
	if len(actions) != 3 ||
		actions[0].Name != "character_set_results" || actions[0].Value != "NULL" ||
		actions[1].Kind != qsbridge.SessionActionSetSQLMode || actions[1].Name != "sql_mode" || actions[1].Value != "ANSI_QUOTES" ||
		actions[2].Name != "sql_select_limit" || actions[2].Value != "DEFAULT" {
		t.Fatalf("SET mixed scoped assignment actions = %#v", actions)
	}

	transaction, err := runtime.ExecuteSQL(context.Background(), "set session transaction isolation level repeatable read", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SET SESSION TRANSACTION failed: %v", err)
	}
	if transaction.Diagnostics.BlocksNative() || transaction.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("transaction diagnostics = %#v runtime=%#v", transaction.Diagnostics, transaction.Runtime.Diagnostics)
	}
	if len(transaction.Runtime.Statement.SessionActions) != 0 {
		t.Fatalf("transaction actions = %#v, want metadata-only statement", transaction.Runtime.Statement.SessionActions)
	}
}

func TestSQLRuntimeExecuteSQLShowDatabasesReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}, {Name: "analytics"}},
		Tables:    []qsbridge.TableDefinition{{Schema: "archive", Name: "events"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show databases", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW DATABASES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if got, want := result.Runtime.RowSet.ProjectionVectors[0].Field.Field, "Database"; got != want {
		t.Fatalf("column name = %q, want %q", got, want)
	}
	if len(chunk.Rows) != 3 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want three one-column rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "analytics" || chunk.Rows[1][0].Value != "archive" || chunk.Rows[2][0].Value != "quanta" {
		t.Fatalf("database rows = %#v, want sorted analytics/archive/quanta", chunk.Rows)
	}
}

func TestSQLRuntimeExecuteSQLShowTablesReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "orders"},
			{Schema: "quanta", Name: "customer"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show tables", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW TABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want two one-column rows", chunk.Rows)
	}
	if got, want := result.Runtime.RowSet.ProjectionVectors[0].Field.Field, "Tables_in_quanta"; got != want {
		t.Fatalf("column name = %q, want %q", got, want)
	}
	if got, want := chunk.Rows[0][0].Value, "customer"; got != want {
		t.Fatalf("first table = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][0].Value, "orders"; got != want {
		t.Fatalf("second table = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowFullTablesReturnsViewsAndTypes(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "orders"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{Schema: "quanta", Name: "order_summary", SQL: "select o_orderkey from orders"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show full tables", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FULL TABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want two two-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "order_summary"; got != want {
		t.Fatalf("first object = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "VIEW"; got != want {
		t.Fatalf("first object type = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "BASE TABLE"; got != want {
		t.Fatalf("second object type = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowFullTablesLikeFiltersTableNames(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "orders"},
			{Schema: "quanta", Name: "superstore_orders"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{Schema: "quanta", Name: "order_summary", SQL: "select o_orderkey from orders"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show full tables from `quanta` like 'superstore_orders'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FULL TABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want one two-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "superstore_orders"; got != want {
		t.Fatalf("object = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "BASE TABLE"; got != want {
		t.Fatalf("object type = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowFullColumnsFromSchemaLike(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "superstore_orders",
			Fields: []qsbridge.FieldDefinition{
				{Name: "row_id", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "order_id", Type: qsbridge.DataTypeString},
				{Name: "region", Type: qsbridge.DataTypeString},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show full columns from `superstore_orders` from `quanta` like 'order_%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FULL COLUMNS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "order_id"; got != want {
		t.Fatalf("field = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowOpenTablesReturnsZeroUseCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "orders"},
			{Schema: "quanta", Name: "customer"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{Schema: "quanta", Name: "order_summary", SQL: "select o_orderkey from orders"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show open tables from quanta like 'ord%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW OPEN TABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 4 {
		t.Fatalf("rows = %#v, want two four-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "quanta"; got != want {
		t.Fatalf("database = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "order_summary"; got != want {
		t.Fatalf("first object = %#v, want %q", got, want)
	}
	if chunk.Rows[0][2].Value != int64(0) || chunk.Rows[0][3].Value != int64(0) {
		t.Fatalf("open table counters = %#v, want zero use/locked", chunk.Rows[0])
	}
	if got, want := chunk.Rows[1][1].Value, "orders"; got != want {
		t.Fatalf("second object = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowVariablesLikeReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{
		SQLModes:  []qsbridge.SQLMode{"STRICT_TRANS_TABLES", "NO_ZERO_DATE"},
		TimeZone:  "+00:00",
		Variables: map[string]string{"autocommit": "0"},
	}

	result, err := runtime.ExecuteSQL(context.Background(), "show variables like 'version%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW VARIABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 4 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want four two-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "version"; got != want {
		t.Fatalf("first variable = %#v, want %q", got, want)
	}

	autocommit, err := runtime.ExecuteSQL(context.Background(), "show variables like 'autocommit'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW VARIABLES autocommit failed: %v", err)
	}
	autocommitChunk, diagnostics := autocommit.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("autocommit chunk diagnostics = %#v", diagnostics)
	}
	if len(autocommitChunk.Rows) != 1 || autocommitChunk.Rows[0][0].Value != "autocommit" || autocommitChunk.Rows[0][1].Value != "OFF" {
		t.Fatalf("autocommit rows = %#v", autocommitChunk.Rows)
	}

	timeZone, err := runtime.ExecuteSQL(context.Background(), "show variables like 'time_zone'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW VARIABLES time_zone failed: %v", err)
	}
	timeZoneChunk, diagnostics := timeZone.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("time zone chunk diagnostics = %#v", diagnostics)
	}
	if len(timeZoneChunk.Rows) != 1 || timeZoneChunk.Rows[0][0].Value != "time_zone" || timeZoneChunk.Rows[0][1].Value != "+00:00" {
		t.Fatalf("time zone rows = %#v", timeZoneChunk.Rows)
	}

	versionResult, err := runtime.ExecuteSQL(context.Background(), "show variables where Variable_name = 'version'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW VARIABLES WHERE version failed: %v", err)
	}
	versionChunk, diagnostics := versionResult.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("version chunk diagnostics = %#v", diagnostics)
	}
	if len(versionChunk.Rows) != 1 || versionChunk.Rows[0][0].Value != "version" || versionChunk.Rows[0][1].Value != version.MySQLVersion() {
		t.Fatalf("version rows = %#v", versionChunk.Rows)
	}

	versionPair, err := runtime.ExecuteSQL(context.Background(), "show variables where Variable_name in ('version', 'version_comment')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW VARIABLES WHERE IN failed: %v", err)
	}
	versionPairChunk, diagnostics := versionPair.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("version pair chunk diagnostics = %#v", diagnostics)
	}
	if len(versionPairChunk.Rows) != 2 || versionPairChunk.Rows[0][0].Value != "version" || versionPairChunk.Rows[1][0].Value != "version_comment" {
		t.Fatalf("version pair rows = %#v", versionPairChunk.Rows)
	}

	charsetVariables, err := runtime.ExecuteSQL(context.Background(), "show variables where Variable_name like 'character_set_%' or Variable_name = 'collation_connection'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW VARIABLES WHERE OR failed: %v", err)
	}
	charsetVariablesChunk, diagnostics := charsetVariables.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("charset variables chunk diagnostics = %#v", diagnostics)
	}
	if len(charsetVariablesChunk.Rows) != 5 || charsetVariablesChunk.Rows[0][0].Value != "character_set_client" || charsetVariablesChunk.Rows[4][0].Value != "collation_connection" {
		t.Fatalf("charset variables rows = %#v", charsetVariablesChunk.Rows)
	}

	globalVersion, err := runtime.ExecuteSQL(context.Background(), "show global variables like 'version_compile_%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW GLOBAL VARIABLES failed: %v", err)
	}
	globalVersionChunk, diagnostics := globalVersion.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("global version chunk diagnostics = %#v", diagnostics)
	}
	if len(globalVersionChunk.Rows) != 2 || globalVersionChunk.Rows[0][0].Value != "version_compile_machine" || globalVersionChunk.Rows[1][0].Value != "version_compile_os" {
		t.Fatalf("global version rows = %#v", globalVersionChunk.Rows)
	}
}

func TestSQLRuntimeExecuteSQLShowStatusLikeReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show status like 'Threads_connected'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW STATUS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want one two-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "Threads_connected"; got != want {
		t.Fatalf("status variable = %#v, want %q", got, want)
	}

	where, err := runtime.ExecuteSQL(context.Background(), "show status where Variable_name = 'Threads_connected'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW STATUS WHERE failed: %v", err)
	}
	whereChunk, diagnostics := where.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("where chunk diagnostics = %#v", diagnostics)
	}
	if len(whereChunk.Rows) != 1 || whereChunk.Rows[0][0].Value != "Threads_connected" {
		t.Fatalf("where status rows = %#v", whereChunk.Rows)
	}

	statusPair, err := runtime.ExecuteSQL(context.Background(), "show status where Variable_name in ('Connections', 'Threads_connected')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW STATUS WHERE IN failed: %v", err)
	}
	statusPairChunk, diagnostics := statusPair.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("status pair chunk diagnostics = %#v", diagnostics)
	}
	if len(statusPairChunk.Rows) != 2 || statusPairChunk.Rows[0][0].Value != "Connections" || statusPairChunk.Rows[1][0].Value != "Threads_connected" {
		t.Fatalf("status pair rows = %#v", statusPairChunk.Rows)
	}

	statusOr, err := runtime.ExecuteSQL(context.Background(), "show status where Variable_name = 'Connections' or Variable_name like 'Threads_%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW STATUS WHERE OR failed: %v", err)
	}
	statusOrChunk, diagnostics := statusOr.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("status or chunk diagnostics = %#v", diagnostics)
	}
	if len(statusOrChunk.Rows) != 2 || statusOrChunk.Rows[0][0].Value != "Connections" || statusOrChunk.Rows[1][0].Value != "Threads_connected" {
		t.Fatalf("status or rows = %#v", statusOrChunk.Rows)
	}

	sessionStatus, err := runtime.ExecuteSQL(context.Background(), "show session status like 'Threads_connected'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW SESSION STATUS failed: %v", err)
	}
	sessionStatusChunk, diagnostics := sessionStatus.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("session status chunk diagnostics = %#v", diagnostics)
	}
	if len(sessionStatusChunk.Rows) != 1 || sessionStatusChunk.Rows[0][0].Value != "Threads_connected" {
		t.Fatalf("session status rows = %#v", sessionStatusChunk.Rows)
	}
}

func TestSQLRuntimeExecuteSQLShowProcesslistReturnsCurrentSessionRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{User: "moli", CurrentSchema: "quanta"}

	result, err := runtime.ExecuteSQL(context.Background(), "show full processlist", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW PROCESSLIST should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 8 {
		t.Fatalf("rows = %#v, want one eight-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[1].Value != "moli" || row[3].Value != "quanta" || row[4].Value != "Query" || row[7].Value != "show full processlist" {
		t.Fatalf("processlist row = %#v, want current session metadata", row)
	}
	if result.Prepared.Kind != qsbridge.QueryKindShowProcesslist || !result.Prepared.Query.Catalog.Full {
		t.Fatalf("prepared query = %#v, want full processlist", result.Prepared.Query)
	}
}

func TestSQLRuntimeExecuteSQLShowEnginesReturnsQuantaStreamEngineRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show engines", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW ENGINES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 6 {
		t.Fatalf("rows = %#v, want one six-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "QUANTASTREAM" || row[1].Value != "DEFAULT" || row[3].Value != "NO" {
		t.Fatalf("engine row = %#v, want QUANTASTREAM default engine metadata", row)
	}
}

func TestSQLRuntimeExecuteSQLShowTableTypesReturnsQuantaStreamEngineRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show table types", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW TABLE TYPES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 6 {
		t.Fatalf("rows = %#v, want one six-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "QUANTASTREAM" || row[1].Value != "DEFAULT" {
		t.Fatalf("table type row = %#v, want QUANTASTREAM default metadata", row)
	}
}

func TestSQLRuntimeExecuteSQLShowPluginsReturnsQuantaStreamPluginRow(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show plugins", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW PLUGINS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 5 {
		t.Fatalf("rows = %#v, want one five-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "QUANTASTREAM" || row[1].Value != "ACTIVE" || row[2].Value != "STORAGE ENGINE" {
		t.Fatalf("plugin row = %#v, want QUANTASTREAM active storage engine metadata", row)
	}
}

func TestSQLRuntimeExecuteSQLShowFunctionStatusReturnsCatalogFunctionRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: []qsbridge.FunctionDefinition{
			{Name: "lower", Kind: qsbridge.FunctionScalar, Origin: qsbridge.FunctionOriginMySQLCompatible, Placement: qsbridge.FunctionPlacementExpression, ReturnType: qsbridge.DataTypeString, Native: true, Deterministic: true},
			{Name: "upper", Kind: qsbridge.FunctionScalar, Origin: qsbridge.FunctionOriginMySQLCompatible, Placement: qsbridge.FunctionPlacementExpression, ReturnType: qsbridge.DataTypeString, Native: true, Deterministic: true},
		},
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{User: "moli", CurrentSchema: "quanta"}

	result, err := runtime.ExecuteSQL(context.Background(), "show function status like 'lo%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FUNCTION STATUS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 11 {
		t.Fatalf("rows = %#v, want one eleven-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "quanta" || row[1].Value != "lower" || row[2].Value != "FUNCTION" || row[3].Value != "moli@%" {
		t.Fatalf("function status row = %#v, want lower function metadata", row)
	}
	if row[7].Value != "kind=scalar origin=mysql_compatible placement=expression native=true deterministic=true" {
		t.Fatalf("function comment = %#v", row[7].Value)
	}
}

func TestSQLRuntimeExecuteSQLShowEmptyRoutineTriggerEventStatus(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		columns int
		kind    qsbridge.QueryKind
	}{
		{name: "procedure status", sql: "show procedure status", columns: 11, kind: qsbridge.QueryKindShowProcedureStatus},
		{name: "triggers", sql: "show triggers from quanta", columns: 11, kind: qsbridge.QueryKindShowTriggers},
		{name: "events", sql: "show events from quanta", columns: 15, kind: qsbridge.QueryKindShowEvents},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executed := false
			runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
				Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
			}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
				executed = true
				return ExecutionResult{}, nil
			})

			result, err := runtime.ExecuteSQL(context.Background(), tc.sql, qsbridge.ExecutionOptions{})
			if err != nil {
				t.Fatalf("ExecuteSQL failed: %v", err)
			}
			if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
			}
			if executed {
				t.Fatalf("%s should not dispatch to the direct executor", tc.sql)
			}
			chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
			if diagnostics.BlocksNative() {
				t.Fatalf("chunk diagnostics = %#v", diagnostics)
			}
			if len(chunk.Rows) != 0 || len(result.Runtime.RowSet.ProjectionVectors) != tc.columns {
				t.Fatalf("rows/vectors = %#v/%d, want empty %d-column metadata", chunk.Rows, len(result.Runtime.RowSet.ProjectionVectors), tc.columns)
			}
			if result.Prepared.Kind != tc.kind {
				t.Fatalf("prepared kind = %q, want %q", result.Prepared.Kind, tc.kind)
			}
		})
	}
}

func TestSQLRuntimeExecuteSQLShowPrivilegesReturnsStaticPrivilegeRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show privileges", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW PRIVILEGES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) < 3 || len(chunk.Rows[0]) != 3 {
		t.Fatalf("rows = %#v, want privilege rows with three columns", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "Alter"; got != want {
		t.Fatalf("first privilege = %#v, want %q", got, want)
	}
	if result.Prepared.Kind != qsbridge.QueryKindShowPrivileges {
		t.Fatalf("prepared kind = %q, want show_privileges", result.Prepared.Kind)
	}
}

func TestSQLRuntimeExecuteSQLShowGrantsReturnsCurrentSessionGrants(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{User: "moli", CurrentSchema: "quanta"}

	result, err := runtime.ExecuteSQL(context.Background(), "show grants", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW GRANTS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want two one-column grant rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "GRANT USAGE ON *.* TO 'moli'@'%'"; got != want {
		t.Fatalf("usage grant = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][0].Value, "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, SHOW VIEW ON `quanta`.* TO 'moli'@'%'"; got != want {
		t.Fatalf("schema grant = %#v, want %q", got, want)
	}
	if result.Prepared.Kind != qsbridge.QueryKindShowGrants {
		t.Fatalf("prepared kind = %q, want show_grants", result.Prepared.Kind)
	}
}

func TestSQLRuntimeExecuteSQLProjectionMetadataFunctions(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{User: "bench@localhost", CurrentSchema: "analytics"}

	result, err := runtime.ExecuteSQL(context.Background(), `
		select database() as db_name,
		       schema() as schema_name,
		       version() as version_value,
		       user() as user_value,
		       current_user() as current_user_value,
		       connection_id() as connection_id_value
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("projection-only metadata functions should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 6 {
		t.Fatalf("rows = %#v, want one six-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "analytics" || row[1].Value != "analytics" || row[2].Value != version.MySQLVersion() {
		t.Fatalf("schema/version cells = %#v", row[:3])
	}
	if row[3].Value != "bench@localhost" || row[4].Value != "bench@localhost" || row[5].Value != int64(1) {
		t.Fatalf("user/connection cells = %#v", row[3:])
	}
}

func TestSQLRuntimeExecuteSQLProjectionOnlyWhere(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select 1 as matched where 3 > 2 and 2 <= 2", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("projection-only WHERE should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want one one-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, any(int64(1)); got != want {
		t.Fatalf("matched = %#v, want %#v", got, want)
	}
}

func TestSQLRuntimeExecuteSQLProjectionOnlyWhereMiss(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select 1 as matched where 3 < 2", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("projection-only WHERE miss should not dispatch to the direct executor")
	}
	if result.Runtime.Count != 0 || result.Runtime.RowSet.CandidateCount() != 0 {
		t.Fatalf("runtime result count=%d candidate_count=%d, want empty", result.Runtime.Count, result.Runtime.RowSet.CandidateCount())
	}
}

func TestSQLRuntimeExecuteSQLProjectionSystemVariables(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{
		SQLModes:  []qsbridge.SQLMode{"STRICT_TRANS_TABLES", "NO_ZERO_DATE"},
		TimeZone:  "+00:00",
		Variables: map[string]string{"autocommit": "0"},
	}

	result, err := runtime.ExecuteSQL(context.Background(), `
		select @@version as version_value,
		       @@version_comment as version_comment,
		       @@version_compile_os as version_compile_os,
		       @@autocommit as autocommit_value,
		       @@sql_mode as sql_mode_value,
		       @@time_zone as time_zone_value
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("projection-only system variables should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 6 {
		t.Fatalf("rows = %#v, want one six-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != version.MySQLVersion() || row[1].Value != version.MySQLVersionComment() || row[2].Value != "Linux" || row[3].Value != int64(0) || row[4].Value != "STRICT_TRANS_TABLES,NO_ZERO_DATE" || row[5].Value != "+00:00" {
		t.Fatalf("system variable cells = %#v", row)
	}
}

func TestSQLRuntimeExecuteSQLConnectorJStartupVariables(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})
	runtime.Session = qsbridge.SessionContext{TimeZone: "+00:00"}

	result, err := runtime.ExecuteSQL(context.Background(), `
		/* mysql-connector-j-8.4.0 (Revision: test) */SELECT @@session.auto_increment_increment AS auto_increment_increment,
		       @@character_set_client AS character_set_client,
		       @@character_set_connection AS character_set_connection,
		       @@character_set_results AS character_set_results,
		       @@character_set_server AS character_set_server,
		       @@collation_server AS collation_server,
		       @@collation_connection AS collation_connection,
		       @@init_connect AS init_connect,
		       @@interactive_timeout AS interactive_timeout,
		       @@license AS license,
		       @@lower_case_table_names AS lower_case_table_names,
		       @@max_allowed_packet AS max_allowed_packet,
		       @@net_write_timeout AS net_write_timeout,
		       @@performance_schema AS performance_schema,
		       @@query_cache_size AS query_cache_size,
		       @@query_cache_type AS query_cache_type,
		       @@sql_mode AS sql_mode,
		       @@system_time_zone AS system_time_zone,
		       @@time_zone AS time_zone,
		       @@tx_isolation AS transaction_isolation,
		       @@wait_timeout AS wait_timeout
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("Connector/J startup variables should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 21 {
		t.Fatalf("rows = %#v, want one twenty-one-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != int64(1) || row[10].Value != int64(0) || row[11].Value != int64(67108864) || row[17].Value != "UTC" || row[18].Value != "+00:00" || row[19].Value != "READ-COMMITTED" || row[20].Value != int64(28800) {
		t.Fatalf("Connector/J metadata row = %#v", row)
	}
}

func TestSQLRuntimeExecuteSQLConnectorJReadOnlyVariables(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select @@session.tx_read_only as session_tx_read_only,
		       @@tx_read_only as tx_read_only,
		       @@transaction_read_only as transaction_read_only
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("Connector/J read-only variables should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 3 {
		t.Fatalf("rows = %#v, want one three-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != int64(0) || row[1].Value != int64(0) || row[2].Value != int64(0) {
		t.Fatalf("read-only variable cells = %#v, want all zero", row)
	}
}

func TestSQLRuntimeExecuteSQLProjectionAutocommitNormalizesBooleanSessionValues(t *testing.T) {
	tests := []struct {
		value string
		want  int64
	}{
		{value: "ON", want: 1},
		{value: "OFF", want: 0},
		{value: "true", want: 1},
		{value: "false", want: 0},
	}

	for _, tt := range tests {
		runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
			Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
		}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			t.Fatalf("@@autocommit projection should not dispatch to the direct executor")
			return ExecutionResult{}, nil
		})
		runtime.Session = qsbridge.SessionContext{
			Variables: map[string]string{"autocommit": tt.value},
		}

		result, err := runtime.ExecuteSQL(context.Background(), "select @@autocommit as autocommit_value", qsbridge.ExecutionOptions{})
		if err != nil {
			t.Fatalf("%s ExecuteSQL failed: %v", tt.value, err)
		}
		if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
			t.Fatalf("%s diagnostics = %#v runtime=%#v", tt.value, result.Diagnostics, result.Runtime.Diagnostics)
		}
		chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
		if diagnostics.BlocksNative() {
			t.Fatalf("%s chunk diagnostics = %#v", tt.value, diagnostics)
		}
		if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != tt.want {
			t.Fatalf("%s rows = %#v, want autocommit %d", tt.value, chunk.Rows, tt.want)
		}
	}
}

func TestSQLRuntimeExecuteSQLShowWarningsCharsetAndCollation(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	warnings, err := runtime.ExecuteSQL(context.Background(), "show warnings", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW WARNINGS failed: %v", err)
	}
	if warnings.Diagnostics.BlocksNative() || warnings.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("warnings diagnostics = %#v runtime=%#v", warnings.Diagnostics, warnings.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW WARNINGS should not dispatch to the direct executor")
	}
	if got, want := len(warnings.Runtime.RowSet.ProjectionVectors), 3; got != want {
		t.Fatalf("warning columns = %d, want %d", got, want)
	}
	if got, want := warnings.Runtime.Count, uint64(0); got != want {
		t.Fatalf("warning count = %d, want %d", got, want)
	}

	limitedWarnings, err := runtime.ExecuteSQL(context.Background(), "show warnings limit 10", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW WARNINGS LIMIT failed: %v", err)
	}
	if limitedWarnings.Diagnostics.BlocksNative() || limitedWarnings.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("limited warnings diagnostics = %#v runtime=%#v", limitedWarnings.Diagnostics, limitedWarnings.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW WARNINGS LIMIT should not dispatch to the direct executor")
	}
	if got, want := len(limitedWarnings.Runtime.RowSet.ProjectionVectors), 3; got != want {
		t.Fatalf("limited warning columns = %d, want %d", got, want)
	}
	if got, want := limitedWarnings.Runtime.Count, uint64(0); got != want {
		t.Fatalf("limited warning count = %d, want %d", got, want)
	}

	errors, err := runtime.ExecuteSQL(context.Background(), "show errors limit 10", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW ERRORS LIMIT failed: %v", err)
	}
	if errors.Diagnostics.BlocksNative() || errors.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("errors diagnostics = %#v runtime=%#v", errors.Diagnostics, errors.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW ERRORS LIMIT should not dispatch to the direct executor")
	}
	if got, want := len(errors.Runtime.RowSet.ProjectionVectors), 3; got != want {
		t.Fatalf("error columns = %d, want %d", got, want)
	}
	if got, want := errors.Runtime.Count, uint64(0); got != want {
		t.Fatalf("error count = %d, want %d", got, want)
	}

	warningCount, err := runtime.ExecuteSQL(context.Background(), "show count(*) warnings", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COUNT(*) WARNINGS failed: %v", err)
	}
	warningCountChunk, diagnostics := warningCount.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("warning count chunk diagnostics = %#v", diagnostics)
	}
	if len(warningCountChunk.Rows) != 1 || len(warningCountChunk.Rows[0]) != 1 || warningCountChunk.Rows[0][0].Value != int64(0) {
		t.Fatalf("warning count rows = %#v", warningCountChunk.Rows)
	}

	errorCount, err := runtime.ExecuteSQL(context.Background(), "show count(*) errors", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COUNT(*) ERRORS failed: %v", err)
	}
	errorCountChunk, diagnostics := errorCount.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("error count chunk diagnostics = %#v", diagnostics)
	}
	if len(errorCountChunk.Rows) != 1 || len(errorCountChunk.Rows[0]) != 1 || errorCountChunk.Rows[0][0].Value != int64(0) {
		t.Fatalf("error count rows = %#v", errorCountChunk.Rows)
	}

	charset, err := runtime.ExecuteSQL(context.Background(), "show character set like 'utf8mb4'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW CHARACTER SET failed: %v", err)
	}
	charsetChunk, diagnostics := charset.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("charset chunk diagnostics = %#v", diagnostics)
	}
	if len(charsetChunk.Rows) != 1 || len(charsetChunk.Rows[0]) != 4 {
		t.Fatalf("charset rows = %#v, want one four-column row", charsetChunk.Rows)
	}
	if got, want := charsetChunk.Rows[0][0].Value, "utf8mb4"; got != want {
		t.Fatalf("charset = %#v, want %q", got, want)
	}

	collation, err := runtime.ExecuteSQL(context.Background(), "show collation like 'utf8mb4_0900%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COLLATION failed: %v", err)
	}
	collationChunk, diagnostics := collation.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("collation chunk diagnostics = %#v", diagnostics)
	}
	if len(collationChunk.Rows) != 1 || len(collationChunk.Rows[0]) != 6 {
		t.Fatalf("collation rows = %#v, want one six-column row", collationChunk.Rows)
	}
	if got, want := collationChunk.Rows[0][0].Value, "utf8mb4_0900_ai_ci"; got != want {
		t.Fatalf("collation = %#v, want %q", got, want)
	}

	collationByIn, err := runtime.ExecuteSQL(context.Background(), "show collation where Charset in ('utf8mb4')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SHOW COLLATION WHERE IN failed: %v", err)
	}
	collationByInChunk, diagnostics := collationByIn.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("collation by in chunk diagnostics = %#v", diagnostics)
	}
	if len(collationByInChunk.Rows) != 2 {
		t.Fatalf("collation by in rows = %#v, want both utf8mb4 collations", collationByInChunk.Rows)
	}
}

func TestSQLRuntimeExecuteSQLExplainReturnsMetadataShape(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "explain select count(*) from lineitem", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("EXPLAIN should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 12 {
		t.Fatalf("rows = %#v, want one twelve-column explain row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != int64(1) || row[1].Value != "SIMPLE" || row[2].Value != "lineitem" || row[4].Value != "QUANTASTREAM" {
		t.Fatalf("explain row = %#v", row)
	}
}

func TestSQLRuntimeExecuteSQLExplainAnnotatesSelectShape(t *testing.T) {
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("EXPLAIN should not execute explained SQL")
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "explain select c_custkey from customer where c_mktsegment = 'BUILDING' order by c_custkey limit 2", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	row := chunk.Rows[0]
	if row[2].Value != "customer" {
		t.Fatalf("table = %#v, want customer", row[2])
	}
	extra, _ := row[11].Value.(string)
	for _, want := range []string{"filtered", "ordered", "limited"} {
		if !strings.Contains(extra, want) {
			t.Fatalf("extra = %q, want %q annotation", extra, want)
		}
	}
}

func TestSQLRuntimeExecuteSQLExplainFormatJSONReturnsObjectPayload(t *testing.T) {
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("EXPLAIN FORMAT=JSON should not execute explained SQL")
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "explain format=json select dx_call from spots where band = '20m' limit 5", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want one JSON explain cell", chunk.Rows)
	}
	payload, _ := chunk.Rows[0][0].Value.(string)
	if !strings.Contains(payload, `"query_block"`) || !strings.Contains(payload, `"table_name":"spots"`) || !strings.Contains(payload, "QuantaStream native plan") {
		t.Fatalf("explain JSON payload = %s", payload)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaTablesReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "customer"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{Schema: "quanta", Name: "customer_projection", SQL: "select c_custkey from customer"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select table_name, table_type, engine, table_collation, table_comment from information_schema.tables where table_schema = 'quanta' order by table_name", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.TABLES should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 5 {
		t.Fatalf("rows = %#v, want two rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "customer"; got != want {
		t.Fatalf("first table = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "BASE TABLE"; got != want {
		t.Fatalf("first table type = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "QUANTASTREAM"; got != want {
		t.Fatalf("first table engine = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][3].Value, "utf8mb4_0900_ai_ci"; got != want {
		t.Fatalf("first table collation = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][4].Value, "BASE TABLE"; got != want {
		t.Fatalf("first table comment = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "VIEW"; got != want {
		t.Fatalf("second table type = %#v, want %q", got, want)
	}
	if got := chunk.Rows[1][2].Value; got != nil {
		t.Fatalf("view engine = %#v, want NULL", got)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaViewsReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "customer"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{
				Schema:       "quanta",
				Name:         "customer_projection",
				SQL:          "create view customer_projection as select c_custkey from customer",
				CanonicalSQL: "select c_custkey from customer",
			},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select table_name, view_definition, check_option, is_updatable, definer
		from information_schema.views
		where table_schema = 'quanta'
		order by table_name
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.VIEWS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 5 {
		t.Fatalf("rows = %#v, want one five-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "customer_projection" || row[1].Value != "select c_custkey from customer" {
		t.Fatalf("view row = %#v, want stored view definition", row)
	}
	if row[2].Value != "NONE" || row[3].Value != "NO" || row[4].Value != "qstream@%" {
		t.Fatalf("view metadata = %#v, want MySQL-compatible defaults", row)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaSchemataReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas: []qsbridge.CatalogSchemaDefinition{
			{Name: "analytics"},
			{Name: "quanta"},
		},
		Tables: []qsbridge.TableDefinition{
			{Schema: "archive", Name: "events"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select schema_name, default_character_set_name, default_collation_name
		from information_schema.schemata
		where schema_name = 'quanta'
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.SCHEMATA should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 3 {
		t.Fatalf("rows = %#v, want one three-column row", chunk.Rows)
	}
	row := chunk.Rows[0]
	if row[0].Value != "quanta" || row[1].Value != "utf8mb4" || row[2].Value != "utf8mb4_0900_ai_ci" {
		t.Fatalf("schema row = %#v", row)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaSchemataSupportsWorkbenchObjectProjection(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		SELECT 'schema' AS 'OBJECT_TYPE',
		       CATALOG_NAME as 'CATALOG',
		       SCHEMA_NAME as 'SCHEMA',
		       SCHEMA_NAME as 'NAME'
		FROM information_schema.schemata
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.SCHEMATA should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 4 {
		t.Fatalf("rows = %#v, want one four-column row", chunk.Rows)
	}
	if got, want := result.Runtime.RowSet.ProjectionVectors[0].Field.Field, "OBJECT_TYPE"; got != want {
		t.Fatalf("literal projection column = %q, want %q", got, want)
	}
	if got, want := result.Request.ResultColumns[0].Name, "OBJECT_TYPE"; got != want {
		t.Fatalf("client projection column = %q, want %q", got, want)
	}
	row := chunk.Rows[0]
	if row[0].Value != "schema" || row[1].Value != "def" || row[2].Value != "quanta" || row[3].Value != "quanta" {
		t.Fatalf("schema object row = %#v", row)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaCharacterSetsAndCollationsReturnRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	charsets, err := runtime.ExecuteSQL(context.Background(), `
		select character_set_name, default_collate_name, maxlen
		from information_schema.character_sets
		where character_set_name = 'utf8mb4'
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INFORMATION_SCHEMA.CHARACTER_SETS failed: %v", err)
	}
	if charsets.Diagnostics.BlocksNative() || charsets.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("charset diagnostics = %#v runtime=%#v", charsets.Diagnostics, charsets.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.CHARACTER_SETS should not dispatch to the direct executor")
	}
	charsetChunk, diagnostics := charsets.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("charset chunk diagnostics = %#v", diagnostics)
	}
	if len(charsetChunk.Rows) != 1 || len(charsetChunk.Rows[0]) != 3 {
		t.Fatalf("charset rows = %#v, want one three-column row", charsetChunk.Rows)
	}
	if charsetChunk.Rows[0][0].Value != "utf8mb4" || charsetChunk.Rows[0][1].Value != "utf8mb4_0900_ai_ci" || charsetChunk.Rows[0][2].Value != int64(4) {
		t.Fatalf("charset row = %#v", charsetChunk.Rows[0])
	}

	collations, err := runtime.ExecuteSQL(context.Background(), `
		select collation_name, is_default, pad_attribute
		from information_schema.collations
		where character_set_name in ('utf8mb4')
		order by collation_name
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INFORMATION_SCHEMA.COLLATIONS failed: %v", err)
	}
	if collations.Diagnostics.BlocksNative() || collations.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("collation diagnostics = %#v runtime=%#v", collations.Diagnostics, collations.Runtime.Diagnostics)
	}
	collationChunk, diagnostics := collations.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("collation chunk diagnostics = %#v", diagnostics)
	}
	if len(collationChunk.Rows) != 2 || len(collationChunk.Rows[0]) != 3 {
		t.Fatalf("collation rows = %#v, want two three-column rows", collationChunk.Rows)
	}
	if collationChunk.Rows[0][0].Value != "utf8mb4_0900_ai_ci" || collationChunk.Rows[0][1].Value != "Yes" || collationChunk.Rows[0][2].Value != "NO PAD" {
		t.Fatalf("first collation row = %#v", collationChunk.Rows[0])
	}
	if collationChunk.Rows[1][0].Value != "utf8mb4_bin" || collationChunk.Rows[1][2].Value != "PAD SPACE" {
		t.Fatalf("second collation row = %#v", collationChunk.Rows[1])
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaColumnsReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select column_name,
		       column_type,
		       character_maximum_length,
		       character_octet_length,
		       numeric_precision,
		       numeric_scale,
		       character_set_name,
		       collation_name,
		       column_key,
		       extra,
		       privileges,
		       column_comment
		from information_schema.columns
		where table_schema = 'quanta' and table_name = 'customer'
		order by ordinal_position
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.COLUMNS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 12 {
		t.Fatalf("rows = %#v, want two twelve-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "c_custkey"; got != want {
		t.Fatalf("first column = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "int"; got != want {
		t.Fatalf("first column type = %#v, want %q", got, want)
	}
	if got := chunk.Rows[0][2].Value; got != nil {
		t.Fatalf("first column character length = %#v, want NULL", got)
	}
	if got, want := chunk.Rows[0][4].Value, int64(10); got != want {
		t.Fatalf("first column numeric precision = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][5].Value, int64(0); got != want {
		t.Fatalf("first column numeric scale = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][8].Value, "PRI"; got != want {
		t.Fatalf("first column key = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][1].Value, "varchar(25)"; got != want {
		t.Fatalf("second column type = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][2].Value, int64(25); got != want {
		t.Fatalf("second column character length = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][3].Value, int64(100); got != want {
		t.Fatalf("second column octet length = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[1][6].Value, "utf8mb4"; got != want {
		t.Fatalf("second column character set = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][7].Value, "utf8mb4_0900_ai_ci"; got != want {
		t.Fatalf("second column collation = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][9].Value, "mapper=StringLexBSI"; got != want {
		t.Fatalf("second column extra = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][10].Value, "select,insert,update,references"; got != want {
		t.Fatalf("second column privileges = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][11].Value, ""; got != want {
		t.Fatalf("second column comment = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaStatisticsReturnsIndexRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select index_name, seq_in_index, column_name, index_comment from information_schema.statistics where table_schema = 'quanta' and table_name = 'customer' order by index_name, seq_in_index", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("INFORMATION_SCHEMA.STATISTICS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 4 {
		t.Fatalf("rows = %#v, want two four-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "PRIMARY"; got != want {
		t.Fatalf("first index = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, int64(1); got != want {
		t.Fatalf("primary seq = %#v, want %#v", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "c_custkey"; got != want {
		t.Fatalf("primary column = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][0].Value, "qs_c_name"; got != want {
		t.Fatalf("mapper index = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][3].Value, "max_length=25"; got != want {
		t.Fatalf("mapper index comment = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLInformationSchemaRelationshipsExposeForeignKeys(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_since", Type: qsbridge.DataTypeTime, PrimaryKey: true},
				},
			},
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []qsbridge.FieldDefinition{
					{Name: "o_orderkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "o_custkey", Type: qsbridge.DataTypeInt},
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:        "orders_customer",
					FromTable:   "orders",
					FromField:   "o_custkey",
					ToTable:     "customer",
					Direction:   qsbridge.JoinChildToParent,
					Cardinality: "many_to_one",
				}},
			},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	columns := executeInformationSchemaRows(t, runtime, `
		select column_name, column_key
		from information_schema.columns
		where table_schema = 'quanta' and table_name = 'orders'
		order by ordinal_position
	`)
	if len(columns) != 2 || columns[0][1].Value != "PRI" || columns[1][1].Value != "MUL" {
		t.Fatalf("columns = %#v, want primary and foreign-key markers", columns)
	}

	tableConstraints := executeInformationSchemaRows(t, runtime, `
		select constraint_name, constraint_type, table_name
		from information_schema.table_constraints
		where table_schema = 'quanta' and table_name = 'orders'
		order by constraint_name
	`)
	if len(tableConstraints) != 2 {
		t.Fatalf("table constraints = %#v, want primary and foreign key", tableConstraints)
	}
	constraintTypes := map[string]any{}
	for _, row := range tableConstraints {
		constraintTypes[fmt.Sprint(row[0].Value)] = row[1].Value
	}
	if constraintTypes["PRIMARY"] != "PRIMARY KEY" {
		t.Fatalf("table constraints = %#v, want PRIMARY KEY row", tableConstraints)
	}
	if constraintTypes["orders_customer"] != "FOREIGN KEY" {
		t.Fatalf("table constraints = %#v, want orders_customer FOREIGN KEY row", tableConstraints)
	}

	keyUsage := executeInformationSchemaRows(t, runtime, `
		select constraint_name, column_name, referenced_table_name, referenced_column_name
		from information_schema.key_column_usage
		where table_schema = 'quanta' and table_name = 'orders' and referenced_table_name is not null
		order by constraint_name
	`)
	if len(keyUsage) != 1 {
		t.Fatalf("key usage = %#v, want foreign key row", keyUsage)
	}
	var foreignKeyUsage qsbridge.ResultRow
	for _, row := range keyUsage {
		if row[0].Value == "orders_customer" {
			foreignKeyUsage = row
			break
		}
	}
	if len(foreignKeyUsage) == 0 || foreignKeyUsage[1].Value != "o_custkey" || foreignKeyUsage[2].Value != "customer" || foreignKeyUsage[3].Value != "c_custkey" {
		t.Fatalf("key usage = %#v, want referenced customer key", keyUsage)
	}

	referential := executeInformationSchemaRows(t, runtime, `
		select constraint_name, table_name, referenced_table_name, update_rule, delete_rule
		from information_schema.referential_constraints
		where constraint_schema = 'quanta' and table_name = 'orders'
		order by constraint_name
	`)
	if len(referential) != 1 || referential[0][0].Value != "orders_customer" || referential[0][1].Value != "orders" || referential[0][2].Value != "customer" {
		t.Fatalf("referential constraints = %#v, want orders_customer relationship", referential)
	}
	if referential[0][3].Value != "RESTRICT" || referential[0][4].Value != "RESTRICT" {
		t.Fatalf("referential rules = %#v, want RESTRICT/RESTRICT", referential[0])
	}

	if executed {
		t.Fatalf("INFORMATION_SCHEMA relationship metadata should not dispatch to the direct executor")
	}
}

func executeInformationSchemaRows(t *testing.T, runtime SQLRuntime, sql string) []qsbridge.ResultRow {
	t.Helper()
	result, err := runtime.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed for %q: %v", sql, err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics for %q = %#v runtime=%#v", sql, result.Diagnostics, result.Runtime.Diagnostics)
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics for %q = %#v", sql, diagnostics)
	}
	return chunk.Rows
}

func TestSQLRuntimeExecuteSQLShowIndexReturnsPKAndMapperRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
				{Name: "notes", Type: qsbridge.DataTypeString, Nullable: true},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show index from customer", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW INDEX should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if got, want := len(result.Runtime.RowSet.ProjectionVectors), 15; got != want {
		t.Fatalf("projection columns = %d, want %d", got, want)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 15 {
		t.Fatalf("rows = %#v, want two fifteen-column rows", chunk.Rows)
	}
	pk := chunk.Rows[0]
	if pk[0].Value != "customer" || pk[1].Value != int64(0) || pk[2].Value != "PRIMARY" || pk[3].Value != int64(1) || pk[4].Value != "c_custkey" {
		t.Fatalf("primary row = %#v", pk)
	}
	if pk[9].Value != "" {
		t.Fatalf("primary row nullability = %#v, want empty", pk[9])
	}
	if pk[10].Value != "BTREE" || pk[11].Value != "mapper=IntBSI" || pk[12].Value != "primary_key=true" {
		t.Fatalf("primary row mapper metadata = %#v", pk)
	}
	mapped := chunk.Rows[1]
	if mapped[1].Value != int64(1) || mapped[2].Value != "qs_c_name" || mapped[4].Value != "c_name" {
		t.Fatalf("mapper row = %#v", mapped)
	}
	if mapped[9].Value != "YES" || mapped[10].Value != "BTREE" || mapped[11].Value != "mapper=StringLexBSI" || mapped[12].Value != "max_length=25" {
		t.Fatalf("mapper row metadata = %#v", mapped)
	}
	if mapped[14].Kind != qsbridge.ValueNull {
		t.Fatalf("expression = %#v, want NULL", mapped[14])
	}
}

func TestSQLRuntimeExecuteSQLShowFullColumnsReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
				{Name: "notes", Type: qsbridge.DataTypeString, Nullable: true},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show full columns from customer", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FULL COLUMNS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 3 || len(chunk.Rows[0]) != 9 {
		t.Fatalf("rows = %#v, want three nine-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "c_custkey"; got != want {
		t.Fatalf("first field = %#v, want %q", got, want)
	}
	if chunk.Rows[0][2].Kind != qsbridge.ValueNull {
		t.Fatalf("int collation = %#v, want SQL NULL", chunk.Rows[0][2])
	}
	if got, want := chunk.Rows[0][3].Value, "NO"; got != want {
		t.Fatalf("primary nullability = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][4].Value, "PRI"; got != want {
		t.Fatalf("first key = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][2].Value, "utf8mb4_0900_ai_ci"; got != want {
		t.Fatalf("string collation = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][7].Value, "select,insert,update,references"; got != want {
		t.Fatalf("privileges = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[2][1].Value, "varchar(255)"; got != want {
		t.Fatalf("unbounded string type = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLShowFullColumnsMarksForeignKeyColumns(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "orders",
			Fields: []qsbridge.FieldDefinition{
				{Name: "o_orderkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "o_custkey", Type: qsbridge.DataTypeInt, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("ParentRelation", qsbridge.LegacyEncodingOptions{})},
			},
			Relationships: []qsbridge.RelationshipDefinition{{
				Name:      "orders_customer",
				FromTable: "orders",
				FromField: "o_custkey",
				ToTable:   "customer",
				ToField:   "c_custkey",
				Direction: qsbridge.JoinChildToParent,
			}},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show full columns from orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW FULL COLUMNS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[1]) != 9 {
		t.Fatalf("rows = %#v, want two nine-column rows", chunk.Rows)
	}
	foreignKeyColumn := chunk.Rows[1]
	if foreignKeyColumn[0].Value != "o_custkey" || foreignKeyColumn[4].Value != "MUL" {
		t.Fatalf("foreign key column = %#v, want o_custkey marked MUL", foreignKeyColumn)
	}
}

func TestSQLRuntimeExecuteSQLShowTableStatusReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{Schema: "quanta", Name: "customer"},
		},
		Views: []qsbridge.SQLViewDefinition{
			{Schema: "quanta", Name: "customer_projection", SQL: "select c_custkey from customer"},
		},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "show table status like 'customer%'", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("SHOW TABLE STATUS should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 18 {
		t.Fatalf("rows = %#v, want two eighteen-column rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "customer" || chunk.Rows[0][1].Value != "QUANTASTREAM" || chunk.Rows[0][17].Value != "BASE TABLE" {
		t.Fatalf("base table status row = %#v", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "customer_projection" || chunk.Rows[1][1].Kind != qsbridge.ValueNull || chunk.Rows[1][17].Value != "VIEW" {
		t.Fatalf("view status row = %#v", chunk.Rows[1])
	}
}

func TestSQLRuntimeExecuteSQLDescribeReturnsCatalogRows(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true, Encoding: qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{})},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true, Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25})},
			},
		}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "describe customer", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("DESCRIBE should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 6 {
		t.Fatalf("rows = %#v, want two six-column rows", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "c_custkey"; got != want {
		t.Fatalf("first field = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "int"; got != want {
		t.Fatalf("first type = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "NO"; got != want {
		t.Fatalf("first nullability = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][3].Value, "PRI"; got != want {
		t.Fatalf("first key = %#v, want %q", got, want)
	}
	if chunk.Rows[0][4].Kind != qsbridge.ValueNull {
		t.Fatalf("first default = %#v, want SQL NULL", chunk.Rows[0][4])
	}
	if got, want := chunk.Rows[0][5].Value, "mapper=IntBSI"; got != want {
		t.Fatalf("first extra = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][2].Value, "YES"; got != want {
		t.Fatalf("second nullability = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[1][5].Value, "mapper=StringLexBSI"; got != want {
		t.Fatalf("second extra = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeReturnsExecutionInstrumentationSnapshot(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		recorder := ExecutionInstrumentationFromContext(ctx)
		if recorder == nil {
			t.Fatal("execution instrumentation was not installed")
		}
		recorder.ObserveCount("test_executor", "rows", 7, "fake executor")
		return ExecutionResult{
			Count: 7,
			Probes: []ExecutionProbe{
				{Section: "test_executor", Name: "rows", Value: "7", Detail: "fake executor"},
				{Section: "test_executor", Name: "phase_probe_elapsed", Value: "5ms", Detail: "returned probe"},
			},
		}, nil
	})
	runtime.ContextWrapper = WithQueryScratchpad

	result, err := runtime.ExecuteSQL(context.Background(), "select count(*) from orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if result.Instrumentation.Empty() {
		t.Fatal("instrumentation snapshot was empty")
	}
	assertExecutionCounter(t, result.Instrumentation, "test_executor", "rows", 7)
	if len(result.Instrumentation.Counters) != 1 {
		t.Fatalf("instrumentation counters = %#v, want duplicate returned counter suppressed", result.Instrumentation.Counters)
	}
	assertExecutionTimingName(t, result.Instrumentation, "sql_runtime", "phase_prepare_elapsed")
	assertExecutionTimingName(t, result.Instrumentation, "sql_runtime", "phase_select_lower_elapsed")
	assertExecutionTimingName(t, result.Instrumentation, "sql_runtime", "phase_execute_prepared_elapsed")
	assertExecutionTimingName(t, result.Instrumentation, "sql_runtime", "phase_total_elapsed")
	assertExecutionTimingName(t, result.Instrumentation, "test_executor", "phase_probe_elapsed")
}

func TestCorrelatedAverageQuantityTypedMatchBuildsDescriptorAndFilters(t *testing.T) {
	sql := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	runtime := newTestSQLRuntime(t)
	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		plan := runtime.Plan(sql)
		t.Fatalf("correlated average typed intent not found: subqueries=%d diagnostics=%#v", len(plan.Query.Subqueries), plan.Diagnostics)
	}
	descriptor := match.Descriptor
	if descriptor.OuterLineitem != "l" || descriptor.InnerLineitem != "l2" || descriptor.OuterPart != "p" || descriptor.Factor != 0.2 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.AggregateFunction != "avg" || descriptor.OuterQuantity.qualifiedName() != "l.l_quantity" || descriptor.InnerQuantity.qualifiedName() != "l2.l_quantity" {
		t.Fatalf("descriptor aggregate refs = %#v", descriptor)
	}
	if descriptor.InnerQuantity.Table != "lineitem" || descriptor.OuterQuantity.Table != "lineitem" || descriptor.OuterKey.Table != "part" {
		t.Fatalf("descriptor typed fields = %#v", descriptor)
	}
	if descriptor.InnerKey.qualifiedName() != "l2.l_partkey" || descriptor.OuterKey.qualifiedName() != "p.p_partkey" {
		t.Fatalf("descriptor correlated keys = %#v", descriptor)
	}
	if got := correlatedQualifiedNames(descriptor.RequiredFilters); len(got) != 2 || got[0] != "p.p_brand" || got[1] != "p.p_container" {
		t.Fatalf("descriptor required filters = %#v", got)
	}
	brand, container, ok := match.requiredPartFilters()
	if !ok || brand != "Brand#45" || container != "MED JAR" {
		t.Fatalf("filters = %q/%q ok=%v", brand, container, ok)
	}
}

func TestCorrelatedAverageNativePredicateBuildsThresholds(t *testing.T) {
	runtime := newTestSQLRuntime(t)
	sql := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select avg(l2.l_quantity) * 0.2
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		t.Fatalf("typed correlated aggregate match not found")
	}
	predicate := correlatedAverageNativePredicate(match.Descriptor, []q17PartThreshold{
		{PartKey: 101, Threshold: 10},
		{PartKey: 202, Threshold: 20.5},
	})

	if predicate.KeyField.Name != "p_partkey" || predicate.ValueField.Name != "l_quantity" || predicate.Operator != qsbridge.BinaryOpLess {
		t.Fatalf("native predicate = %#v", predicate)
	}
	if len(predicate.Thresholds) != 2 || predicate.Thresholds[0].Key != 101 || predicate.Thresholds[0].Threshold != 10 || predicate.Thresholds[1].Key != 202 || predicate.Thresholds[1].Threshold != 20.5 {
		t.Fatalf("thresholds = %#v", predicate.Thresholds)
	}
}

func TestCorrelatedAverageNativePredicateExpressionReportSummarizesRuntimeShape(t *testing.T) {
	runtime := newTestSQLRuntime(t)
	sql := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select avg(l2.l_quantity) * 0.2
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		t.Fatalf("typed correlated aggregate match not found")
	}
	report := correlatedAverageNativePredicateExpressionReport(correlatedAverageNativePredicate(match.Descriptor, []q17PartThreshold{
		{PartKey: 101, Threshold: 10},
		{PartKey: 202, Threshold: 20.5},
	}))

	if report.Kind != qsbridge.ExprKind("native_correlated_aggregate_predicate") || report.Operator != qsbridge.BinaryOpLess || report.BranchCount != 2 || report.LiteralCount != 4 {
		t.Fatalf("report = %#v", report)
	}
	if got, want := report.FieldNames, []string{"l.l_quantity", "p.p_partkey"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
}

func TestCorrelatedAverageNativePredicateHandlesEmptyThresholds(t *testing.T) {
	predicate := correlatedAverageNativePredicate(correlatedAverageQuantityDescriptor{}, nil)

	if len(predicate.Thresholds) != 0 || predicate.Operator != qsbridge.BinaryOpLess {
		t.Fatalf("predicate = %#v, want empty threshold native predicate", predicate)
	}
}

func TestScalarSubqueryResultCellReadsUnnamedProjectionVector(t *testing.T) {
	cell, diagnostics := scalarSubqueryResultCell(qsbridge.QuantaProjectedRowSet{
		Rownums: []qsbridge.QuantaRownum{1},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: 95025.42544399995}},
		}},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if cell.Value != 95025.42544399995 {
		t.Fatalf("cell = %#v", cell)
	}
}

func TestSQLRuntimeApplyPreflightRewritesRunsOrderedBoundary(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	query := `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`
	result, err := runtime.applyPreflightRewrites(context.Background(), query, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.SQL != query {
		t.Fatalf("preflight SQL = %q, want original scalar SQL", result.SQL)
	}
	if got, want := len(result.Optimization.Rewrites), 0; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, result.Optimization.Rewrites)
	}
	if result.Preflight.Total != 0 || result.Preflight.Applied != 0 || result.Preflight.Skipped != 0 || len(result.Preflight.Rewrites) != 0 {
		t.Fatalf("preflight summary = %#v, want no active SQL rewrite work", result.Preflight)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesScalarSubqueryInBoundQuery(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Preflight.Total != 0 {
		t.Fatalf("preflight summary = %#v, want no SQL rewrite preflight work", result.Preflight)
	}
	if got := result.Preflight.HelperExecutionReports(); len(got) != 0 {
		t.Fatalf("helper reports = %#v, want none for typed scalar materialization", got)
	}
	if got, want := len(result.Request.Bound.Prepared.Query.Having), 1; got != want {
		t.Fatalf("having predicates = %d, want %d", got, want)
	}
	binary, ok := result.Request.Bound.Prepared.Query.Having[0].Expr.(qsbridge.BinaryExpr)
	if !ok {
		t.Fatalf("having expr = %T, want binary", result.Request.Bound.Prepared.Query.Having[0].Expr)
	}
	literal, ok := binary.Right.(qsbridge.LiteralExpr)
	if !ok || literal.Kind != qsbridge.ValueInt || scalarSubqueryTestIntLiteralValue(literal) != 7 {
		t.Fatalf("having right = %#v, want materialized int literal 7", binary.Right)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesScalarSubqueryInUpdateAssignment(t *testing.T) {
	var updateRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.Mutation.Kind == qsbridge.MutationUpdate {
			updateRequest = request
			return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: 1}}, nil
		}
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "1-URGENT"}},
			}},
		}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `update orders
set o_orderpriority = (
  select o_orderpriority
  from orders
  where o_orderkey = 1
)
where o_orderkey = 7`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if got := updateRequest.Mutation.Kind; got != qsbridge.MutationUpdate {
		t.Fatalf("mutation kind = %q, want update", got)
	}
	if got, want := len(updateRequest.Mutation.Assignments), 1; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	literal, ok := updateRequest.Mutation.Assignments[0].Value.(qsbridge.LiteralExpr)
	if !ok || literal.Kind != qsbridge.ValueString || literal.Value != "1-URGENT" {
		t.Fatalf("assignment value = %#v, want materialized string literal", updateRequest.Mutation.Assignments[0].Value)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesInListSubqueryInMutationPredicate(t *testing.T) {
	var updateRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.Mutation.Kind == qsbridge.MutationUpdate {
			updateRequest = request
			return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: 2}}, nil
		}
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1, 2},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(7)},
					{Kind: qsbridge.ValueInt, Value: int64(8)},
				},
			}},
		}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `update orders
set o_orderpriority = '2-HIGH'
where o_orderkey in (
  select o_orderkey
  from orders
  where o_orderkey <= 8
)`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if got := updateRequest.Mutation.Kind; got != qsbridge.MutationUpdate {
		t.Fatalf("mutation kind = %q, want update", got)
	}
	if got, want := len(updateRequest.Mutation.Predicates), 1; got != want {
		t.Fatalf("predicates = %d, want %d", got, want)
	}
	binary, ok := updateRequest.Mutation.Predicates[0].Expr.(qsbridge.BinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want BinaryExpr", updateRequest.Mutation.Predicates[0].Expr)
	}
	list, ok := binary.Right.(qsbridge.ListExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want ListExpr", binary.Right)
	}
	if got, want := len(list.Items), 2; got != want {
		t.Fatalf("list items = %d, want %d", got, want)
	}
	first, ok := list.Items[0].(qsbridge.LiteralExpr)
	if !ok || first.Kind != qsbridge.ValueInt || first.Value != int64(7) {
		t.Fatalf("first list item = %#v, want int literal 7", list.Items[0])
	}
	second, ok := list.Items[1].(qsbridge.LiteralExpr)
	if !ok || second.Kind != qsbridge.ValueInt || second.Value != int64(8) {
		t.Fatalf("second list item = %#v, want int literal 8", list.Items[1])
	}
}

func TestSQLRuntimeExecuteSQLAppliesCorrelatedAggregateNativePredicate(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})
	runtime.NativeSubquerySteps = q17TypedPathNativeStepExecutor{}
	query := `select count(*) as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select avg(l2.l_quantity) * 0.2
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	result, err := runtime.ExecuteSQL(context.Background(), query, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.Runtime.Count != 3 {
		t.Fatalf("runtime count = %d, want fake executor result count 3", result.Runtime.Count)
	}
	if result.Preflight.Total != 0 {
		t.Fatalf("preflight summary = %#v, want no SQL rewrite preflight work", result.Preflight)
	}
	if result.NativeSubqueries.CorrelatedAggregates != 1 {
		t.Fatalf("native subquery summary = %#v, want one correlated aggregate", result.NativeSubqueries)
	}
	if result.Prepared.SQL != query {
		t.Fatalf("prepared SQL = %q, want original SQL", result.Prepared.SQL)
	}
	if result.Prepared.Inspection.Diagnostics.BlocksNative() {
		t.Fatalf("inspection diagnostics = %#v, want none after correlated aggregate replacement", result.Prepared.Inspection.Diagnostics)
	}
	if qsbridge.PhysicalPlanDiagnostics(result.Prepared.Physical.Root).BlocksNative() {
		t.Fatalf("physical diagnostics = %#v, want none after correlated aggregate replacement", qsbridge.PhysicalPlanDiagnostics(result.Prepared.Physical.Root))
	}
	if got := len(result.Request.Bound.Prepared.Query.Subqueries); got != 0 {
		t.Fatalf("subqueries = %d, want consumed correlated aggregate intent: %#v", got, result.Request.Bound.Prepared.Query.Subqueries)
	}
	predicates := result.Request.Bound.Prepared.Query.Predicates
	if got, want := len(predicates), 2; got != want {
		t.Fatalf("predicates = %d, want %d parent filters without residual replacement: %#v", got, want, predicates)
	}
	if got, want := len(gotRequest.NativePredicates.CorrelatedAggregate), 1; got != want {
		t.Fatalf("runtime native correlated aggregate predicates = %d, want %d", got, want)
	}
	if got, want := len(result.NativeSubqueries.HelperExecutionReports()), 2; got != want {
		t.Fatalf("native subquery helper reports = %d, want %d", got, want)
	}
	native := gotRequest.NativePredicates.CorrelatedAggregate[0]
	if native.KeyField.Name != "p_partkey" || native.ValueField.Name != "l_quantity" || native.Operator != qsbridge.BinaryOpLess {
		t.Fatalf("runtime native predicate = %#v", native)
	}
	if len(native.Thresholds) != 1 || native.Thresholds[0].Key != 101 || native.Thresholds[0].Threshold != 10 {
		t.Fatalf("runtime native thresholds = %#v", native.Thresholds)
	}
}

func TestSQLRuntimeNativeCorrelatedAggregateTraceIsPlannerVisible(t *testing.T) {
	trace := qsbridge.NewOptimizationTrace()
	trace = mergeRuntimeOptimizationTrace(trace, correlatedAverageRewriteTrace())

	if !trace.Supported {
		t.Fatalf("trace supported = false, want true")
	}
	if got, want := len(trace.Rewrites), 1; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, trace.Rewrites)
	}
	if trace.Rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregateNativePredicate || trace.Rewrites[0].Status != qsbridge.RewriteApplied {
		t.Fatalf("rewrite = %#v, want correlated aggregate native predicate applied", trace.Rewrites[0])
	}

	runtime := newTestSQLRuntime(t)
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{
		SQL:          "select o_orderkey from orders where o_orderkey >= 1",
		Optimization: trace,
	}, qsbridge.ExecutionOptions{})

	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics = %#v, want none", request.Diagnostics)
	}
	rewrites := prepared.Inspection.Optimization.Rewrites
	if got, want := len(rewrites), 1; got != want {
		t.Fatalf("prepared rewrites = %d, want %d: %#v", got, want, rewrites)
	}
	if rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregateNativePredicate {
		t.Fatalf("prepared rewrite rules = %#v", rewrites)
	}
}

func TestSQLRuntimeBuilderRequiresParser(t *testing.T) {
	runtime, diagnostics, err := SQLRuntimeBuilder{}.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if runtime.Environment.Ready() {
		t.Fatalf("runtime = %#v, want not ready", runtime)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}

func TestSQLRuntimePlansThroughEnvironmentCatalog(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.Plan("select o_orderkey from orders")

	if result.Diagnostics.BlocksNative() {
		t.Fatalf("plan diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.DefaultSchema != "quanta" {
		t.Fatalf("default schema = %q, want quanta", result.DefaultSchema)
	}
	if result.CatalogVersion != "test-catalog-v1" {
		t.Fatalf("catalog version = %q, want test-catalog-v1", result.CatalogVersion)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "orders" {
		t.Fatalf("sources = %#v, want orders", result.Query.Sources)
	}
}

func TestSQLRuntimeExecutesPreparedNeutralRequest(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result, err := runtime.ExecutePrepared(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))

	if err != nil {
		t.Fatalf("execute prepared: %v", err)
	}
	if result.Count != 11 {
		t.Fatalf("count = %d, want 11", result.Count)
	}
}

func TestSQLRuntimeExecuteSQLPlansLowersAndRunsSimpleSelect(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSelect {
		t.Fatalf("prepared kind = %s, want SELECT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one: %#v", len(result.Intermediate.Fragments), result.Intermediate.Fragments)
	}
	fragment := result.Intermediate.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpGE {
		t.Fatalf("fragment = %#v, want orders.o_orderkey >= 100", fragment)
	}
	if gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("runtime request fragments/projections = %d/%d, want 1/1", gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	if result.Runtime.Count != 3 {
		t.Fatalf("runtime count = %d, want 3", result.Runtime.Count)
	}
}

func TestSQLRuntimeExecuteSQLRunsConstantScalarProjection(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select lower('SEATTLE') as city_lower, coalesce(null, 'fallback') as value, substring('alphabet', 4, 2) as part", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("constant scalar projection should not dispatch to the direct executor")
	}
	if len(result.Intermediate.Fragments) != 0 {
		t.Fatalf("fragments = %d, want no bitmap lowering for constant scalar projection", len(result.Intermediate.Fragments))
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 3 {
		t.Fatalf("rows = %#v, want one three-column row", chunk.Rows)
	}
	if got, want := chunk.Rows[0][0].Value, "seattle"; got != want {
		t.Fatalf("city_lower = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][1].Value, "fallback"; got != want {
		t.Fatalf("coalesce value = %#v, want %q", got, want)
	}
	if got, want := chunk.Rows[0][2].Value, "ha"; got != want {
		t.Fatalf("substring part = %#v, want %q", got, want)
	}
}

func TestSQLRuntimeExecuteSQLRunsNowConstantScalarProjection(t *testing.T) {
	executed := false
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})

	before := time.Now().UTC()
	result, err := runtime.ExecuteSQL(context.Background(), "select now() as current_time", qsbridge.ExecutionOptions{})
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if executed {
		t.Fatalf("constant now() projection should not dispatch to the direct executor")
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want one one-column row", chunk.Rows)
	}
	cell := chunk.Rows[0][0]
	if cell.Kind != qsbridge.ValueTime {
		t.Fatalf("now() cell = %#v, want time", cell)
	}
	value, ok := cell.Value.(time.Time)
	if !ok {
		t.Fatalf("now() value = %#v, want time.Time", cell.Value)
	}
	if value.Before(before) || value.After(after) {
		t.Fatalf("now() value = %v, want between %v and %v", value, before, after)
	}
}

func TestSQLRuntimeExecuteSQLRunsMySQLCompatibleConstantScalarFunctions(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("constant scalar projection should not dispatch to the direct executor")
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `select concat('quanta', 'stream') as product,
		trim('  core  ') as trimmed,
		left('stream', 3) as left_value,
		right('stream', 3) as right_value,
		substring('alphabet', -3) as tail,
		ifnull(null, 'fallback') as fallback,
		nullif('same', 'same') as null_value,
		abs(-42) as magnitude,
		round(12345, -2) as rounded,
		month('1995-03-15') as ship_month,
		day('1995-03-15') as ship_day`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one row", chunk.Rows)
	}
	row := chunk.Rows[0]
	want := []qsbridge.ResultCell{
		{Kind: qsbridge.ValueString, Value: "quantastream"},
		{Kind: qsbridge.ValueString, Value: "core"},
		{Kind: qsbridge.ValueString, Value: "str"},
		{Kind: qsbridge.ValueString, Value: "eam"},
		{Kind: qsbridge.ValueString, Value: "bet"},
		{Kind: qsbridge.ValueString, Value: "fallback"},
		{Kind: qsbridge.ValueNull, Value: nil},
		{Kind: qsbridge.ValueFloat, Value: float64(42)},
		{Kind: qsbridge.ValueFloat, Value: float64(12300)},
		{Kind: qsbridge.ValueInt, Value: int64(3)},
		{Kind: qsbridge.ValueInt, Value: int64(15)},
	}
	if len(row) != len(want) {
		t.Fatalf("row width = %d, want %d: %#v", len(row), len(want), row)
	}
	for i := range want {
		if row[i].Kind != want[i].Kind || fmt.Sprint(row[i].Value) != fmt.Sprint(want[i].Value) {
			t.Fatalf("cell %d = %#v, want %#v", i, row[i], want[i])
		}
	}
}

func TestSQLRuntimeExecuteSQLExpandsWildcardSelect(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select * from orders", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if len(result.Prepared.ResultColumns) != 3 {
		t.Fatalf("result columns = %d, want fixture order fields", len(result.Prepared.ResultColumns))
	}
	if gotRequest.ProjectionCount() != 3 {
		t.Fatalf("runtime projection count = %d, want 3", gotRequest.ProjectionCount())
	}
	if gotRequest.ProjectionOrder[0].Name != "o_orderkey" || gotRequest.ProjectionOrder[2].Name != "o_orderpriority" {
		t.Fatalf("projection order = %#v, want catalog order", gotRequest.ProjectionOrder)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesJoinProjectionExpressionInputs(t *testing.T) {
	var gotRequest ExecutionRequest
	catalog := qsbridge.MemoryCatalog{
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_mktsegment", Type: qsbridge.DataTypeString},
				},
			},
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []qsbridge.FieldDefinition{
					{Name: "o_orderkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "o_custkey", Type: qsbridge.DataTypeInt},
					{Name: "o_orderpriority", Type: qsbridge.DataTypeString},
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:      "orders_customer",
					FromTable: "orders",
					FromField: "o_custkey",
					ToTable:   "customer",
					ToField:   "c_custkey",
					Direction: qsbridge.JoinChildToParent,
					Encoding: qsbridge.RelationshipEncodingProfile{
						Kind: qsbridge.RelationshipEncodingVector,
					},
				}},
			},
		},
	}
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `select c.c_custkey,
       o.o_orderkey,
       case when o.o_orderpriority in ('1-URGENT', '2-HIGH') then 'urgent'
            else lower(c.c_mktsegment) end as order_label
from customer as c
inner join orders as o on o.o_custkey = c.c_custkey
where c.c_custkey = '1'
order by o.o_orderkey
limit 3`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	got := make(map[string]bool, len(gotRequest.Materialization.ProjectionFields))
	for _, field := range gotRequest.Materialization.ProjectionFields {
		got[string(field.Role)+"."+field.Field] = true
	}
	for _, want := range []string{"c.c_custkey", "o.o_orderkey", "o.o_orderpriority", "c.c_mktsegment"} {
		if !got[want] {
			t.Fatalf("materialization fields = %#v, missing %s", gotRequest.Materialization.ProjectionFields, want)
		}
	}
	visible, diagnostics := legacyDirectRelationshipVisibleProjectionFields(gotRequest)
	if diagnostics.BlocksNative() {
		t.Fatalf("visible projection diagnostics = %#v", diagnostics)
	}
	graphFields := legacyDirectRelationshipGraphProjectionMaterializationFields(gotRequest, visible)
	graphGot := make(map[string]bool, len(graphFields))
	for _, field := range graphFields {
		graphGot[string(field.Role)+"."+field.Field] = true
	}
	for _, want := range []string{"c.c_custkey", "o.o_orderkey", "o.o_orderpriority", "c.c_mktsegment"} {
		if !graphGot[want] {
			t.Fatalf("graph materialization fields = %#v, missing %s", graphFields, want)
		}
	}
}

func TestSQLRuntimeExecuteSQLRunsInsertMutationWithoutLowering(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: 1, LastInsertID: 9001}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "insert into orders (o_orderkey, o_orderpriority) values (9001, '1-URGENT')", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindInsert {
		t.Fatalf("prepared kind = %s, want INSERT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 0 {
		t.Fatalf("fragments = %d, want no SELECT lowering for insert", len(result.Intermediate.Fragments))
	}
	if gotRequest.Mutation.Kind != qsbridge.MutationInsert {
		t.Fatalf("runtime mutation = %q, want insert", gotRequest.Mutation.Kind)
	}
	if gotRequest.Mutation.Target.Table != "orders" {
		t.Fatalf("runtime target = %#v, want orders", gotRequest.Mutation.Target)
	}
	if len(gotRequest.Mutation.Columns) != 2 || gotRequest.Mutation.Columns[0].Name != "o_orderkey" {
		t.Fatalf("runtime columns = %#v, want order columns", gotRequest.Mutation.Columns)
	}
	if result.Runtime.Statement.AffectedRows != 1 {
		t.Fatalf("affected rows = %d, want 1", result.Runtime.Statement.AffectedRows)
	}
}

func TestSQLRuntimeExecuteSQLInsertSelectMaterializesRows(t *testing.T) {
	catalog := qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
			{
				Schema: "quanta",
				Name:   "scratch_keys",
				Fields: []qsbridge.FieldDefinition{
					{Name: "customer_key", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "customer_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
		},
	}
	var insertRequest ExecutionRequest
	var sourceCalls int
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.Mutation.Kind == qsbridge.MutationInsert {
			insertRequest = request
			return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: uint64(len(request.Mutation.Rows))}}, nil
		}
		sourceCalls++
		return ExecutionResult{
			RowSet: qsbridge.QuantaProjectedRowSet{
				Index:   "customer",
				Rownums: []qsbridge.QuantaRownum{1, 2},
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "c_custkey", Type: qsbridge.DataTypeInt, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1)}, {Kind: qsbridge.ValueInt, Value: int64(2)}},
					},
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "Customer#000000001"}, {Kind: qsbridge.ValueString, Value: "Customer#000000002"}},
					},
				},
			},
		}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys (customer_key, customer_name) select c_custkey, c_name from customer order by c_custkey limit 2", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if sourceCalls != 1 {
		t.Fatalf("source SELECT calls = %d, want 1", sourceCalls)
	}
	if result.Runtime.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.Runtime.Statement.AffectedRows)
	}
	if insertRequest.Mutation.Target.Table != "scratch_keys" {
		t.Fatalf("insert target = %#v, want scratch_keys", insertRequest.Mutation.Target)
	}
	if len(insertRequest.Mutation.Rows) != 2 {
		t.Fatalf("insert rows = %d, want 2", len(insertRequest.Mutation.Rows))
	}
	if insertRequest.Mutation.SourceSQL != "" {
		t.Fatalf("insert SourceSQL = %q, want cleared before physical insert", insertRequest.Mutation.SourceSQL)
	}
	firstID, ok := insertRequest.Mutation.Rows[0].Values[0].(qsbridge.LiteralExpr)
	if !ok || firstID.Value != int64(1) {
		t.Fatalf("first inserted id = %#v, want literal 1", insertRequest.Mutation.Rows[0].Values[0])
	}
	secondName, ok := insertRequest.Mutation.Rows[1].Values[1].(qsbridge.LiteralExpr)
	if !ok || secondName.Value != "Customer#000000002" {
		t.Fatalf("second inserted name = %#v", insertRequest.Mutation.Rows[1].Values[1])
	}
}

func TestSQLRuntimeExecuteSQLInsertSelectWithoutColumnListUsesTargetFields(t *testing.T) {
	catalog := qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
			{
				Schema: "quanta",
				Name:   "scratch_keys",
				Fields: []qsbridge.FieldDefinition{
					{Name: "customer_key", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "customer_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
		},
	}
	var insertRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.Mutation.Kind == qsbridge.MutationInsert {
			insertRequest = request
			return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: uint64(len(request.Mutation.Rows))}}, nil
		}
		return ExecutionResult{
			RowSet: qsbridge.QuantaProjectedRowSet{
				Index:   "customer",
				Rownums: []qsbridge.QuantaRownum{1},
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "c_custkey", Type: qsbridge.DataTypeInt, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1)}},
					},
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "Customer#000000001"}},
					},
				},
			},
		}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_keys select c_custkey, c_name from customer order by c_custkey limit 1", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if got := insertRequest.Mutation.Columns; len(got) != 2 || got[0].Name != "customer_key" || got[1].Name != "customer_name" {
		t.Fatalf("insert columns = %#v, want target table fields", got)
	}
	if result.Runtime.Statement.AffectedRows != 1 {
		t.Fatalf("affected rows = %d, want 1", result.Runtime.Statement.AffectedRows)
	}
}

func TestSQLRuntimeExecuteSQLCreateTableAsSelectCreatesAndInsertsRows(t *testing.T) {
	catalog := qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true},
			},
		}},
	}
	var mutationKinds []qsbridge.MutationKind
	var createRequest ExecutionRequest
	var insertRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.Mutation.Kind != qsbridge.MutationUnknown {
			mutationKinds = append(mutationKinds, request.Mutation.Kind)
			switch request.Mutation.Kind {
			case qsbridge.MutationCreateTable:
				createRequest = request
				return ExecutionResult{Statement: qsbridge.StatementResult{Status: "Table scratch_customer created"}}, nil
			case qsbridge.MutationInsert:
				insertRequest = request
				return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: uint64(len(request.Mutation.Rows))}}, nil
			default:
				t.Fatalf("unexpected mutation kind = %q", request.Mutation.Kind)
			}
		}
		return ExecutionResult{
			RowSet: qsbridge.QuantaProjectedRowSet{
				Index:   "customer",
				Rownums: []qsbridge.QuantaRownum{1, 2},
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "customer_key", Type: qsbridge.DataTypeInt, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1)}, {Kind: qsbridge.ValueInt, Value: int64(2)}},
					},
					{
						Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "customer_name", Type: qsbridge.DataTypeString, Visible: true},
						Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "Customer#000000001"}, {Kind: qsbridge.ValueString, Value: "Customer#000000002"}},
					},
				},
			},
		}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "create table scratch_customer as select c_custkey as customer_key, c_name as customer_name from customer order by c_custkey limit 2", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Runtime.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.Runtime.Statement.AffectedRows)
	}
	if got, want := fmt.Sprint(mutationKinds), "[create_table insert]"; got != want {
		t.Fatalf("mutation dispatch order = %s, want %s", got, want)
	}
	if createRequest.Mutation.Target.Table != "scratch_customer" || createRequest.Mutation.SourceSQL == "" {
		t.Fatalf("create mutation = %#v, want durable CTAS", createRequest.Mutation)
	}
	if insertRequest.Mutation.Target.Table != "scratch_customer" {
		t.Fatalf("insert target = %#v, want scratch_customer", insertRequest.Mutation.Target)
	}
	if got, want := len(insertRequest.Mutation.Rows), 2; got != want {
		t.Fatalf("insert rows = %d, want %d", got, want)
	}
	firstID, ok := insertRequest.Mutation.Rows[0].Values[0].(qsbridge.LiteralExpr)
	if !ok || firstID.Value != int64(1) {
		t.Fatalf("first inserted id = %#v, want literal 1", insertRequest.Mutation.Rows[0].Values[0])
	}
	secondName, ok := insertRequest.Mutation.Rows[1].Values[1].(qsbridge.LiteralExpr)
	if !ok || secondName.Value != "Customer#000000002" {
		t.Fatalf("second inserted name = %#v", insertRequest.Mutation.Rows[1].Values[1])
	}
}

func TestSQLRuntimeCreateTableAsSelectAppliesStorageMutationGuardBeforeSourceExecution(t *testing.T) {
	catalog := qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []qsbridge.FieldDefinition{
				{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
				{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true},
			},
		}},
	}
	var sourceExecuted bool
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		sourceExecuted = true
		return ExecutionResult{}, nil
	})
	runtime.StorageMutationGuard = func(ctx context.Context, operation string) qsbridge.DiagnosticSet {
		if operation != "create_table_as_select" {
			t.Fatalf("storage mutation operation = %q, want create_table_as_select", operation)
		}
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "storage is quiesced"),
		}
	}

	result, err := runtime.ExecuteSQL(context.Background(), "create table scratch_customer as select c_custkey as customer_key, c_name as customer_name from customer", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Supported() || !result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("result diagnostics = %#v runtime=%#v, want storage mutation blocker", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if sourceExecuted {
		t.Fatalf("CTAS source SELECT executed before storage mutation guard")
	}
}

func TestSQLRuntimeInsertSelectAppliesStorageMutationGuardBeforeSourceExecution(t *testing.T) {
	catalog := qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "c_custkey", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "c_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
			{
				Schema: "quanta",
				Name:   "scratch_customer",
				Fields: []qsbridge.FieldDefinition{
					{Name: "customer_key", Type: qsbridge.DataTypeInt, PrimaryKey: true},
					{Name: "customer_name", Type: qsbridge.DataTypeString, Nullable: true},
				},
			},
		},
	}
	var sourceExecuted bool
	runtime := newTestSQLRuntimeWithCatalog(t, catalog, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		sourceExecuted = true
		return ExecutionResult{}, nil
	})
	runtime.StorageMutationGuard = func(ctx context.Context, operation string) qsbridge.DiagnosticSet {
		if operation != "insert_select" {
			t.Fatalf("storage mutation operation = %q, want insert_select", operation)
		}
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "storage is quiesced"),
		}
	}

	result, err := runtime.ExecuteSQL(context.Background(), "insert into scratch_customer (customer_key, customer_name) select c_custkey, c_name from customer", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Supported() || !result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("result diagnostics = %#v runtime=%#v, want storage mutation blocker", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if sourceExecuted {
		t.Fatalf("INSERT SELECT source query executed before storage mutation guard")
	}
}

func TestSQLRuntimeExecuteSQLReturnsTransactionStatementsWithoutExecution(t *testing.T) {
	tests := []struct {
		sql      string
		wantKind qsbridge.SessionActionKind
	}{
		{sql: "begin", wantKind: qsbridge.SessionActionBeginTransaction},
		{sql: "start transaction", wantKind: qsbridge.SessionActionBeginTransaction},
		{sql: "rollback", wantKind: qsbridge.SessionActionRollbackTransaction},
		{sql: "commit", wantKind: qsbridge.SessionActionCommitTransaction},
	}

	for _, tt := range tests {
		runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			t.Fatalf("%s should not execute direct runtime", tt.sql)
			return ExecutionResult{}, nil
		})

		result, err := runtime.ExecuteSQL(context.Background(), tt.sql, qsbridge.ExecutionOptions{})

		if err != nil {
			t.Fatalf("%s execute sql: %v", tt.sql, err)
		}
		if !result.Supported() {
			t.Fatalf("%s result diagnostics = %#v / runtime %#v, want supported", tt.sql, result.Diagnostics, result.Runtime.Diagnostics)
		}
		if result.Prepared.Kind != qsbridge.QueryKindSession {
			t.Fatalf("%s prepared kind = %s, want session", tt.sql, result.Prepared.Kind)
		}
		actions := result.Runtime.Statement.SessionActions
		if len(actions) != 1 || actions[0].Kind != tt.wantKind {
			t.Fatalf("%s runtime session actions = %#v, want %s", tt.sql, actions, tt.wantKind)
		}
		if len(result.Intermediate.Fragments) != 0 {
			t.Fatalf("%s fragments = %d, want no SELECT lowering", tt.sql, len(result.Intermediate.Fragments))
		}
	}
}

func TestSQLRuntimeExecuteSQLReturnsSetTransactionWithoutExecution(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("set transaction should not execute direct runtime")
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "set transaction read only", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSession {
		t.Fatalf("prepared kind = %s, want session", result.Prepared.Kind)
	}
	if len(result.Runtime.Statement.SessionActions) != 0 {
		t.Fatalf("runtime session actions = %#v, want no-op", result.Runtime.Statement.SessionActions)
	}
	if len(result.Intermediate.Fragments) != 0 {
		t.Fatalf("fragments = %d, want no SELECT lowering", len(result.Intermediate.Fragments))
	}
}

func TestSQLRuntimeInspectSQLPlansLowersAndInspectsWithoutExecution(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute runtime")
		return ExecutionResult{}, nil
	})

	result := runtime.InspectSQL("select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})

	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSelect {
		t.Fatalf("prepared kind = %s, want SELECT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one: %#v", len(result.Intermediate.Fragments), result.Intermediate.Fragments)
	}
	if result.Runtime.SelectedExecutor != ExecutionInspectionExecutorDirect {
		t.Fatalf("selected executor = %q, want direct", result.Runtime.SelectedExecutor)
	}
	if result.Runtime.CallPlan.RootIndex != "orders" {
		t.Fatalf("call plan root = %q, want orders", result.Runtime.CallPlan.RootIndex)
	}
	if !result.Runtime.CallPlan.Contains(LegacyExecutionStepBuildBitmapQuery) {
		t.Fatalf("call plan missing bitmap query step: %v", result.Runtime.CallPlan.Steps)
	}
}

func TestSQLRuntimeInspectSQLLowersMixedBooleanFilterDespiteNativeBlocker(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.InspectSQL(
		"select o_orderkey from orders where (o_orderkey = 7 and o_orderkey > 1) or (o_orderkey = 8 and o_orderkey > 2)",
		qsbridge.ExecutionOptions{},
	)

	if result.Supported() {
		t.Fatalf("result supported, want mixed boolean blocker")
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticMixedBooleanPredicate)
	filter := result.Intermediate.Filter
	if filter.Operation != qsbridge.QuantaFilterUnion {
		t.Fatalf("filter operation = %s, want %s: %#v", filter.Operation, qsbridge.QuantaFilterUnion, filter)
	}
	if len(filter.Children) != 2 {
		t.Fatalf("filter children = %d, want 2: %#v", len(filter.Children), filter.Children)
	}
	for branchIndex, branch := range filter.Children {
		if branch.Operation != qsbridge.QuantaFilterIntersect {
			t.Fatalf("branch %d operation = %s, want %s: %#v", branchIndex, branch.Operation, qsbridge.QuantaFilterIntersect, branch)
		}
	}
}

func TestSQLRuntimeInspectSQLReturnsParserDiagnostics(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.InspectSQL("select from", qsbridge.ExecutionOptions{})

	if result.Supported() {
		t.Fatalf("result = %#v, want parser diagnostics", result)
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticParserBoundary)
}

func TestSQLRuntimeExecuteSQLReturnsParserDiagnostics(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), "select from", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Supported() {
		t.Fatalf("result = %#v, want parser diagnostics", result)
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticParserBoundary)
}

func TestSQLRuntimeExecuteSQLMaterializesTrueExistsGate(t *testing.T) {
	var parentRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if len(request.Query.Fragments) > 0 {
			return ExecutionResult{Count: 1}, nil
		}
		parentRequest = request
		return ExecutionResult{Count: 11}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select count(*)
		from orders
		where exists (
			select o_orderkey
			from orders
			where o_orderkey = 1
		)
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if parentRequest.HasCandidateSet {
		t.Fatalf("parent request has candidate set for true EXISTS gate: %#v", parentRequest.CandidateSet)
	}
	if got, want := len(result.Request.Bound.Prepared.Query.Predicates), 0; got != want {
		t.Fatalf("prepared predicates = %d, want true gate pruned", got)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesFalseExistsGateAsEmptyCandidateSet(t *testing.T) {
	var parentRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.HasCandidateSet {
			parentRequest = request
			return ExecutionResult{Count: uint64(len(request.CandidateSet.Rownums))}, nil
		}
		return ExecutionResult{Count: 0}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select count(*)
		from orders
		where exists (
			select o_orderkey
			from orders
			where o_orderkey = -999
		)
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if !parentRequest.HasCandidateSet {
		t.Fatalf("parent request missing empty candidate set for false EXISTS gate")
	}
	if parentRequest.CandidateSet.Index != "orders" || len(parentRequest.CandidateSet.Rownums) != 0 {
		t.Fatalf("candidate set = %#v, want empty orders set", parentRequest.CandidateSet)
	}
}

func newTestSQLRuntime(t *testing.T) SQLRuntime {
	t.Helper()
	return newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 11}, nil
	})
}

func TestCatalogCharacterMetadataRowsFilterAcrossDisplayedFields(t *testing.T) {
	charsets := showCharacterSetRows("utf8mb4_0900_ai_ci", nil)
	if len(charsets) != 1 || charsets[0].charset != "utf8mb4" {
		t.Fatalf("charsets = %#v, want utf8mb4 row by default collation", charsets)
	}

	collations := showCollationRows("utf8mb4", nil)
	if len(collations) != 2 {
		t.Fatalf("collations = %#v, want both utf8mb4 collations by charset", collations)
	}
}

func newTestSQLRuntimeWithDirect(t *testing.T, execute func(context.Context, ExecutionRequest) (ExecutionResult, error)) SQLRuntime {
	t.Helper()
	builder := SQLRuntimeBuilder{
		Parser:         qsbridge.SimpleParserBridge{},
		DefaultSchema:  "quanta",
		CatalogVersion: qsbridge.CatalogVersion("test-catalog-v1"),
		EnvironmentBuilder: RuntimeEnvironmentBuilder{
			Config:         NewDirectRuntimeConfig("", "", 0, 0),
			CatalogFactory: LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()},
			DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
				return DirectRuntimeFunc(execute), nil, nil
			}),
		},
	}
	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build sql runtime: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("build diagnostics = %#v, want none", diagnostics)
	}
	return runtime
}

func newTestSQLRuntimeWithCatalog(t *testing.T, catalog qsbridge.Catalog, execute func(context.Context, ExecutionRequest) (ExecutionResult, error)) SQLRuntime {
	t.Helper()
	builder := SQLRuntimeBuilder{
		Parser:         qsbridge.SimpleParserBridge{},
		DefaultSchema:  "quanta",
		CatalogVersion: qsbridge.CatalogVersion("test-catalog-v1"),
		EnvironmentBuilder: RuntimeEnvironmentBuilder{
			Config: NewDirectRuntimeConfig("", "", 0, 0),
			CatalogFactory: RuntimeCatalogFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
				return qsbridge.NewCachedCatalog(catalog), nil, nil
			}),
			DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
				return DirectRuntimeFunc(execute), nil, nil
			}),
		},
	}
	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build sql runtime: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("build diagnostics = %#v, want none", diagnostics)
	}
	return runtime
}
