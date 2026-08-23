package qsruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLegacySchemaMutationHandleCreatesAndDropsFileCatalogView(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "customer_names",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:    qsbridge.MutationCreateView,
			Target:  qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
			ViewSQL: "select c_custkey, c_name from customer",
			ViewDependencies: []qsbridge.TableInstance{
				{Schema: "quanta", Table: "customer"},
			},
		},
	}

	statement, diagnostics, err := handle.CreateView(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("CreateView() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View customer_names created" {
		t.Fatalf("status = %q", statement.Status)
	}
	active, err := shared.CatalogViewActive(configDir, "quanta", "customer_names")
	if err != nil {
		t.Fatalf("CatalogViewActive() error = %v", err)
	}
	if !active {
		t.Fatalf("view should be active")
	}
	view, err := shared.LoadViewDefinition(configDir, "customer_names")
	if err != nil {
		t.Fatalf("LoadViewDefinition() error = %v", err)
	}
	if view.SQL != request.Mutation.ViewSQL {
		t.Fatalf("view SQL = %q, want %q", view.SQL, request.Mutation.ViewSQL)
	}
	if len(view.Dependencies) != 1 || view.Dependencies[0].ObjectName != "customer" {
		t.Fatalf("dependencies = %#v, want customer", view.Dependencies)
	}
	catalogView, catalogDiagnostics := (LegacyTableCacheCatalog{ConfigDir: configDir}).View("quanta", "customer_names")
	if catalogDiagnostics.BlocksNative() {
		t.Fatalf("catalog view diagnostics = %#v", catalogDiagnostics)
	}
	if catalogView.Name != "customer_names" || catalogView.SQL != request.Mutation.ViewSQL {
		t.Fatalf("catalog view = %#v", catalogView)
	}

	dropRequest := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropView,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
		},
	}
	statement, diagnostics, err = handle.DropView(context.Background(), dropRequest)
	if err != nil {
		t.Fatalf("DropView() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View customer_names dropped" {
		t.Fatalf("drop status = %q", statement.Status)
	}
	active, err = shared.CatalogViewActive(configDir, "quanta", "customer_names")
	if err != nil {
		t.Fatalf("CatalogViewActive() after drop error = %v", err)
	}
	if active {
		t.Fatalf("view should not be active after drop")
	}
	if shared.ViewDefinitionExists(configDir, "customer_names") {
		t.Fatalf("view definition should be removed")
	}
}

func TestLegacySchemaMutationHandleDropViewIfExistsIgnoresMissingFileCatalogView(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "missing_view",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropView,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "missing_view"},
		},
	}
	_, diagnostics, err := handle.DropView(context.Background(), request)
	if err == nil {
		t.Fatalf("DropView() error = nil, want missing-view error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView() diagnostics = %#v", diagnostics)
	}

	request.Mutation.IfExists = true
	statement, diagnostics, err := handle.DropView(context.Background(), request)
	if err != nil {
		t.Fatalf("DropView(if exists) error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView(if exists) diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View missing_view dropped" {
		t.Fatalf("status = %q, want missing_view dropped", statement.Status)
	}
}

func TestLegacySchemaMutationHandleDropViewCascadeRemovesDependentViews(t *testing.T) {
	configDir := t.TempDir()
	now := testSchemaMutationTime()
	views := []shared.ViewDefinition{
		{
			SchemaName: "quanta",
			ViewName:   "customer_names",
			SQL:        "select c_custkey, c_name from customer",
			Dependencies: []shared.ViewDependency{
				{SchemaName: "quanta", ObjectName: "customer", ObjectType: shared.CatalogObjectTypeTable},
			},
			CreationDate:     now,
			ModificationDate: now,
		},
		{
			SchemaName: "quanta",
			ViewName:   "customer_names_copy",
			SQL:        "select c_custkey, c_name from customer_names",
			Dependencies: []shared.ViewDependency{
				{SchemaName: "quanta", ObjectName: "customer_names", ObjectType: shared.CatalogObjectTypeView},
				{SchemaName: "quanta", ObjectName: "customer", ObjectType: shared.CatalogObjectTypeTable},
			},
			CreationDate:     now,
			ModificationDate: now,
		},
	}
	for _, view := range views {
		if err := shared.SaveViewDefinition(configDir, view); err != nil {
			t.Fatalf("SaveViewDefinition(%s) error = %v", view.ViewName, err)
		}
		if err := shared.ActivateCatalogView(configDir, "quanta", view.ViewName, now); err != nil {
			t.Fatalf("ActivateCatalogView(%s) error = %v", view.ViewName, err)
		}
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customer_names",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:    qsbridge.MutationDropView,
			Target:  qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
			Cascade: true,
		},
	}

	statement, diagnostics, err := handle.DropView(context.Background(), request)
	if err != nil {
		t.Fatalf("DropView(cascade) error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView(cascade) diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View customer_names dropped" {
		t.Fatalf("status = %q, want customer_names dropped", statement.Status)
	}
	for _, viewName := range []string{"customer_names", "customer_names_copy"} {
		active, err := shared.CatalogViewActive(configDir, "quanta", viewName)
		if err != nil {
			t.Fatalf("CatalogViewActive(%s) error = %v", viewName, err)
		}
		if active {
			t.Fatalf("view %s should not be active after cascade", viewName)
		}
		if shared.ViewDefinitionExists(configDir, viewName) {
			t.Fatalf("view definition %s should be removed after cascade", viewName)
		}
	}
}

func TestLegacySchemaMutationHandleDropTableIfExistsIgnoresMissingFileCatalogTable(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "missing_table",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropTable,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "missing_table"},
		},
	}
	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want missing-table error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}

	request.Mutation.IfExists = true
	statement, diagnostics, err := handle.DropTable(context.Background(), request)
	if err != nil {
		t.Fatalf("DropTable(if exists) error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable(if exists) diagnostics = %#v", diagnostics)
	}
	if statement.Status != "Table missing_table dropped" {
		t.Fatalf("status = %q, want missing_table dropped", statement.Status)
	}
}

func TestLegacySchemaMutationHandleAlterTableAddPrimaryKeyBlocksUnknownRowCountWithoutCatalogOnly(t *testing.T) {
	t.Setenv(alterTableAddPrimaryKeyCatalogOnlyEnv, "")
	configDir := t.TempDir()
	writeSchemaMutationKeylessCatalogSchema(t, configDir, "scratch_orders")
	if err := shared.ActivateCatalogTable(configDir, "quanta", "scratch_orders", testSchemaMutationTime()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "scratch_orders",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationAlterTableAddPrimaryKey,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "scratch_orders"},
			Columns: []qsbridge.FieldRef{
				{Name: "order_key", PrimaryKey: true},
			},
			ValidationSteps: []qsbridge.MutationValidationStep{
				{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
				{Kind: qsbridge.MutationValidationPrimaryKeyDuplicateScan},
			},
		},
	}

	_, diagnostics, err := handle.AlterTableAddPrimaryKey(context.Background(), request)
	if err != nil {
		t.Fatalf("AlterTableAddPrimaryKey() error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want authority artifact blocker", diagnostics)
	}
	if len(diagnostics) == 0 ||
		diagnostics[0].Code != qsbridge.DiagnosticUnsupportedMutation ||
		!strings.Contains(diagnostics[0].Error(), "cannot activate metadata") ||
		!strings.Contains(diagnostics[0].Error(), "row count is unknown") ||
		!strings.Contains(diagnostics[0].Error(), "primary-key authority artifacts") {
		t.Fatalf("diagnostics = %#v, want authority artifact activation blocker", diagnostics)
	}
}

func TestLegacySchemaMutationHandleAlterTableAddPrimaryKeyCatalogOnlyUpdatesFileCatalog(t *testing.T) {
	t.Setenv(alterTableAddPrimaryKeyCatalogOnlyEnv, "1")
	configDir := t.TempDir()
	writeSchemaMutationKeylessCatalogSchema(t, configDir, "scratch_orders")
	if err := shared.ActivateCatalogTable(configDir, "quanta", "scratch_orders", testSchemaMutationTime()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "scratch_orders",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationAlterTableAddPrimaryKey,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "scratch_orders"},
			Columns: []qsbridge.FieldRef{
				{Name: "order_key", PrimaryKey: true, Nullable: false},
			},
			ValidationSteps: []qsbridge.MutationValidationStep{
				{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
				{Kind: qsbridge.MutationValidationPrimaryKeyDuplicateScan},
			},
		},
	}

	statement, diagnostics, err := handle.AlterTableAddPrimaryKey(context.Background(), request)
	if err != nil {
		t.Fatalf("AlterTableAddPrimaryKey() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("AlterTableAddPrimaryKey() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "Primary key added to table scratch_orders" {
		t.Fatalf("status = %q, want primary key added", statement.Status)
	}
	active, err := shared.CatalogTableActive(configDir, "quanta", "scratch_orders")
	if err != nil {
		t.Fatalf("CatalogTableActive() error = %v", err)
	}
	if !active {
		t.Fatalf("table should remain active after ALTER TABLE ADD PRIMARY KEY")
	}
	table, err := shared.LoadSchema(configDir, "scratch_orders", nil)
	if err != nil {
		t.Fatalf("LoadSchema() error = %v", err)
	}
	if table.PrimaryKey != "order_key" {
		t.Fatalf("primary key = %q, want order_key", table.PrimaryKey)
	}
	orderKey, err := table.GetAttribute("order_key")
	if err != nil {
		t.Fatalf("GetAttribute(order_key) error = %v", err)
	}
	if !orderKey.ColumnID || !orderKey.Required {
		t.Fatalf("order_key metadata = columnID:%v required:%v, want true/true", orderKey.ColumnID, orderKey.Required)
	}
	customerKey, err := table.GetAttribute("customer_key")
	if err != nil {
		t.Fatalf("GetAttribute(customer_key) error = %v", err)
	}
	if customerKey.ColumnID {
		t.Fatalf("customer_key ColumnID = true, want false")
	}
}

func TestLegacySchemaMutationHandleAlterTableAddPrimaryKeyAllowsActiveCatalogWithoutRootAsEmpty(t *testing.T) {
	t.Setenv(alterTableAddPrimaryKeyCatalogOnlyEnv, "")
	configDir := t.TempDir()
	writeSchemaMutationKeylessCatalogSchema(t, configDir, "scratch_orders")
	if err := shared.ActivateCatalogTable(configDir, "quanta", "scratch_orders", testSchemaMutationTime()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "scratch_orders",
		Session: &core.Session{
			BasePath: configDir,
			BitIndex: &shared.BitmapIndex{},
		},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationAlterTableAddPrimaryKey,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "scratch_orders"},
			Columns: []qsbridge.FieldRef{
				{Name: "order_key", PrimaryKey: true, Nullable: false},
			},
			ValidationSteps: []qsbridge.MutationValidationStep{
				{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
				{Kind: qsbridge.MutationValidationPrimaryKeyDuplicateScan},
			},
		},
	}

	statement, diagnostics, err := handle.AlterTableAddPrimaryKey(context.Background(), request)
	if err != nil {
		t.Fatalf("AlterTableAddPrimaryKey() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("AlterTableAddPrimaryKey() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "Primary key added to table scratch_orders (catalog-only row_count=0)" {
		t.Fatalf("status = %q, want catalog-only zero-row status", statement.Status)
	}
	table, err := shared.LoadSchema(configDir, "scratch_orders", nil)
	if err != nil {
		t.Fatalf("LoadSchema() error = %v", err)
	}
	if table.PrimaryKey != "order_key" {
		t.Fatalf("primary key = %q, want order_key", table.PrimaryKey)
	}
}

func TestLegacySchemaMutationHandleAlterTableAddPrimaryKeyCatalogOnlyRequiresValidationSteps(t *testing.T) {
	t.Setenv(alterTableAddPrimaryKeyCatalogOnlyEnv, "1")
	handle := LegacyQuantaSessionHandle{
		TableName: "scratch_orders",
		Session:   &core.Session{BasePath: t.TempDir()},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationAlterTableAddPrimaryKey,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "scratch_orders"},
			Columns: []qsbridge.FieldRef{
				{Name: "order_key", PrimaryKey: true},
			},
		},
	}

	_, diagnostics, err := handle.AlterTableAddPrimaryKey(context.Background(), request)
	if err != nil {
		t.Fatalf("AlterTableAddPrimaryKey() error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want missing validation blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticInternalInvariant || !strings.Contains(diagnostics[0].Error(), "primary_key_null_scan, primary_key_duplicate_scan") {
		t.Fatalf("diagnostics = %#v, want missing validation step diagnostic", diagnostics)
	}
}

func TestAlterTableAddPrimaryKeyCatalogOnlyNonEmptyDiagnostic(t *testing.T) {
	plans := []alterTableAddPrimaryKeyValidationPlan{
		{
			Mode:    alterTableAddPrimaryKeyValidationNullScan,
			Table:   "scratch_orders",
			Columns: []string{"order_key", "line_number"},
		},
		{
			Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
			Table:   "scratch_orders",
			Columns: []string{"order_key", "line_number"},
		},
	}
	diagnostics := alterTableAddPrimaryKeyCatalogOnlyNonEmptyDiagnostic("scratch_orders", 3, plans)
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticUnsupportedMutation {
		t.Fatalf("diagnostics = %#v, want unsupported mutation", diagnostics)
	}
	message := diagnostics[0].Error()
	if !strings.Contains(message, "scratch_orders with 3 existing row(s)") ||
		!strings.Contains(message, "pending validation scans") ||
		!strings.Contains(message, "pk_null_scan(order_key,line_number)") ||
		!strings.Contains(message, "pk_duplicate_scan(order_key,line_number)") {
		t.Fatalf("diagnostic = %q, want row count and validation guidance", message)
	}
}

func TestAlterTableAddPrimaryKeyCatalogOnlyStatusIncludesKnownRowCount(t *testing.T) {
	if got := alterTableAddPrimaryKeyCatalogOnlyStatus("scratch_orders", 0, false); got != "Primary key added to table scratch_orders" {
		t.Fatalf("status = %q, want plain status when row count is unknown", got)
	}
	if got := alterTableAddPrimaryKeyCatalogOnlyStatus("scratch_orders", 0, true); got != "Primary key added to table scratch_orders (catalog-only row_count=0)" {
		t.Fatalf("status = %q, want catalog-only row count", got)
	}
}

func TestAlterTableAddPrimaryKeyActivationDiagnosticsAllowsProvenEmptyTable(t *testing.T) {
	diagnostics := alterTableAddPrimaryKeyActivationDiagnostics("scratch_orders", alterTableAddPrimaryKeyValidationResult{
		RowCountKnown: true,
		RowCount:      0,
	}, false)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want proven empty table allowed", diagnostics)
	}
}

func TestAlterTableAddPrimaryKeyActivationDiagnosticsBlocksNonEmptyWithoutCatalogOnly(t *testing.T) {
	diagnostics := alterTableAddPrimaryKeyActivationDiagnostics("scratch_orders", alterTableAddPrimaryKeyValidationResult{
		RowCountKnown: true,
		RowCount:      3,
	}, false)
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want non-empty table blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticUnsupportedMutation || !strings.Contains(diagnostics[0].Error(), "table has 3 existing row(s)") {
		t.Fatalf("diagnostics = %#v, want row-count blocker", diagnostics)
	}
}

func TestAlterTableAddPrimaryKeyActivationDiagnosticsAllowsCatalogOnlyOverride(t *testing.T) {
	diagnostics := alterTableAddPrimaryKeyActivationDiagnostics("scratch_orders", alterTableAddPrimaryKeyValidationResult{
		RowCountKnown: true,
		RowCount:      3,
	}, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want catalog-only override allowed", diagnostics)
	}
}

func TestValidateAlterTableAddPrimaryKeyCatalogOnlyAllowsUnknownLightweightCount(t *testing.T) {
	handle := LegacyQuantaSessionHandle{}
	result, diagnostics, err := handle.validateAlterTableAddPrimaryKeyCatalogOnly(context.Background(), "quanta", "scratch_orders", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{{Name: "order_key", PrimaryKey: true}},
		ValidationSteps: []qsbridge.MutationValidationStep{
			{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
			{Kind: qsbridge.MutationValidationPrimaryKeyDuplicateScan},
		},
	})
	if err != nil {
		t.Fatalf("validateAlterTableAddPrimaryKeyCatalogOnly() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("validateAlterTableAddPrimaryKeyCatalogOnly() diagnostics = %#v", diagnostics)
	}
	if result.RowCountKnown {
		t.Fatalf("RowCountKnown = true, want false for lightweight handle")
	}
	if len(result.Plans) != 2 {
		t.Fatalf("plans = %#v, want validation plans surfaced", result.Plans)
	}
	if result.Plans[0].Mode != alterTableAddPrimaryKeyValidationNullScan || result.Plans[1].Mode != alterTableAddPrimaryKeyValidationDuplicateScan {
		t.Fatalf("plans = %#v, want null then duplicate scan", result.Plans)
	}
}

func TestValidateAlterTableAddPrimaryKeyRowCountPropagatesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handle := LegacyQuantaSessionHandle{}
	_, diagnostics, err := handle.validateAlterTableAddPrimaryKeyRowCount(ctx, "quanta", "scratch_orders", qsbridge.MutationShape{}, nil)
	if err == nil {
		t.Fatalf("validateAlterTableAddPrimaryKeyRowCount() error = nil, want context cancellation")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("validateAlterTableAddPrimaryKeyRowCount() diagnostics = %#v", diagnostics)
	}
}

func TestExecuteAlterTableAddPrimaryKeyValidationPlansKeepsUnknownNullScanPending(t *testing.T) {
	handle := LegacyQuantaSessionHandle{}
	result, diagnostics, err := handle.executeAlterTableAddPrimaryKeyValidationPlans(context.Background(), alterTableAddPrimaryKeyValidationResult{
		RowCount:      3,
		RowCountKnown: true,
		Plans: []alterTableAddPrimaryKeyValidationPlan{
			{
				Mode:    alterTableAddPrimaryKeyValidationNullScan,
				Table:   "scratch_orders",
				Columns: []string{"order_key"},
			},
			{
				Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
				Table:   "scratch_orders",
				Columns: []string{"order_key"},
			},
		},
	})
	if err != nil {
		t.Fatalf("executeAlterTableAddPrimaryKeyValidationPlans() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("executeAlterTableAddPrimaryKeyValidationPlans() diagnostics = %#v", diagnostics)
	}
	if len(result.PendingPlans) != 2 || len(result.CompletedPlans) != 0 {
		t.Fatalf("pending=%#v completed=%#v, want both plans pending", result.PendingPlans, result.CompletedPlans)
	}
}

func TestAlterTableAddPrimaryKeyNullScanRequestUsesNullCheckFragment(t *testing.T) {
	request := alterTableAddPrimaryKeyNullScanRequest(alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationNullScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key"},
	}, "order_key")
	if got, want := strings.Join(request.SourceIndexes, ","), "scratch_orders"; got != want {
		t.Fatalf("SourceIndexes = %q, want %q", got, want)
	}
	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one null-check fragment", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "scratch_orders" || fragment.Field != "order_key" || fragment.Operation != qsbridge.QuantaOperationIntersect || !fragment.NullCheck {
		t.Fatalf("fragment = %#v, want scratch_orders.order_key IS NULL", fragment)
	}
}

func TestAlterTableAddPrimaryKeyNullDiagnosticUsesMutationNullCode(t *testing.T) {
	diagnostics := alterTableAddPrimaryKeyNullDiagnostic(alterTableAddPrimaryKeyValidationPlan{
		Table: "scratch_orders",
	}, alterTableAddPrimaryKeyNullScanResult{
		Known:  true,
		Column: "order_key",
		Count:  2,
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticMutationPrimaryKeyNull {
		t.Fatalf("diagnostics = %#v, want mutation primary key null code", diagnostics)
	}
	if message := diagnostics[0].Error(); !strings.Contains(message, "order_key has 2 NULL value(s)") {
		t.Fatalf("diagnostic = %q, want null count", message)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanMaterializationRequestProjectsKeyColumns(t *testing.T) {
	request := alterTableAddPrimaryKeyDuplicateScanMaterializationRequest(alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key", "line_number"},
	}, []qsbridge.QuantaRownum{7, 8})
	if request.Index != "scratch_orders" {
		t.Fatalf("Index = %q, want scratch_orders", request.Index)
	}
	if got, want := fmt.Sprint(request.Rownums), "[7 8]"; got != want {
		t.Fatalf("Rownums = %s, want %s", got, want)
	}
	if len(request.ProjectionFields) != 2 {
		t.Fatalf("ProjectionFields = %#v, want two key columns", request.ProjectionFields)
	}
	if request.ProjectionFields[0].Field != "order_key" || request.ProjectionFields[1].Field != "line_number" {
		t.Fatalf("ProjectionFields = %#v, want key columns in plan order", request.ProjectionFields)
	}
	if request.ProjectionFields[0].Visible || request.ProjectionFields[1].Visible {
		t.Fatalf("ProjectionFields = %#v, want hidden validation fields", request.ProjectionFields)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanProjectedRowsDetectsDuplicateTuple(t *testing.T) {
	plan := alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key", "line_number"},
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "scratch_orders",
		Rownums: []qsbridge.QuantaRownum{11, 12, 13},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "scratch_orders", Field: "order_key"},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(100)},
					{Kind: qsbridge.ValueInt, Value: int64(101)},
					{Kind: qsbridge.ValueInt, Value: int64(100)},
				},
			},
			{
				Field: qsbridge.QuantaProjectionField{Index: "scratch_orders", Field: "line_number"},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(1)},
				},
			},
		},
	}
	result, diagnostics := alterTableAddPrimaryKeyDuplicateScanProjectedRows(plan, rowSet)
	if diagnostics.BlocksNative() {
		t.Fatalf("alterTableAddPrimaryKeyDuplicateScanProjectedRows() diagnostics = %#v", diagnostics)
	}
	if !result.Known || result.Count != 2 || result.FirstRownum != 11 || result.DuplicateRownum != 13 {
		t.Fatalf("result = %#v, want duplicate tuple on rownums 11 and 13", result)
	}
	if got, want := alterTableAddPrimaryKeyTupleDisplay(result.Tuple), "(100,1)"; got != want {
		t.Fatalf("tuple = %q, want %q", got, want)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanStateDetectsDuplicateAcrossProjectedRowSets(t *testing.T) {
	plan := alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key"},
	}
	state := alterTableAddPrimaryKeyDuplicateScanState{}
	first := qsbridge.QuantaProjectedRowSet{
		Index:   "scratch_orders",
		Rownums: []qsbridge.QuantaRownum{11},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "scratch_orders", Field: "order_key"},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(100)},
				},
			},
		},
	}
	result, diagnostics := state.projectRows(plan, first)
	if diagnostics.BlocksNative() {
		t.Fatalf("ProjectRows(first) diagnostics = %#v", diagnostics)
	}
	if !result.Known || result.Count != 0 {
		t.Fatalf("first result = %#v, want clean known batch", result)
	}
	second := qsbridge.QuantaProjectedRowSet{
		Index:   "scratch_orders",
		Rownums: []qsbridge.QuantaRownum{42},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "scratch_orders", Field: "order_key"},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(100)},
				},
			},
		},
	}
	result, diagnostics = state.projectRows(plan, second)
	if diagnostics.BlocksNative() {
		t.Fatalf("ProjectRows(second) diagnostics = %#v", diagnostics)
	}
	if !result.Known || result.Count != 2 || result.FirstRownum != 11 || result.DuplicateRownum != 42 {
		t.Fatalf("second result = %#v, want duplicate across batches", result)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanProjectedRowsAcceptsUniqueTuples(t *testing.T) {
	plan := alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key"},
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "scratch_orders",
		Rownums: []qsbridge.QuantaRownum{11, 12},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "scratch_orders", Field: "order_key"},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(100)},
					{Kind: qsbridge.ValueInt, Value: int64(101)},
				},
			},
		},
	}
	result, diagnostics := alterTableAddPrimaryKeyDuplicateScanProjectedRows(plan, rowSet)
	if diagnostics.BlocksNative() {
		t.Fatalf("alterTableAddPrimaryKeyDuplicateScanProjectedRows() diagnostics = %#v", diagnostics)
	}
	if !result.Known || result.Count != 0 {
		t.Fatalf("result = %#v, want known clean duplicate scan", result)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanRownumBatchesCopiesBoundedBatches(t *testing.T) {
	rownums := []qsbridge.QuantaRownum{1, 2, 3, 4, 5}
	batches := alterTableAddPrimaryKeyDuplicateScanRownumBatches(rownums, 2)
	if len(batches) != 3 {
		t.Fatalf("batches = %#v, want 3 batches", batches)
	}
	if got, want := fmt.Sprint(batches[0]), "[1 2]"; got != want {
		t.Fatalf("batch[0] = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(batches[1]), "[3 4]"; got != want {
		t.Fatalf("batch[1] = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(batches[2]), "[5]"; got != want {
		t.Fatalf("batch[2] = %s, want %s", got, want)
	}
	rownums[0] = 99
	if batches[0][0] != 1 {
		t.Fatalf("batch[0][0] = %d, want copied rownum", batches[0][0])
	}
}

func TestAlterTableAddPrimaryKeyDuplicateScanWithoutMaterializerIsUnknown(t *testing.T) {
	handle := LegacyQuantaSessionHandle{}
	result, diagnostics, err := handle.alterTableAddPrimaryKeyDuplicateScan(context.Background(), alterTableAddPrimaryKeyValidationPlan{
		Mode:    alterTableAddPrimaryKeyValidationDuplicateScan,
		Table:   "scratch_orders",
		Columns: []string{"order_key"},
	})
	if err != nil {
		t.Fatalf("alterTableAddPrimaryKeyDuplicateScan() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("alterTableAddPrimaryKeyDuplicateScan() diagnostics = %#v", diagnostics)
	}
	if result.Known {
		t.Fatalf("result = %#v, want unknown duplicate scan without materializer", result)
	}
}

func TestAlterTableAddPrimaryKeyDuplicateDiagnosticUsesMutationDuplicateCode(t *testing.T) {
	diagnostics := alterTableAddPrimaryKeyDuplicateDiagnostic(alterTableAddPrimaryKeyValidationPlan{
		Table: "scratch_orders",
	}, alterTableAddPrimaryKeyDuplicateScanResult{
		Known:           true,
		Count:           2,
		FirstRownum:     11,
		DuplicateRownum: 13,
		Tuple: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(100)},
			{Kind: qsbridge.ValueInt, Value: int64(1)},
		},
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticMutationPrimaryKeyDuplicate {
		t.Fatalf("diagnostics = %#v, want mutation primary key duplicate code", diagnostics)
	}
	message := diagnostics[0].Error()
	if !strings.Contains(message, "duplicate primary-key tuple (100,1)") || !strings.Contains(message, "first_rownum=11") || !strings.Contains(message, "duplicate_rownum=13") {
		t.Fatalf("diagnostic = %q, want duplicate tuple detail", message)
	}
}

func TestAlterTableAddPrimaryKeyValidationPlansBuildsNullAndDuplicateScans(t *testing.T) {
	plans, diagnostics := alterTableAddPrimaryKeyValidationPlans("scratch_orders", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{
			{Name: "order_key", PrimaryKey: true},
			{Name: "line_number", PrimaryKey: true},
		},
		ValidationSteps: []qsbridge.MutationValidationStep{
			{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
			{Kind: qsbridge.MutationValidationPrimaryKeyDuplicateScan},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("alterTableAddPrimaryKeyValidationPlans() diagnostics = %#v", diagnostics)
	}
	if len(plans) != 2 {
		t.Fatalf("plans = %#v, want two plans", plans)
	}
	if plans[0].Mode != alterTableAddPrimaryKeyValidationNullScan || plans[1].Mode != alterTableAddPrimaryKeyValidationDuplicateScan {
		t.Fatalf("plans = %#v, want null then duplicate scan", plans)
	}
	for _, plan := range plans {
		if plan.Table != "scratch_orders" {
			t.Fatalf("plan table = %q, want scratch_orders", plan.Table)
		}
		if got, want := strings.Join(plan.Columns, ","), "order_key,line_number"; got != want {
			t.Fatalf("plan columns = %q, want %q", got, want)
		}
	}
}

func TestAlterTableAddPrimaryKeyValidationPlansHonorsStepColumns(t *testing.T) {
	plans, diagnostics := alterTableAddPrimaryKeyValidationPlans("scratch_orders", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{{Name: "order_key", PrimaryKey: true}},
		ValidationSteps: []qsbridge.MutationValidationStep{
			{
				Kind: qsbridge.MutationValidationPrimaryKeyNullScan,
				Columns: []qsbridge.FieldRef{
					{Name: "order_key", PrimaryKey: true},
					{Name: "line_number", PrimaryKey: true},
				},
			},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("alterTableAddPrimaryKeyValidationPlans() diagnostics = %#v", diagnostics)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one plan", plans)
	}
	if got, want := strings.Join(plans[0].Columns, ","), "order_key,line_number"; got != want {
		t.Fatalf("plan columns = %q, want %q", got, want)
	}
}

func TestAlterTableAddPrimaryKeyValidationPlansRejectsDuplicateColumns(t *testing.T) {
	_, diagnostics := alterTableAddPrimaryKeyValidationPlans("scratch_orders", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{
			{Name: "order_key", PrimaryKey: true},
			{Name: "ORDER_KEY", PrimaryKey: true},
		},
		ValidationSteps: []qsbridge.MutationValidationStep{
			{Kind: qsbridge.MutationValidationPrimaryKeyNullScan},
		},
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want duplicate-column blocker", diagnostics)
	}
	if len(diagnostics) == 0 || !strings.Contains(diagnostics[0].Error(), "duplicate column") {
		t.Fatalf("diagnostics = %#v, want duplicate-column message", diagnostics)
	}
}

func TestApplyAlterTableAddPrimaryKeyCatalogMutationAddsCompoundAuthority(t *testing.T) {
	table := &shared.BasicTable{
		Name: "scratch_order_lines",
		Attributes: []shared.BasicAttribute{
			{FieldName: "order_key", SourceName: "/order_key", Type: "Integer", MappingStrategy: "IntBSI"},
			{FieldName: "line_number", SourceName: "/line_number", Type: "Integer", MappingStrategy: "IntBSI"},
			{FieldName: "amount", SourceName: "/amount", Type: "Float", MappingStrategy: "FloatScaleBSI"},
		},
	}
	diagnostics, err := applyAlterTableAddPrimaryKeyCatalogMutation(table, qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{
			{Name: "order_key", PrimaryKey: true},
			{Name: "line_number", PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("applyAlterTableAddPrimaryKeyCatalogMutation() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("applyAlterTableAddPrimaryKeyCatalogMutation() diagnostics = %#v", diagnostics)
	}
	if table.PrimaryKey != "order_key+line_number" {
		t.Fatalf("primary key = %q, want order_key+line_number", table.PrimaryKey)
	}
	for _, name := range []string{"order_key", "line_number"} {
		attr := table.Attributes[schemaMutationAttributeIndex(table, name)]
		if !attr.Required {
			t.Fatalf("%s Required = false, want true", name)
		}
		if attr.ColumnID {
			t.Fatalf("%s ColumnID = true for compound primary key, want false", name)
		}
	}
	if _, err := table.GetAttribute(shared.CompoundPrimaryKeyAuthorityFieldName); err != nil {
		t.Fatalf("compound authority attribute missing: %v", err)
	}
}

func TestLegacySchemaMutationHandleAlterTableAddForeignKeyReportsExplicitUnsupported(t *testing.T) {
	handle := LegacyQuantaSessionHandle{
		TableName: "orders",
		Session:   &core.Session{BasePath: t.TempDir()},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationAlterTableAddForeignKey,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "orders"},
			Columns: []qsbridge.FieldRef{
				{Name: "o_custkey"},
			},
			Relationships: []qsbridge.RelationshipDefinition{
				{
					Name:      "fk_orders_customer",
					FromTable: "orders",
					FromField: "o_custkey",
					ToTable:   "customer",
					ToField:   "c_custkey",
					Direction: qsbridge.JoinChildToParent,
				},
			},
			ValidationSteps: []qsbridge.MutationValidationStep{
				{Kind: qsbridge.MutationValidationForeignKeyParentKeyCheck},
				{Kind: qsbridge.MutationValidationForeignKeyTypeCompatibility},
				{Kind: qsbridge.MutationValidationForeignKeyOrphanScan},
			},
		},
	}

	_, diagnostics, err := handle.AlterTableAddForeignKey(context.Background(), request)
	if err != nil {
		t.Fatalf("AlterTableAddForeignKey() error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported mutation blocker", diagnostics)
	}
	if len(diagnostics) == 0 || diagnostics[0].Code != qsbridge.DiagnosticUnsupportedMutation || !strings.Contains(diagnostics[0].Error(), "ALTER TABLE ADD FOREIGN KEY is not implemented yet") || !strings.Contains(diagnostics[0].Error(), "foreign_key_parent_key_check, foreign_key_type_compatibility, foreign_key_orphan_scan") {
		t.Fatalf("diagnostics = %#v, want explicit unsupported ALTER TABLE ADD FOREIGN KEY", diagnostics)
	}
}

func TestLegacySchemaMutationHandleDropTableRejectsActiveDependentView(t *testing.T) {
	configDir := t.TempDir()
	now := testSchemaMutationTime()
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customer", now); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	if err := shared.SaveViewDefinition(configDir, shared.ViewDefinition{
		SchemaName: "quanta",
		ViewName:   "customer_projection",
		SQL:        "select c_custkey from customer",
		Dependencies: []shared.ViewDependency{
			{SchemaName: "quanta", ObjectName: "customer", ObjectType: shared.CatalogObjectTypeTable},
		},
		CreationDate:     now,
		ModificationDate: now,
	}); err != nil {
		t.Fatalf("SaveViewDefinition() error = %v", err)
	}
	if err := shared.ActivateCatalogView(configDir, "quanta", "customer_projection", now); err != nil {
		t.Fatalf("ActivateCatalogView() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customer",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:     qsbridge.MutationDropTable,
			Target:   qsbridge.TableInstance{Schema: "quanta", Table: "customer"},
			IfExists: true,
		},
	}

	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want dependent-view error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}
	if got := err.Error(); got != "cannot drop table referenced by views: customer_projection" {
		t.Fatalf("error = %q, want dependent-view error", got)
	}
}

func TestLegacySchemaMutationHandleDropTableRejectsActiveChildTable(t *testing.T) {
	configDir := t.TempDir()
	now := testSchemaMutationTime()
	writeSchemaMutationCatalogSchema(t, configDir, "customers", "")
	writeSchemaMutationCatalogSchema(t, configDir, "orders", "customers.id")
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customers", now); err != nil {
		t.Fatalf("ActivateCatalogTable(customers) error = %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", "orders", now); err != nil {
		t.Fatalf("ActivateCatalogTable(orders) error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customers",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:     qsbridge.MutationDropTable,
			Target:   qsbridge.TableInstance{Schema: "quanta", Table: "customers"},
			IfExists: true,
		},
	}

	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want child-table dependency error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}
	if got := err.Error(); got != "cannot drop table with dependencies: orders" {
		t.Fatalf("error = %q, want child-table dependency error", got)
	}
}

func TestLegacySchemaMutationHandleCreateViewRejectsTableNameCollision(t *testing.T) {
	configDir := t.TempDir()
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customer_names", testSchemaMutationTime()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customer_names",
		Session:   &core.Session{BasePath: configDir},
	}

	_, diagnostics, err := handle.CreateView(context.Background(), ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:    qsbridge.MutationCreateView,
			Target:  qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
			ViewSQL: "select c_custkey from customer",
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("CreateView() diagnostics = %#v", diagnostics)
	}
	if err == nil {
		t.Fatalf("CreateView() should reject an active table with the same name")
	}
}

func TestCreateTableAsSelectSchemaFromMutationUsesConservativeMapperDefaults(t *testing.T) {
	table, diagnostics := createTableAsSelectSchemaFromMutation("scratch_customer", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{
			{
				Name:       "id",
				Type:       qsbridge.DataTypeInt,
				PrimaryKey: true,
			},
			{
				Name:     "name",
				Type:     qsbridge.DataTypeString,
				Nullable: true,
				Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{MaxLength: 25}),
			},
			{
				Name:     "revenue",
				Type:     qsbridge.DataTypeFloat,
				Nullable: true,
				Encoding: qsbridge.LegacyEncodingProfile("FloatScaleBSI", qsbridge.LegacyEncodingOptions{Scale: 4}),
			},
			{
				Name:     "created_at",
				Type:     qsbridge.DataTypeTime,
				Nullable: true,
				Encoding: qsbridge.LegacyEncodingProfile("TimestampBSI", qsbridge.LegacyEncodingOptions{}),
			},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("schema diagnostics = %#v", diagnostics)
	}
	if table.Name != "scratch_customer" || table.PrimaryKey != "id" {
		t.Fatalf("table = %#v, want scratch_customer primary key id", table)
	}
	if got := table.Attributes[0]; got.MappingStrategy != "IntBSI" || !got.ColumnID || !got.Required {
		t.Fatalf("id attr = %#v, want required IntBSI primary key", got)
	}
	if got := table.Attributes[1]; got.MappingStrategy != "StringEnum" || got.Size != 25 {
		t.Fatalf("name attr = %#v, want StringEnum with source max length", got)
	}
	if got := table.Attributes[2]; got.MappingStrategy != "FloatScaleBSI" || got.Scale != 4 {
		t.Fatalf("revenue attr = %#v, want FloatScaleBSI scale 4", got)
	}
	if got := table.Attributes[3]; got.MappingStrategy != "TimestampBSI" || got.MapperConfig["granularity"] != "millisecond" {
		t.Fatalf("created_at attr = %#v, want TimestampBSI millisecond", got)
	}
}

func TestCreateTableAsSelectSchemaFromMutationKeepsKeylessTableHeapLike(t *testing.T) {
	table, diagnostics := createTableAsSelectSchemaFromMutation("scratch_metrics", qsbridge.MutationShape{
		Columns: []qsbridge.FieldRef{
			{
				Name:     "revenue",
				Type:     qsbridge.DataTypeFloat,
				Nullable: true,
				Encoding: qsbridge.LegacyEncodingProfile("FloatScaleBSI", qsbridge.LegacyEncodingOptions{Scale: 2}),
			},
			{
				Name:     "market_segment",
				Type:     qsbridge.DataTypeString,
				Nullable: true,
				Encoding: qsbridge.LegacyEncodingProfile("StringEnum", qsbridge.LegacyEncodingOptions{}),
			},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("schema diagnostics = %#v", diagnostics)
	}
	if table.Name != "scratch_metrics" || table.PrimaryKey != "" {
		t.Fatalf("table = %#v, want scratch_metrics without primary key", table)
	}
	for _, attr := range table.Attributes {
		if attr.ColumnID {
			t.Fatalf("attribute = %#v, keyless CTAS should not expose a synthetic ColumnID", attr)
		}
	}
}

func testSchemaMutationTime() time.Time {
	return time.Date(2026, 8, 16, 11, 0, 0, 0, time.UTC)
}

func writeSchemaMutationCatalogSchema(t *testing.T, configDir string, table string, foreignKey string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	fkLine := ""
	if foreignKey != "" {
		fkLine = "  foreignKey: " + foreignKey + "\n"
	}
	schema := "tableName: " + table + `
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: IntBSI
  type: Integer
` + fkLine
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}

func writeSchemaMutationKeylessCatalogSchema(t *testing.T, configDir string, table string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	schema := "tableName: " + table + `
attributes:
- fieldName: order_key
  sourceName: /order_key
  mappingStrategy: IntBSI
  type: Integer
- fieldName: customer_key
  sourceName: /customer_key
  mappingStrategy: IntBSI
  type: Integer
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}
