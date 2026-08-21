package qsruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
)

const alterTableAddPrimaryKeyCatalogOnlyEnv = "QUANTASTREAM_SCHEMA_MUTATION_ADD_PRIMARY_KEY_CATALOG_ONLY"

// NewLegacySchemaMutationHandle creates a short-lived handle for schema mutations
// that must read YAML before the target table is active in the runtime catalog.
func NewLegacySchemaMutationHandle(quantaSource *source.QuantaSource, tableName, schemaDir string) (LegacyQuantaSessionHandle, error) {
	if quantaSource == nil || quantaSource.GetConnection() == nil {
		return LegacyQuantaSessionHandle{}, fmt.Errorf("schema mutation source is not initialized")
	}
	conn := quantaSource.GetConnection()
	session := &core.Session{
		BasePath:     strings.TrimSpace(schemaDir),
		BitIndex:     shared.NewBitmapIndex(conn),
		KVStore:      shared.NewKVStore(conn),
		StringIndex:  shared.NewStringSearch(conn, 1000),
		TableBuffers: map[string]*core.TableBuffer{},
		CreatedAt:    time.Now().UTC(),
	}
	return LegacyQuantaSessionHandle{
		TableName: strings.TrimSpace(tableName),
		Pool:      quantaSource.GetSessionPool(),
		Session:   session,
		Synthetic: true,
	}, nil
}

// CreateTable activates and deploys a YAML-backed table schema.
func (h LegacyQuantaSessionHandle) CreateTable(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationCreateTable {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "create table called for non-CREATE mutation"),
		}, nil
	}
	tableName, schemaName, diagnostics := h.schemaMutationTarget(request, "create table")
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if strings.TrimSpace(request.Mutation.SourceSQL) != "" {
		if consul := h.schemaMutationConsul(); consul != nil {
			return h.createConsulCatalogTableAsSelect(ctx, consul, schemaName, tableName, request.Mutation)
		}
		return h.createFileCatalogTableAsSelect(ctx, schemaName, tableName, request.Mutation)
	}
	if consul := h.schemaMutationConsul(); consul != nil {
		return h.createConsulCatalogTable(ctx, consul, schemaName, tableName)
	}
	return h.createFileCatalogTable(ctx, schemaName, tableName)
}

// DropTable deactivates a table schema and drops its bitmap-backed data.
func (h LegacyQuantaSessionHandle) DropTable(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationDropTable {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "drop table called for non-DROP mutation"),
		}, nil
	}
	tableName, schemaName, diagnostics := h.schemaMutationTarget(request, "drop table")
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if consul := h.schemaMutationConsul(); consul != nil {
		return h.dropConsulCatalogTable(ctx, consul, schemaName, tableName, request.Mutation.IfExists)
	}
	return h.dropFileCatalogTable(ctx, schemaName, tableName, request.Mutation.IfExists)
}

// AlterTableAddPrimaryKey reserves the default parser/binder/runtime path for
// future validation and artifact work. A guarded catalog-only scaffold lets
// tests exercise schema metadata writes before full PK authority rebuilds exist.
func (h LegacyQuantaSessionHandle) AlterTableAddPrimaryKey(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationAlterTableAddPrimaryKey {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "alter table add primary key called for non-ALTER mutation"),
		}, nil
	}
	tableName, schemaName, diagnostics := h.schemaMutationTarget(request, "alter table add primary key")
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	validationSteps := schemaMutationValidationStepKinds(request.Mutation.ValidationSteps)
	if validationSteps == "" {
		validationSteps = "primary_key_null_scan, primary_key_duplicate_scan"
	}
	if !alterTableAddPrimaryKeyCatalogOnlyEnabled() {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY is not implemented yet; required validation steps: "+validationSteps+"; QS must validate existing rows and build primary-key authority artifacts before enabling it"),
		}, nil
	}
	if diagnostics := validateAlterTableAddPrimaryKeyCatalogOnlyScaffold(request.Mutation); diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if consul := h.schemaMutationConsul(); consul != nil {
		return h.alterConsulCatalogTableAddPrimaryKey(ctx, consul, schemaName, tableName, request.Mutation)
	}
	return h.alterFileCatalogTableAddPrimaryKey(ctx, schemaName, tableName, request.Mutation)
}

func alterTableAddPrimaryKeyCatalogOnlyEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(alterTableAddPrimaryKeyCatalogOnlyEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func validateAlterTableAddPrimaryKeyCatalogOnlyScaffold(mutation qsbridge.MutationShape) qsbridge.DiagnosticSet {
	if len(mutation.Columns) == 0 {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY catalog scaffold requires resolved key columns"),
		}
	}
	required := map[qsbridge.MutationValidationKind]bool{
		qsbridge.MutationValidationPrimaryKeyNullScan:      false,
		qsbridge.MutationValidationPrimaryKeyDuplicateScan: false,
	}
	for _, step := range mutation.ValidationSteps {
		if _, ok := required[step.Kind]; ok {
			required[step.Kind] = true
		}
	}
	missing := make([]string, 0, len(required))
	if !required[qsbridge.MutationValidationPrimaryKeyNullScan] {
		missing = append(missing, string(qsbridge.MutationValidationPrimaryKeyNullScan))
	}
	if !required[qsbridge.MutationValidationPrimaryKeyDuplicateScan] {
		missing = append(missing, string(qsbridge.MutationValidationPrimaryKeyDuplicateScan))
	}
	if len(missing) == 0 {
		return nil
	}
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY catalog scaffold requires modeled validation steps before metadata can be written: "+strings.Join(missing, ", ")),
	}
}

func (h LegacyQuantaSessionHandle) alterFileCatalogTableAddPrimaryKey(ctx context.Context, schemaName, tableName string, mutation qsbridge.MutationShape) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := strings.TrimSpace(h.Session.BasePath)
	if configDir == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY requires a file-backed schema directory"),
		}, nil
	}
	active, err := shared.CatalogTableActive(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !active {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s doesn't exist", tableName)
	}
	table, err := shared.LoadSchema(configDir, tableName, nil)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("load table schema %s: %w", tableName, err)
	}
	validation, diagnostics, err := h.validateAlterTableAddPrimaryKeyCatalogOnly(ctx, tableName, mutation)
	if diagnostics.BlocksNative() || err != nil {
		return qsbridge.StatementResult{}, diagnostics, err
	}
	if diagnostics, err := applyAlterTableAddPrimaryKeyCatalogMutation(table, mutation); diagnostics.BlocksNative() || err != nil {
		return qsbridge.StatementResult{}, diagnostics, err
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := saveCreateTableAsSelectSchema(configDir, table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.ActivateCatalogTable(configDir, schemaName, tableName, time.Now().UTC()); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	return qsbridge.StatementResult{Status: alterTableAddPrimaryKeyCatalogOnlyStatus(tableName, validation.RowCount, validation.RowCountKnown)}, nil, nil
}

func (h LegacyQuantaSessionHandle) alterConsulCatalogTableAddPrimaryKey(ctx context.Context, consul *api.Client, schemaName, tableName string, mutation qsbridge.MutationShape) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	exists, err := shared.TableExists(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !exists {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s doesn't exist", tableName)
	}
	table, err := shared.LoadSchema("", tableName, consul)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("load active schema from consul: %w", err)
	}
	validation, diagnostics, err := h.validateAlterTableAddPrimaryKeyCatalogOnly(ctx, tableName, mutation)
	if diagnostics.BlocksNative() || err != nil {
		return qsbridge.StatementResult{}, diagnostics, err
	}
	if diagnostics, err := applyAlterTableAddPrimaryKeyCatalogMutation(table, mutation); diagnostics.BlocksNative() || err != nil {
		return qsbridge.StatementResult{}, diagnostics, err
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.DeleteTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.UpdateModTimeForTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.MarshalConsul(table, consul); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	_ = schemaName
	return qsbridge.StatementResult{Status: alterTableAddPrimaryKeyCatalogOnlyStatus(tableName, validation.RowCount, validation.RowCountKnown)}, nil, nil
}

type alterTableAddPrimaryKeyValidationResult struct {
	RowCount      uint64
	RowCountKnown bool
}

type alterTableAddPrimaryKeyValidationMode string

const (
	alterTableAddPrimaryKeyValidationNullScan      alterTableAddPrimaryKeyValidationMode = "pk_null_scan"
	alterTableAddPrimaryKeyValidationDuplicateScan alterTableAddPrimaryKeyValidationMode = "pk_duplicate_scan"
)

type alterTableAddPrimaryKeyValidationPlan struct {
	Mode    alterTableAddPrimaryKeyValidationMode
	Table   string
	Columns []string
}

func (h LegacyQuantaSessionHandle) validateAlterTableAddPrimaryKeyCatalogOnly(ctx context.Context, tableName string, mutation qsbridge.MutationShape) (alterTableAddPrimaryKeyValidationResult, qsbridge.DiagnosticSet, error) {
	plans, diagnostics := alterTableAddPrimaryKeyValidationPlans(tableName, mutation)
	if diagnostics.BlocksNative() {
		return alterTableAddPrimaryKeyValidationResult{}, diagnostics, nil
	}
	result, diagnostics, err := h.validateAlterTableAddPrimaryKeyEmptyTable(ctx, tableName, mutation)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	_ = plans
	return result, nil, nil
}

func alterTableAddPrimaryKeyValidationPlans(tableName string, mutation qsbridge.MutationShape) ([]alterTableAddPrimaryKeyValidationPlan, qsbridge.DiagnosticSet) {
	columns, diagnostics := alterTableAddPrimaryKeyValidationColumns(mutation)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	plans := make([]alterTableAddPrimaryKeyValidationPlan, 0, len(mutation.ValidationSteps))
	for _, step := range mutation.ValidationSteps {
		var mode alterTableAddPrimaryKeyValidationMode
		switch step.Kind {
		case qsbridge.MutationValidationPrimaryKeyNullScan:
			mode = alterTableAddPrimaryKeyValidationNullScan
		case qsbridge.MutationValidationPrimaryKeyDuplicateScan:
			mode = alterTableAddPrimaryKeyValidationDuplicateScan
		default:
			continue
		}
		planColumns := columns
		if len(step.Columns) > 0 {
			planColumns, diagnostics = alterTableAddPrimaryKeyValidationColumns(qsbridge.MutationShape{Columns: step.Columns})
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
		}
		plans = append(plans, alterTableAddPrimaryKeyValidationPlan{
			Mode:    mode,
			Table:   tableName,
			Columns: planColumns,
		})
	}
	return plans, nil
}

func alterTableAddPrimaryKeyValidationColumns(mutation qsbridge.MutationShape) ([]string, qsbridge.DiagnosticSet) {
	columns := make([]string, 0, len(mutation.Columns))
	seen := make(map[string]struct{}, len(mutation.Columns))
	for _, column := range mutation.Columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY validation plan has an empty column"),
			}
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY validation plan has duplicate column: "+name),
			}
		}
		seen[key] = struct{}{}
		columns = append(columns, name)
	}
	if len(columns) == 0 {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY validation plan requires columns"),
		}
	}
	return columns, nil
}

func (h LegacyQuantaSessionHandle) validateAlterTableAddPrimaryKeyEmptyTable(ctx context.Context, tableName string, mutation qsbridge.MutationShape) (alterTableAddPrimaryKeyValidationResult, qsbridge.DiagnosticSet, error) {
	_ = mutation
	count, known, diagnostics, err := h.alterTableAddPrimaryKeyCatalogOnlyRowCount(ctx, tableName)
	result := alterTableAddPrimaryKeyValidationResult{
		RowCount:      count,
		RowCountKnown: known,
	}
	if err != nil || diagnostics.BlocksNative() || !known || count == 0 {
		return result, diagnostics, err
	}
	return result, alterTableAddPrimaryKeyCatalogOnlyNonEmptyDiagnostic(tableName, count), nil
}

func alterTableAddPrimaryKeyCatalogOnlyStatus(tableName string, rowCount uint64, rowCountKnown bool) string {
	status := fmt.Sprintf("Primary key added to table %s", tableName)
	if rowCountKnown {
		status += fmt.Sprintf(" (catalog-only row_count=%d)", rowCount)
	}
	return status
}

func alterTableAddPrimaryKeyCatalogOnlyNonEmptyDiagnostic(tableName string, count uint64) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, fmt.Sprintf("ALTER TABLE ADD PRIMARY KEY catalog-only mode cannot promote table %s with %d existing row(s); QS must run null and duplicate validation scans before updating primary-key metadata", tableName, count)),
	}
}

func (h LegacyQuantaSessionHandle) alterTableAddPrimaryKeyCatalogOnlyRowCount(ctx context.Context, tableName string) (uint64, bool, qsbridge.DiagnosticSet, error) {
	if err := ctx.Err(); err != nil {
		return 0, true, nil, err
	}
	if h.Session == nil || h.Session.BitIndex == nil {
		return 0, false, nil, nil
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{tableName}
	if h.cachedRootTable(request) == nil {
		return 0, false, nil, nil
	}
	result, diagnostics, err := h.QueryBitmapCountOnly(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return 0, true, diagnostics, err
	}
	return result.Count, true, nil, nil
}

func applyAlterTableAddPrimaryKeyCatalogMutation(table *shared.BasicTable, mutation qsbridge.MutationShape) (qsbridge.DiagnosticSet, error) {
	if table == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY table schema is nil"),
		}, nil
	}
	if strings.TrimSpace(table.PrimaryKey) != "" {
		return nil, fmt.Errorf("table %s already has a primary key", table.Name)
	}
	keyNames := make([]string, 0, len(mutation.Columns))
	keyIndexes := make(map[int]struct{}, len(mutation.Columns))
	seen := make(map[string]struct{}, len(mutation.Columns))
	for _, column := range mutation.Columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "ALTER TABLE ADD PRIMARY KEY resolved column name is empty"),
			}, nil
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate primary key column %s", name)
		}
		seen[key] = struct{}{}
		index := schemaMutationAttributeIndex(table, name)
		if index < 0 {
			return nil, fmt.Errorf("primary key column %s does not exist on table %s", name, table.Name)
		}
		keyNames = append(keyNames, strings.TrimSpace(table.Attributes[index].FieldName))
		keyIndexes[index] = struct{}{}
	}
	table.PrimaryKey = strings.Join(keyNames, "+")
	for index := range table.Attributes {
		_, keyColumn := keyIndexes[index]
		table.Attributes[index].Required = table.Attributes[index].Required || keyColumn
		if len(keyIndexes) == 1 {
			table.Attributes[index].ColumnID = keyColumn
		}
	}
	shared.EnsureCompoundPrimaryKeyAuthorityAttribute(table)
	return nil, nil
}

func schemaMutationAttributeIndex(table *shared.BasicTable, columnName string) int {
	if table == nil {
		return -1
	}
	columnName = strings.TrimSpace(columnName)
	for i := range table.Attributes {
		attr := table.Attributes[i]
		if strings.EqualFold(strings.TrimSpace(attr.FieldName), columnName) {
			return i
		}
		if strings.TrimSpace(attr.FieldName) == "" && strings.EqualFold(strings.TrimSpace(attr.SourceName), columnName) {
			return i
		}
	}
	return -1
}

func schemaMutationValidationStepKinds(steps []qsbridge.MutationValidationStep) string {
	if len(steps) == 0 {
		return ""
	}
	kinds := make([]string, 0, len(steps))
	for _, step := range steps {
		if step.Kind == "" {
			continue
		}
		kinds = append(kinds, string(step.Kind))
	}
	return strings.Join(kinds, ", ")
}

// AlterTableAddForeignKey reserves the parser/binder/runtime path for future
// relationship catalog and artifact work without mutating table schemas yet.
func (h LegacyQuantaSessionHandle) AlterTableAddForeignKey(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationAlterTableAddForeignKey {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "alter table add foreign key called for non-ALTER mutation"),
		}, nil
	}
	if _, _, diagnostics := h.schemaMutationTarget(request, "alter table add foreign key"); diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	validationSteps := schemaMutationValidationStepKinds(request.Mutation.ValidationSteps)
	if validationSteps == "" {
		validationSteps = "foreign_key_parent_key_check, foreign_key_type_compatibility, foreign_key_orphan_scan"
	}
	return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "ALTER TABLE ADD FOREIGN KEY is not implemented yet; required validation steps: "+validationSteps+"; QS must validate existing rows and update relationship/catalog artifacts before enabling it"),
	}, nil
}

// CreateView persists a logical, non-materialized SQL view definition.
func (h LegacyQuantaSessionHandle) CreateView(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationCreateView {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "create view called for non-CREATE VIEW mutation"),
		}, nil
	}
	viewName, schemaName, diagnostics := h.schemaMutationTarget(request, "create view")
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if consul := h.schemaMutationConsul(); consul != nil {
		return h.createConsulCatalogView(ctx, consul, schemaName, viewName, request)
	}
	return h.createFileCatalogView(ctx, schemaName, viewName, request)
}

// DropView removes a logical, non-materialized SQL view definition.
func (h LegacyQuantaSessionHandle) DropView(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if request.Mutation.Kind != qsbridge.MutationDropView {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedMutation, qsbridge.PhaseExecute, "drop view called for non-DROP VIEW mutation"),
		}, nil
	}
	viewName, schemaName, diagnostics := h.schemaMutationTarget(request, "drop view")
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if consul := h.schemaMutationConsul(); consul != nil {
		return h.dropConsulCatalogView(ctx, consul, schemaName, viewName, request.Mutation.IfExists, request.Mutation.Cascade)
	}
	return h.dropFileCatalogView(ctx, schemaName, viewName, request.Mutation.IfExists, request.Mutation.Cascade)
}

func (h LegacyQuantaSessionHandle) createFileCatalogTableAsSelect(ctx context.Context, schemaName, tableName string, mutation qsbridge.MutationShape) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := strings.TrimSpace(h.Session.BasePath)
	if configDir == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT requires a file-backed schema directory"),
		}, nil
	}
	tableActive, err := shared.CatalogTableActive(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if tableActive {
		if mutation.IfNotExists {
			return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s already exists", tableName)}, nil, nil
		}
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s already exists", tableName)
	}
	viewActive, err := shared.CatalogViewActive(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if viewActive {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create table %s because an active view with that name exists", tableName)
	}
	table, diagnostics := createTableAsSelectSchemaFromMutation(tableName, mutation)
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := saveCreateTableAsSelectSchema(configDir, table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.ActivateCatalogTable(configDir, schemaName, tableName, time.Now().UTC()); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "deploy"); err != nil {
		_ = shared.RemoveCatalogTable(configDir, schemaName, tableName)
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s created", tableName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) createFileCatalogTable(ctx context.Context, schemaName, tableName string) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := h.Session.BasePath
	table, err := shared.LoadSchema(configDir, tableName, nil)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("load table schema %s: %w", tableName, err)
	}
	if !strings.EqualFold(table.Name, tableName) {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("schema tableName %q does not match CREATE TABLE target %q", table.Name, tableName)
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	ok, err := shared.CheckParentRelationInCatalog(configDir, schemaName, table)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !ok {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create table due to missing parent FK constraint dependency")
	}
	if err := shared.ActivateCatalogTable(configDir, schemaName, tableName, time.Now().UTC()); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "deploy"); err != nil {
		_ = shared.RemoveCatalogTable(configDir, schemaName, tableName)
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	return qsbridge.StatementResult{
		Status: fmt.Sprintf("Table %s created", tableName),
	}, nil, nil
}

func (h LegacyQuantaSessionHandle) dropFileCatalogTable(ctx context.Context, schemaName, tableName string, ifExists bool) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := h.Session.BasePath
	active, err := shared.CatalogTableActive(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !active {
		if ifExists {
			return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s dropped", tableName)}, nil, nil
		}
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s doesn't exist", tableName)
	}
	viewDependencies, err := shared.CheckViewDependenciesInCatalog(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if len(viewDependencies) > 0 {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot drop table referenced by views: %s", strings.Join(viewDependencies, ", "))
	}
	dependencies, err := shared.CheckChildRelationInCatalog(configDir, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if len(dependencies) > 0 {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot drop table with dependencies: %s", strings.Join(dependencies, ", "))
	}
	if err := shared.RemoveCatalogTable(configDir, schemaName, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "drop"); err != nil {
		_ = shared.ActivateCatalogTable(configDir, schemaName, tableName, time.Now().UTC())
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationDictionaries(schemaName, tableName)
	h.invalidateSchemaMutationTable(tableName)
	return qsbridge.StatementResult{
		Status: fmt.Sprintf("Table %s dropped", tableName),
	}, nil, nil
}

func (h LegacyQuantaSessionHandle) createFileCatalogView(ctx context.Context, schemaName, viewName string, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := h.Session.BasePath
	if strings.TrimSpace(request.Mutation.ViewSQL) == "" {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("CREATE VIEW %s has no SELECT definition", viewName)
	}
	tableActive, err := shared.CatalogTableActive(configDir, schemaName, viewName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if tableActive {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create view %s because an active table with that name exists", viewName)
	}
	viewActive, err := shared.CatalogViewActive(configDir, schemaName, viewName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if viewActive && !request.Mutation.Replace {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("view %s already exists", viewName)
	}
	now := time.Now().UTC()
	creation := now
	if viewActive {
		if existing, err := shared.LoadViewDefinition(configDir, viewName); err == nil && !existing.CreationDate.IsZero() {
			creation = existing.CreationDate
		}
	}
	view := h.viewDefinitionFromMutation(schemaName, viewName, request.Mutation, creation, now)
	if err := shared.SaveViewDefinition(configDir, view); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.ActivateCatalogView(configDir, schemaName, viewName, now); err != nil {
		_ = shared.RemoveViewDefinition(configDir, viewName)
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{Status: fmt.Sprintf("View %s created", viewName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) dropFileCatalogView(ctx context.Context, schemaName, viewName string, ifExists bool, cascade bool) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	configDir := h.Session.BasePath
	if err := dropFileCatalogViewRecursive(ctx, configDir, schemaName, viewName, ifExists, cascade, map[string]struct{}{}); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{Status: fmt.Sprintf("View %s dropped", viewName)}, nil, nil
}

func dropFileCatalogViewRecursive(ctx context.Context, configDir, schemaName, viewName string, ifExists bool, cascade bool, visited map[string]struct{}) error {
	key := strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(viewName))
	if _, ok := visited[key]; ok {
		return nil
	}
	visited[key] = struct{}{}
	active, err := shared.CatalogViewActive(configDir, schemaName, viewName)
	if err != nil {
		return err
	}
	if !active {
		if ifExists {
			return nil
		}
		return fmt.Errorf("view %s doesn't exist", viewName)
	}
	if cascade {
		dependencies, err := shared.CheckViewDependenciesByObjectInCatalog(configDir, schemaName, viewName, shared.CatalogObjectTypeView)
		if err != nil {
			return err
		}
		for _, dependency := range dependencies {
			if strings.EqualFold(strings.TrimSpace(dependency), strings.TrimSpace(viewName)) {
				continue
			}
			if err := dropFileCatalogViewRecursive(ctx, configDir, schemaName, dependency, true, true, visited); err != nil {
				return err
			}
		}
	}
	if err := shared.RemoveViewDefinition(configDir, viewName); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := shared.RemoveCatalogView(configDir, schemaName, viewName); err != nil {
		return err
	}
	return nil
}

func (h LegacyQuantaSessionHandle) createConsulCatalogTable(ctx context.Context, consul *api.Client, schemaName, tableName string) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	table, err := shared.LoadSchema(h.Session.BasePath, tableName, consul)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("load table schema %s: %w", tableName, err)
	}
	if !strings.EqualFold(table.Name, tableName) {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("schema tableName %q does not match CREATE TABLE target %q", table.Name, tableName)
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	exists, err := shared.TableExists(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if exists {
		deployed, err := shared.LoadSchema("", tableName, consul)
		if err != nil {
			return qsbridge.StatementResult{}, nil, fmt.Errorf("load active schema from consul: %w", err)
		}
		ok, warnings, err := deployed.Compare(table)
		if err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
		if !ok {
			return qsbridge.StatementResult{}, nil, fmt.Errorf("active schema differs from YAML for %s: %s", tableName, strings.Join(warnings, "; "))
		}
		return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s already exists", tableName)}, nil, nil
	}
	ok, err := shared.CheckParentRelation(consul, table)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !ok {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create table due to missing parent FK constraint dependency")
	}
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.DeleteTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.UpdateModTimeForTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.MarshalConsul(table, consul); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	deployed, err := shared.LoadSchema("", tableName, consul)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("verify active schema from consul: %w", err)
	}
	ok, warnings, err := deployed.Compare(table)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !ok {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("active schema verification failed for %s: %s", tableName, strings.Join(warnings, "; "))
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "deploy"); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	_ = schemaName
	return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s created", tableName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) createConsulCatalogTableAsSelect(ctx context.Context, consul *api.Client, schemaName, tableName string, mutation qsbridge.MutationShape) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	exists, err := shared.TableExists(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if exists {
		if mutation.IfNotExists {
			return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s already exists", tableName)}, nil, nil
		}
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s already exists", tableName)
	}
	viewExists, err := shared.ViewExists(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if viewExists {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create table %s because an active view with that name exists", tableName)
	}
	table, diagnostics := createTableAsSelectSchemaFromMutation(tableName, mutation)
	if diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	if err := shared.ValidateCatalogTableDefinition(table); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.DeleteTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.UpdateModTimeForTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.MarshalConsul(table, consul); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	deployed, err := shared.LoadSchema("", tableName, consul)
	if err != nil {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("verify active schema from consul: %w", err)
	}
	ok, warnings, err := deployed.Compare(table)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !ok {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("active schema verification failed for %s: %s", tableName, strings.Join(warnings, "; "))
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "deploy"); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	h.invalidateSchemaMutationTable(tableName)
	_ = schemaName
	return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s created", tableName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) dropConsulCatalogTable(ctx context.Context, consul *api.Client, schemaName, tableName string, ifExists bool) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "schema mutation session is not initialized"),
		}, nil
	}
	exists, err := shared.TableExists(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if !exists {
		if ifExists {
			return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s dropped", tableName)}, nil, nil
		}
		return qsbridge.StatementResult{}, nil, fmt.Errorf("table %s doesn't exist", tableName)
	}
	viewDependencies, err := shared.CheckViewDependencies(consul, schemaName, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if len(viewDependencies) > 0 {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot drop table referenced by views: %s", strings.Join(viewDependencies, ", "))
	}
	dependencies, err := shared.CheckChildRelation(consul, tableName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if len(dependencies) > 0 {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot drop table with dependencies: %s", strings.Join(dependencies, ", "))
	}
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := shared.DeleteTable(consul, tableName); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "drop"); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if h.Session.KVStore != nil && h.Session.KVStore.Conn != nil && h.Session.KVStore.Conn.ServicePort != 0 {
		if err := h.Session.KVStore.DeleteIndicesWithPrefix(tableName, false); err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
	}
	h.invalidateSchemaMutationDictionaries(schemaName, tableName)
	h.invalidateSchemaMutationTable(tableName)
	return qsbridge.StatementResult{Status: fmt.Sprintf("Table %s dropped", tableName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) createConsulCatalogView(ctx context.Context, consul *api.Client, schemaName, viewName string, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if strings.TrimSpace(request.Mutation.ViewSQL) == "" {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("CREATE VIEW %s has no SELECT definition", viewName)
	}
	tableExists, err := shared.TableExists(consul, viewName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if tableExists {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("cannot create view %s because an active table with that name exists", viewName)
	}
	viewExists, err := shared.ViewExists(consul, viewName)
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if viewExists && !request.Mutation.Replace {
		return qsbridge.StatementResult{}, nil, fmt.Errorf("view %s already exists", viewName)
	}
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	now := time.Now().UTC()
	creation := now
	if viewExists {
		if existing, err := shared.LoadViewDefinitionConsul(consul, viewName); err == nil && !existing.CreationDate.IsZero() {
			creation = existing.CreationDate
		}
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	view := h.viewDefinitionFromMutation(schemaName, viewName, request.Mutation, creation, now)
	if err := shared.MarshalViewConsul(view, consul); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{Status: fmt.Sprintf("View %s created", viewName)}, nil, nil
}

func (h LegacyQuantaSessionHandle) dropConsulCatalogView(ctx context.Context, consul *api.Client, schemaName, viewName string, ifExists bool, cascade bool) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	lock, err := shared.Lock(consul, "admin-tool", "query-engine")
	if err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	defer shared.Unlock(consul, lock)
	if err := dropConsulCatalogViewRecursive(ctx, consul, schemaName, viewName, ifExists, cascade, map[string]struct{}{}); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{Status: fmt.Sprintf("View %s dropped", viewName)}, nil, nil
}

func dropConsulCatalogViewRecursive(ctx context.Context, consul *api.Client, schemaName, viewName string, ifExists bool, cascade bool, visited map[string]struct{}) error {
	key := strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(viewName))
	if _, ok := visited[key]; ok {
		return nil
	}
	visited[key] = struct{}{}
	viewExists, err := shared.ViewExists(consul, viewName)
	if err != nil {
		return err
	}
	if !viewExists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("view %s doesn't exist", viewName)
	}
	if cascade {
		dependencies, err := shared.CheckViewDependenciesByObject(consul, schemaName, viewName, shared.CatalogObjectTypeView)
		if err != nil {
			return err
		}
		for _, dependency := range dependencies {
			if strings.EqualFold(strings.TrimSpace(dependency), strings.TrimSpace(viewName)) {
				continue
			}
			if err := dropConsulCatalogViewRecursive(ctx, consul, schemaName, dependency, true, true, visited); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := shared.DeleteView(consul, viewName); err != nil {
		return err
	}
	return nil
}

func (h LegacyQuantaSessionHandle) viewDefinitionFromMutation(schemaName, viewName string, mutation qsbridge.MutationShape, creation, modification time.Time) shared.ViewDefinition {
	dependencies := make([]shared.ViewDependency, 0, len(mutation.ViewDependencies))
	for _, dependency := range mutation.ViewDependencies {
		objectName := strings.TrimSpace(dependency.Table)
		if objectName == "" {
			continue
		}
		objectSchema := strings.TrimSpace(dependency.Schema)
		if objectSchema == "" {
			objectSchema = schemaName
		}
		objectType := shared.CatalogObjectTypeTable
		if strings.EqualFold(strings.TrimSpace(dependency.Role), shared.CatalogObjectTypeView) {
			objectType = shared.CatalogObjectTypeView
		}
		dependencies = append(dependencies, shared.ViewDependency{
			SchemaName: objectSchema,
			ObjectName: objectName,
			ObjectType: objectType,
		})
	}
	return shared.ViewDefinition{
		SchemaName:       schemaName,
		ViewName:         viewName,
		SQL:              strings.TrimSpace(mutation.ViewSQL),
		CanonicalSQL:     strings.TrimSpace(mutation.ViewSQL),
		Dependencies:     dependencies,
		CreationDate:     creation,
		ModificationDate: modification,
	}
}

func createTableAsSelectSchemaFromMutation(tableName string, mutation qsbridge.MutationShape) (*shared.BasicTable, qsbridge.DiagnosticSet) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT target table is empty"),
		}
	}
	if len(mutation.Columns) == 0 {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT requires at least one column"),
		}
	}
	table := &shared.BasicTable{
		Name:       tableName,
		Selector:   `type="` + tableName + `"`,
		Attributes: make([]shared.BasicAttribute, 0, len(mutation.Columns)),
	}
	seen := make(map[string]struct{}, len(mutation.Columns))
	for index, column := range mutation.Columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "CREATE TABLE AS SELECT column name is empty"),
			}
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, fmt.Sprintf("Duplicate column name '%s'", name)),
			}
		}
		seen[key] = struct{}{}
		attr := createTableAsSelectAttribute(column, index+1)
		table.Attributes = append(table.Attributes, attr)
		if table.PrimaryKey == "" && column.PrimaryKey {
			table.PrimaryKey = name
		}
	}
	return table, nil
}

func createTableAsSelectAttribute(column qsbridge.FieldRef, ordinal int) shared.BasicAttribute {
	name := strings.TrimSpace(column.Name)
	attr := shared.BasicAttribute{
		FieldName:       name,
		SourceName:      name,
		Type:            createTableAsSelectSharedType(column.Type),
		MappingStrategy: createTableAsSelectMappingStrategy(column),
		SourceOrdinal:   ordinal,
		Required:        !column.Nullable,
		ColumnID:        column.PrimaryKey,
	}
	if column.Type == qsbridge.DataTypeFloat {
		scale := column.Encoding.Scale
		if scale <= 0 {
			scale = 6
		}
		attr.Scale = scale
	}
	if column.Type == qsbridge.DataTypeString {
		maxLen := column.Encoding.MaxLength
		if maxLen <= 0 {
			maxLen = 256
		}
		attr.Size = maxLen
	}
	if column.Type == qsbridge.DataTypeTime {
		attr.MapperConfig = map[string]string{"granularity": "millisecond"}
	}
	return attr
}

func createTableAsSelectSharedType(dataType qsbridge.DataType) string {
	switch dataType {
	case qsbridge.DataTypeBool:
		return "Boolean"
	case qsbridge.DataTypeFloat:
		return "Float"
	case qsbridge.DataTypeInt:
		return "Integer"
	case qsbridge.DataTypeTime:
		return "DateTime"
	case qsbridge.DataTypeString:
		return "String"
	default:
		return "String"
	}
}

func createTableAsSelectMappingStrategy(column qsbridge.FieldRef) string {
	switch column.Type {
	case qsbridge.DataTypeString, qsbridge.DataTypeBool:
		return "StringEnum"
	case qsbridge.DataTypeTime:
		return "SysMillisBSI"
	}
	legacy := strings.TrimSpace(column.Encoding.LegacyName)
	if legacy != "" {
		switch legacy {
		case "ParentRelation":
			return "IntBSI"
		default:
			return legacy
		}
	}
	switch column.Type {
	case qsbridge.DataTypeFloat:
		return "FloatScaleBSI"
	case qsbridge.DataTypeInt:
		return "IntBSI"
	default:
		return "StringEnum"
	}
}

func saveCreateTableAsSelectSchema(configDir string, table *shared.BasicTable) error {
	if table == nil {
		return fmt.Errorf("CREATE TABLE AS SELECT schema is nil")
	}
	tableDir := filepath.Join(configDir, strings.TrimSpace(table.Name))
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return err
	}
	data, err := yaml.Marshal(table)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(tableDir, "schema.yaml"), data, 0644)
}

func (h LegacyQuantaSessionHandle) schemaMutationTarget(request ExecutionRequest, operation string) (string, string, qsbridge.DiagnosticSet) {
	tableName := strings.TrimSpace(request.Mutation.Target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(h.TableName)
	}
	if tableName == "" {
		return "", "", qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, operation+" mutation has no target table"),
		}
	}
	schemaName := strings.TrimSpace(request.Mutation.Target.Schema)
	if schemaName == "" {
		schemaName = "quanta"
	}
	return tableName, schemaName, nil
}

func (h LegacyQuantaSessionHandle) schemaMutationConsul() *api.Client {
	if h.Session == nil || h.Session.KVStore == nil || h.Session.KVStore.Conn == nil {
		return nil
	}
	if h.Session.KVStore.Conn.ServicePort == 0 {
		return nil
	}
	return h.Session.KVStore.Conn.Consul
}

func (h LegacyQuantaSessionHandle) invalidateSchemaMutationTable(tableName string) {
	if h.Pool != nil {
		h.Pool.InvalidateTable(tableName)
	}
}

func (h LegacyQuantaSessionHandle) invalidateSchemaMutationDictionaries(schemaName, tableName string) {
	if h.DictionaryInvalidator.Dictionaries == nil {
		return
	}
	for _, attribute := range h.schemaMutationStringEnumAttributes(tableName) {
		fieldName := strings.TrimSpace(attribute.FieldName)
		if fieldName == "" {
			fieldName = strings.TrimSpace(attribute.SourceName)
		}
		if fieldName == "" {
			continue
		}
		h.DictionaryInvalidator.InvalidateValueChange(DictionaryValueChange{
			Schema: schemaName,
			Table:  tableName,
			Field:  fieldName,
		})
	}
}

func (h LegacyQuantaSessionHandle) schemaMutationStringEnumAttributes(tableName string) []core.Attribute {
	table := h.schemaMutationCachedTable(tableName)
	if table == nil {
		return nil
	}
	attributes := make([]core.Attribute, 0)
	for _, attribute := range table.Attributes {
		if qsbridge.LegacyEncodingProfile(attribute.MappingStrategy, qsbridge.LegacyEncodingOptions{}).Kind == qsbridge.EncodingStringEnum {
			attributes = append(attributes, attribute)
		}
	}
	return attributes
}

func (h LegacyQuantaSessionHandle) schemaMutationCachedTable(tableName string) *core.Table {
	if h.Session != nil {
		for name, buffer := range h.Session.TableBuffers {
			if buffer == nil || buffer.Table == nil {
				continue
			}
			if strings.EqualFold(name, tableName) || strings.EqualFold(buffer.Table.Name, tableName) {
				return buffer.Table
			}
		}
	}
	if h.Pool == nil || h.Pool.TableCache == nil {
		return nil
	}
	h.Pool.TableCache.TableCacheLock.RLock()
	defer h.Pool.TableCache.TableCacheLock.RUnlock()
	for name, table := range h.Pool.TableCache.TableCache {
		if table == nil {
			continue
		}
		if strings.EqualFold(name, tableName) || strings.EqualFold(table.Name, tableName) {
			return table
		}
	}
	return nil
}
