package qsruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	"github.com/hashicorp/consul/api"
)

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
