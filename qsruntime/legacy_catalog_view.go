package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// LegacyCatalogViewAdapter projects legacy table-cache metadata into qsbridge catalog views.
//
// This is compatibility glue: legacy cache objects may feed the refactor while
// old session and node code still exists, but the returned qsbridge views are
// the handoff contracts for new planner/runtime work.
type LegacyCatalogViewAdapter struct {
	Catalog LegacyTableCacheCatalog
}

// NodeCatalogView returns the node-facing physical catalog projection for named tables.
func (a LegacyCatalogViewAdapter) NodeCatalogView(schema string, tableNames ...string) (qsbridge.NodeCatalogView, qsbridge.DiagnosticSet) {
	tables, diagnostics := a.tableDefinitions(schema, tableNames...)
	if diagnostics.BlocksNative() {
		return qsbridge.NodeCatalogView{}, diagnostics
	}
	return qsbridge.NewNodeCatalogView(tables), nil
}

// QueryCatalogView returns the query-facing semantic catalog projection for named tables.
func (a LegacyCatalogViewAdapter) QueryCatalogView(schema string, tableNames ...string) (qsbridge.QueryCatalogView, qsbridge.DiagnosticSet) {
	tables, diagnostics := a.tableDefinitions(schema, tableNames...)
	if diagnostics.BlocksNative() {
		return qsbridge.QueryCatalogView{}, diagnostics
	}
	return qsbridge.NewQueryCatalogView(tables, nil, legacyCatalogViewFunctions(a.Catalog.Functions)), nil
}

// QueryCatalogViewForCachedTables returns a query-facing semantic catalog for
// every table currently resident in the legacy table cache.
func (a LegacyCatalogViewAdapter) QueryCatalogViewForCachedTables(schema string) (qsbridge.QueryCatalogView, qsbridge.DiagnosticSet) {
	cachedTables := a.Catalog.cachedTables()
	tables := make([]qsbridge.TableDefinition, 0, len(cachedTables))
	for _, table := range cachedTables {
		tables = append(tables, a.Catalog.tableDefinition(schema, table))
	}
	return qsbridge.NewQueryCatalogView(tables, nil, legacyCatalogViewFunctions(a.Catalog.Functions)), nil
}

func (a LegacyCatalogViewAdapter) tableDefinitions(schema string, tableNames ...string) ([]qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	tables := make([]qsbridge.TableDefinition, 0, len(tableNames))
	var diagnostics qsbridge.DiagnosticSet
	for _, tableName := range tableNames {
		table, tableDiagnostics := a.Catalog.Table(schema, tableName)
		if tableDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, tableDiagnostics...)
			continue
		}
		tables = append(tables, table)
	}
	return tables, diagnostics
}

func legacyCatalogViewFunctions(functions []qsbridge.FunctionDefinition) []qsbridge.FunctionDefinition {
	if len(functions) == 0 {
		return qsbridge.BuiltinSQLFunctionDefinitions()
	}
	return append([]qsbridge.FunctionDefinition(nil), functions...)
}
