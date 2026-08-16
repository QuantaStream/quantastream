package qsbridge

// PlanRequest describes one parser-to-plan request for the native SQL scaffold.
//
// It intentionally carries only parser, catalog, optimizer-audit, and physical
// scope inputs. Runtime execution, sessions, transactions, and wire-protocol
// concerns stay outside this boundary.
type PlanRequest struct {
	SQL            string
	DefaultSchema  string
	CatalogVersion CatalogVersion
	Session        SessionContext
	Scope          PhysicalScope
	Optimization   OptimizationTrace
}

// PlanResult is the stable envelope returned by the native planning facade.
type PlanResult struct {
	SQL            string
	DefaultSchema  string
	CatalogVersion CatalogVersion
	Session        SessionContext
	Scope          PhysicalScope
	Unbound        UnboundStatement
	Query          QueryIR
	Diagnostics    DiagnosticSet
	Logical        LogicalPlan
	Physical       PhysicalPlan
	Inspection     InspectionReport
	Supported      bool
}

// Planner composes parsing, catalog binding, planning, and inspection.
//
// The planner does not execute anything. It gives parser adapters, tests, and
// future protocol surfaces one small boundary that returns every planning
// artifact needed for explain, diagnostics, and executor selection.
type Planner struct {
	Parser         ParserBridge
	Catalog        Catalog
	DefaultSchema  string
	CatalogVersion CatalogVersion
	Session        SessionContext
	Scope          PhysicalScope
}

// Plan parses and plans SQL using the planner's default schema and scope.
func (p Planner) Plan(sql string) PlanResult {
	return p.PlanWithRequest(PlanRequest{SQL: sql})
}

// PlanWithRequest parses and plans SQL using request overrides when present.
func (p Planner) PlanWithRequest(request PlanRequest) PlanResult {
	request = p.withDefaults(request)
	result := PlanResult{
		SQL:            request.SQL,
		DefaultSchema:  request.DefaultSchema,
		CatalogVersion: request.CatalogVersion,
		Session:        request.Session.Clone(),
		Scope:          clonePhysicalScope(request.Scope),
	}

	if p.Parser == nil {
		result.Diagnostics = DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseParse, "parser bridge is nil"),
		}
		return result
	}

	unbound, parseDiagnostics := p.Parser.Parse(request.SQL)
	result.Unbound = unbound
	if result.SQL == "" {
		result.SQL = unbound.SQL
	}
	if parseDiagnostics.BlocksNative() {
		result.Diagnostics = parseDiagnostics
		return result
	}

	expanded, expansionDiagnostics := ExpandStatementViews(p.Catalog, p.Parser, request.DefaultSchema, unbound)
	result.Unbound = expanded
	if expansionDiagnostics.BlocksNative() {
		result.Diagnostics = mergeDiagnosticSets(parseDiagnostics, expansionDiagnostics)
		return result
	}
	unbound = expanded

	context := NewBindContext(p.Catalog, request.DefaultSchema)
	query, bindDiagnostics := unbound.Bind(context)
	result.Query = query
	queryDiagnostics := query.Diagnostics()
	result.Diagnostics = mergeDiagnosticSets(parseDiagnostics, bindDiagnostics, queryDiagnostics)
	if result.Diagnostics.BlocksNative() {
		return result
	}

	logical := BuildLogicalPlan(query)
	physical := BuildPhysicalPlan(logical, request.Scope)
	optimization := mergeOptimizationTraces(request.Optimization, AnalyzeOptimizationCandidates(query))
	inspection := InspectOptimizedQuery(query, optimization, request.Scope)

	result.Logical = logical
	result.Physical = physical
	result.Inspection = inspection
	result.Supported = inspection.Supported
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, inspection.Diagnostics)
	if result.Diagnostics.BlocksNative() {
		result.Supported = false
	}
	return result
}

func (p Planner) withDefaults(request PlanRequest) PlanRequest {
	if request.Session.ID == "" && request.Session.User == "" && request.Session.CurrentSchema == "" && len(request.Session.Roles) == 0 && len(request.Session.SQLModes) == 0 && len(request.Session.Variables) == 0 {
		request.Session = p.Session.Clone()
	} else {
		request.Session = request.Session.Clone()
	}
	if request.DefaultSchema == "" {
		request.DefaultSchema = request.Session.EffectiveSchema(p.DefaultSchema)
	}
	if request.Scope.Unscoped() {
		request.Scope = p.Scope
	}
	if request.CatalogVersion == "" {
		request.CatalogVersion = p.CatalogVersion
	}
	return request
}
