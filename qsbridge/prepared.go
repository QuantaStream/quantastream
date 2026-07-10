package qsbridge

// PreparedPlan is the protocol-facing result of preparing SQL.
//
// It preserves the planning artifacts and client-visible metadata that a MySQL
// prepared-statement adapter needs before execution: parameter metadata, result
// columns, statement OK shape, diagnostics, and support status.
type PreparedPlan struct {
	Handle         PreparedStatementHandle
	SQL            string
	DefaultSchema  string
	CatalogVersion CatalogVersion
	Session        SessionContext
	Scope          PhysicalScope
	Kind           QueryKind
	Query          QueryIR
	Diagnostics    DiagnosticSet
	Logical        LogicalPlan
	Physical       PhysicalPlan
	Inspection     InspectionReport
	Access         []AccessRequirement
	Parameters     []ParameterRef
	ResultColumns  []ResultColumn
	Statement      StatementResult
	Result         ResultShape
	Supported      bool
}

// PreparedPlan returns a protocol-facing prepared-plan snapshot.
func (r PlanResult) PreparedPlan() PreparedPlan {
	return PreparePlan(r)
}

// PreparePlan creates a prepared-plan snapshot from a planning result.
func PreparePlan(result PlanResult) PreparedPlan {
	query := result.Query
	diagnostics := append(DiagnosticSet(nil), result.Diagnostics...)
	supported := result.Supported && !diagnostics.BlocksNative()
	return PreparedPlan{
		SQL:            result.SQL,
		DefaultSchema:  result.DefaultSchema,
		CatalogVersion: result.CatalogVersion,
		Session:        result.Session.Clone(),
		Scope:          clonePhysicalScope(result.Scope),
		Kind:           query.Kind,
		Query:          query,
		Diagnostics:    diagnostics,
		Logical:        result.Logical,
		Physical:       result.Physical,
		Inspection:     result.Inspection,
		Access:         query.RequiredAccess(),
		Parameters:     query.RequiredParameters(),
		ResultColumns:  query.ResultColumns(),
		Statement:      query.StatementResult(),
		Result:         query.Result,
		Supported:      supported,
	}
}

// AccessIntent reports whether the prepared plan should read or write physical data.
func (p PreparedPlan) AccessIntent() PhysicalAccessIntent {
	return PhysicalIntentForQueryKind(p.Kind)
}

// BoundPlan is a prepared plan paired with one validated parameter set.
type BoundPlan struct {
	Prepared    PreparedPlan
	Parameters  ParameterBindingSet
	Diagnostics DiagnosticSet
	Supported   bool
}

// Bind validates execute-time values for this prepared plan.
func (p PreparedPlan) Bind(values ...ParameterValue) BoundPlan {
	parameters := BindParameterValues(p.Parameters, values...)
	diagnostics := mergeDiagnosticSets(p.Diagnostics, parameters.Diagnostics)
	supported := p.Supported && !diagnostics.BlocksNative()
	return BoundPlan{
		Prepared:    p,
		Parameters:  parameters,
		Diagnostics: diagnostics,
		Supported:   supported,
	}
}

// SupportedForExecution reports whether the plan and parameters are valid.
func (p BoundPlan) SupportedForExecution() bool {
	return p.Supported && !p.Diagnostics.BlocksNative()
}
