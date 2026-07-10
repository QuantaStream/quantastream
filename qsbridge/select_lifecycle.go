package qsbridge

// SelectLifecycleStage identifies one step in the simple SELECT planning spine.
type SelectLifecycleStage string

const (
	// SelectLifecycleParse records parser-preview completion.
	SelectLifecycleParse SelectLifecycleStage = "parse"
	// SelectLifecycleBind records catalog binding completion.
	SelectLifecycleBind SelectLifecycleStage = "bind"
	// SelectLifecycleQueryIR records bound QueryIR availability.
	SelectLifecycleQueryIR SelectLifecycleStage = "query_ir"
	// SelectLifecycleLogicalPlan records logical-plan availability.
	SelectLifecycleLogicalPlan SelectLifecycleStage = "logical_plan"
	// SelectLifecyclePhysicalPlan records physical-plan availability.
	SelectLifecyclePhysicalPlan SelectLifecycleStage = "physical_plan"
	// SelectLifecycleResultSchema records client-visible result metadata availability.
	SelectLifecycleResultSchema SelectLifecycleStage = "result_schema"
	// SelectLifecycleDiagnostics records final diagnostic state.
	SelectLifecycleDiagnostics SelectLifecycleStage = "diagnostics"
)

// SelectLifecycleStep summarizes one stage in the simple SELECT planning path.
type SelectLifecycleStep struct {
	Stage           SelectLifecycleStage
	Complete        bool
	Supported       bool
	Detail          string
	Diagnostics     []DiagnosticCode
	LogicalRoot     PlanNodeKind
	PhysicalRoot    PhysicalNodeKind
	RequiredFields  int
	ResultColumns   int
	NativeBlockers  int
	CapabilityCount int
}

// SimpleSelectLifecycle is an end-to-end planning trace for a single SELECT.
type SimpleSelectLifecycle struct {
	SQL            string
	Kind           QueryKind
	Supported      bool
	Diagnostics    DiagnosticSet
	Sources        []string
	RequiredFields []string
	ResultColumns  []ResultColumn
	Steps          []SelectLifecycleStep
}

// SimpleSelectLifecycle returns a runtime-neutral vertical trace of SELECT planning.
func (r PlanResult) SimpleSelectLifecycle() SimpleSelectLifecycle {
	trace := SimpleSelectLifecycle{
		SQL:            r.SQL,
		Kind:           r.Query.Kind,
		Supported:      r.Supported && r.Query.Kind == QueryKindSelect,
		Diagnostics:    cloneDiagnosticSet(r.Diagnostics),
		Sources:        tableInstanceNames(r.Query.Sources),
		RequiredFields: qualifiedFieldNames(r.Query.RequiredFields()),
		ResultColumns:  append([]ResultColumn(nil), r.Query.ResultColumns()...),
	}
	if trace.Kind == "" {
		trace.Kind = r.Unbound.Kind
	}
	if trace.Kind != "" && trace.Kind != QueryKindSelect {
		trace.Diagnostics = mergeDiagnosticSets(trace.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhasePlan, "simple SELECT lifecycle requires a SELECT statement"),
		})
		trace.Supported = false
	}
	trace.Steps = []SelectLifecycleStep{
		r.selectLifecycleParseStep(),
		r.selectLifecycleBindStep(),
		r.selectLifecycleQueryIRStep(),
		r.selectLifecycleLogicalStep(),
		r.selectLifecyclePhysicalStep(),
		r.selectLifecycleResultSchemaStep(),
		r.selectLifecycleDiagnosticsStep(trace.Diagnostics),
	}
	return trace
}

func (r PlanResult) selectLifecycleParseStep() SelectLifecycleStep {
	complete := r.Unbound.Kind != "" || r.Unbound.SQL != ""
	return SelectLifecycleStep{
		Stage:       SelectLifecycleParse,
		Complete:    complete,
		Supported:   complete && !r.Diagnostics.BlocksNative(),
		Detail:      lifecycleDetail(complete, "parser produced unbound statement", "parser did not produce an unbound statement"),
		Diagnostics: r.Diagnostics.Codes(),
	}
}

func (r PlanResult) selectLifecycleBindStep() SelectLifecycleStep {
	complete := r.Query.Kind != ""
	return SelectLifecycleStep{
		Stage:          SelectLifecycleBind,
		Complete:       complete,
		Supported:      complete && r.Query.Kind == QueryKindSelect && !r.Diagnostics.BlocksNative(),
		Detail:         lifecycleDetail(complete, "catalog binding produced QueryIR", "catalog binding did not produce QueryIR"),
		Diagnostics:    r.Diagnostics.Codes(),
		RequiredFields: len(r.Query.RequiredFields()),
	}
}

func (r PlanResult) selectLifecycleQueryIRStep() SelectLifecycleStep {
	complete := r.Query.Kind == QueryKindSelect
	return SelectLifecycleStep{
		Stage:          SelectLifecycleQueryIR,
		Complete:       complete,
		Supported:      complete && r.Query.Supported(),
		Detail:         lifecycleDetail(complete, "QueryIR is a SELECT", "QueryIR is not a SELECT"),
		Diagnostics:    r.Query.Diagnostics().Codes(),
		RequiredFields: len(r.Query.RequiredFields()),
		ResultColumns:  len(r.Query.ResultColumns()),
		NativeBlockers: len(r.Query.Blockers),
	}
}

func (r PlanResult) selectLifecycleLogicalStep() SelectLifecycleStep {
	var root PlanNodeKind
	if r.Logical.Root != nil {
		root = r.Logical.Root.NodeKind()
	}
	complete := root != ""
	return SelectLifecycleStep{
		Stage:           SelectLifecycleLogicalPlan,
		Complete:        complete,
		Supported:       complete && !LogicalPlanDiagnostics(r.Logical.Root).BlocksNative(),
		Detail:          lifecycleDetail(complete, "logical plan root is available", "logical plan root is missing"),
		Diagnostics:     LogicalPlanDiagnostics(r.Logical.Root).Codes(),
		LogicalRoot:     root,
		CapabilityCount: len(r.Logical.Classification.Capabilities),
	}
}

func (r PlanResult) selectLifecyclePhysicalStep() SelectLifecycleStep {
	var root PhysicalNodeKind
	if r.Physical.Root != nil {
		root = r.Physical.Root.PhysicalKind()
	}
	complete := root != ""
	return SelectLifecycleStep{
		Stage:           SelectLifecyclePhysicalPlan,
		Complete:        complete,
		Supported:       complete && !PhysicalPlanDiagnostics(r.Physical.Root).BlocksNative(),
		Detail:          lifecycleDetail(complete, "physical plan root is available", "physical plan root is missing"),
		Diagnostics:     PhysicalPlanDiagnostics(r.Physical.Root).Codes(),
		PhysicalRoot:    root,
		CapabilityCount: len(r.Inspection.Capabilities),
	}
}

func (r PlanResult) selectLifecycleResultSchemaStep() SelectLifecycleStep {
	columns := r.Query.ResultColumns()
	complete := r.Query.Kind == QueryKindSelect && len(columns) > 0
	return SelectLifecycleStep{
		Stage:         SelectLifecycleResultSchema,
		Complete:      complete,
		Supported:     complete && !r.Diagnostics.BlocksNative(),
		Detail:        lifecycleDetail(complete, "result schema is available", "result schema is missing"),
		Diagnostics:   r.Diagnostics.Codes(),
		ResultColumns: len(columns),
	}
}

func (r PlanResult) selectLifecycleDiagnosticsStep(diagnostics DiagnosticSet) SelectLifecycleStep {
	return SelectLifecycleStep{
		Stage:       SelectLifecycleDiagnostics,
		Complete:    true,
		Supported:   !diagnostics.BlocksNative(),
		Detail:      lifecycleDetail(!diagnostics.BlocksNative(), "no blocking diagnostics", "blocking diagnostics are present"),
		Diagnostics: diagnostics.Codes(),
	}
}

func lifecycleDetail(ok bool, success, failure string) string {
	if ok {
		return success
	}
	return failure
}
