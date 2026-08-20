package qsbridge

// MutationLifecycleStage identifies one step in mutation planning.
type MutationLifecycleStage string

const (
	// MutationLifecycleParse records parser-preview completion.
	MutationLifecycleParse MutationLifecycleStage = "parse"
	// MutationLifecycleBind records catalog binding completion.
	MutationLifecycleBind MutationLifecycleStage = "bind"
	// MutationLifecycleQueryIR records bound mutation QueryIR availability.
	MutationLifecycleQueryIR MutationLifecycleStage = "query_ir"
	// MutationLifecycleLogicalPlan records logical statement-plan availability.
	MutationLifecycleLogicalPlan MutationLifecycleStage = "logical_plan"
	// MutationLifecyclePhysicalPlan records physical statement-plan availability.
	MutationLifecyclePhysicalPlan MutationLifecycleStage = "physical_plan"
	// MutationLifecycleStatementResult records OK/affected-rows metadata availability.
	MutationLifecycleStatementResult MutationLifecycleStage = "statement_result"
	// MutationLifecycleDiagnostics records final diagnostic state.
	MutationLifecycleDiagnostics MutationLifecycleStage = "diagnostics"
)

// MutationLifecycleStep summarizes one stage in mutation planning.
type MutationLifecycleStep struct {
	Stage          MutationLifecycleStage
	Complete       bool
	Supported      bool
	Detail         string
	Diagnostics    []DiagnosticCode
	LogicalRoot    PlanNodeKind
	PhysicalRoot   PhysicalNodeKind
	Target         string
	Columns        int
	Rows           int
	Assignments    int
	Predicates     int
	ParameterCount int
}

// MutationLifecycle is an end-to-end planning trace for INSERT, UPDATE, or DELETE.
type MutationLifecycle struct {
	SQL            string
	Kind           QueryKind
	Mutation       MutationKind
	AccessIntent   PhysicalAccessIntent
	Supported      bool
	Diagnostics    DiagnosticSet
	Target         string
	Columns        []string
	ParameterCount int
	Statement      StatementResult
	Steps          []MutationLifecycleStep
}

// MutationLifecycle returns a runtime-neutral vertical trace of mutation planning.
func (r PlanResult) MutationLifecycle() MutationLifecycle {
	trace := MutationLifecycle{
		SQL:            r.SQL,
		Kind:           r.Query.Kind,
		Mutation:       r.Query.Mutation.Kind,
		AccessIntent:   PhysicalIntentForQueryKind(r.Query.Kind),
		Supported:      r.Supported && isMutationQueryKind(r.Query.Kind),
		Diagnostics:    cloneDiagnosticSet(r.Diagnostics),
		Target:         r.Query.Mutation.Target.DisplayName(),
		Columns:        qualifiedFieldNames(r.Query.Mutation.Columns),
		ParameterCount: len(r.Query.RequiredParameters()),
		Statement:      r.Query.StatementResult(),
	}
	if trace.Kind == "" {
		trace.Kind = r.Unbound.Kind
		trace.AccessIntent = PhysicalIntentForQueryKind(trace.Kind)
	}
	if trace.Kind != "" && !isMutationQueryKind(trace.Kind) {
		trace.Diagnostics = mergeDiagnosticSets(trace.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhasePlan, "mutation lifecycle requires INSERT, UPDATE, or DELETE"),
		})
		trace.Supported = false
	}
	trace.Steps = []MutationLifecycleStep{
		r.mutationLifecycleParseStep(),
		r.mutationLifecycleBindStep(),
		r.mutationLifecycleQueryIRStep(),
		r.mutationLifecycleLogicalStep(),
		r.mutationLifecyclePhysicalStep(),
		r.mutationLifecycleStatementStep(),
		r.mutationLifecycleDiagnosticsStep(trace.Diagnostics),
	}
	return trace
}

func (r PlanResult) mutationLifecycleParseStep() MutationLifecycleStep {
	complete := r.Unbound.Kind != "" || r.Unbound.SQL != ""
	return MutationLifecycleStep{
		Stage:       MutationLifecycleParse,
		Complete:    complete,
		Supported:   complete && !r.Diagnostics.BlocksNative(),
		Detail:      lifecycleDetail(complete, "parser produced unbound statement", "parser did not produce an unbound statement"),
		Diagnostics: r.Diagnostics.Codes(),
	}
}

func (r PlanResult) mutationLifecycleBindStep() MutationLifecycleStep {
	complete := r.Query.Kind != ""
	return MutationLifecycleStep{
		Stage:          MutationLifecycleBind,
		Complete:       complete,
		Supported:      complete && isMutationQueryKind(r.Query.Kind) && !r.Diagnostics.BlocksNative(),
		Detail:         lifecycleDetail(complete, "catalog binding produced mutation QueryIR", "catalog binding did not produce QueryIR"),
		Diagnostics:    r.Diagnostics.Codes(),
		Target:         r.Query.Mutation.Target.DisplayName(),
		Columns:        len(r.Query.Mutation.Columns),
		ParameterCount: len(r.Query.RequiredParameters()),
	}
}

func (r PlanResult) mutationLifecycleQueryIRStep() MutationLifecycleStep {
	complete := isMutationQueryKind(r.Query.Kind)
	return MutationLifecycleStep{
		Stage:          MutationLifecycleQueryIR,
		Complete:       complete,
		Supported:      complete && r.Query.Supported(),
		Detail:         lifecycleDetail(complete, "QueryIR is a mutation", "QueryIR is not a mutation"),
		Diagnostics:    r.Query.Diagnostics().Codes(),
		Target:         r.Query.Mutation.Target.DisplayName(),
		Columns:        len(r.Query.Mutation.Columns),
		Rows:           len(r.Query.Mutation.Rows),
		Assignments:    len(r.Query.Mutation.Assignments),
		Predicates:     len(r.Query.Mutation.Predicates),
		ParameterCount: len(r.Query.RequiredParameters()),
	}
}

func (r PlanResult) mutationLifecycleLogicalStep() MutationLifecycleStep {
	var root PlanNodeKind
	if r.Logical.Root != nil {
		root = r.Logical.Root.NodeKind()
	}
	complete := root != ""
	return MutationLifecycleStep{
		Stage:       MutationLifecycleLogicalPlan,
		Complete:    complete,
		Supported:   complete && !LogicalPlanDiagnostics(r.Logical.Root).BlocksNative(),
		Detail:      lifecycleDetail(complete, "logical statement plan root is available", "logical statement plan root is missing"),
		Diagnostics: LogicalPlanDiagnostics(r.Logical.Root).Codes(),
		LogicalRoot: root,
	}
}

func (r PlanResult) mutationLifecyclePhysicalStep() MutationLifecycleStep {
	var root PhysicalNodeKind
	if r.Physical.Root != nil {
		root = r.Physical.Root.PhysicalKind()
	}
	complete := root != ""
	return MutationLifecycleStep{
		Stage:        MutationLifecyclePhysicalPlan,
		Complete:     complete,
		Supported:    complete && !PhysicalPlanDiagnostics(r.Physical.Root).BlocksNative(),
		Detail:       lifecycleDetail(complete, "physical statement plan root is available", "physical statement plan root is missing"),
		Diagnostics:  PhysicalPlanDiagnostics(r.Physical.Root).Codes(),
		PhysicalRoot: root,
	}
}

func (r PlanResult) mutationLifecycleStatementStep() MutationLifecycleStep {
	complete := r.Query.Result.Kind == ResultStatement
	return MutationLifecycleStep{
		Stage:       MutationLifecycleStatementResult,
		Complete:    complete,
		Supported:   complete && !r.Diagnostics.BlocksNative(),
		Detail:      lifecycleDetail(complete, "statement result metadata is available", "statement result metadata is missing"),
		Diagnostics: r.Diagnostics.Codes(),
	}
}

func (r PlanResult) mutationLifecycleDiagnosticsStep(diagnostics DiagnosticSet) MutationLifecycleStep {
	return MutationLifecycleStep{
		Stage:       MutationLifecycleDiagnostics,
		Complete:    true,
		Supported:   !diagnostics.BlocksNative(),
		Detail:      lifecycleDetail(!diagnostics.BlocksNative(), "no blocking diagnostics", "blocking diagnostics are present"),
		Diagnostics: diagnostics.Codes(),
	}
}

func isMutationQueryKind(kind QueryKind) bool {
	return kind == QueryKindInsert ||
		kind == QueryKindUpdate ||
		kind == QueryKindDelete ||
		kind == QueryKindTruncate ||
		kind == QueryKindCreateTable ||
		kind == QueryKindDropTable ||
		kind == QueryKindAlterTable ||
		kind == QueryKindCreateView ||
		kind == QueryKindDropView
}
