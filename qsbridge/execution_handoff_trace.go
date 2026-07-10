package qsbridge

// ExecutionHandoffTrace summarizes the native executor-facing request boundary.
//
// It is a discovery and diagnostics artifact. It does not route, execute, or
// inspect runtime storage; it only makes the already-built execution request
// legible for adapters, explain surfaces, and future executor tests.
type ExecutionHandoffTrace struct {
	RequestID       ExecutionRequestID
	SQL             string
	Kind            QueryKind
	Supported       bool
	Diagnostics     DiagnosticSet
	Options         ExecutionOptions
	AccessIntent    PhysicalAccessIntent
	Lifecycle       ClientPlanLifecycleKind
	LifecycleSteps  int
	Scope           PhysicalScope
	PhysicalRoot    PhysicalNodeKind
	PhysicalNodes   int
	Strategies      []PhysicalStrategy
	RequiredFields  []string
	ResultColumns   []ResultColumn
	Access          []AccessRequirement
	SessionActions  []SessionAction
	ParameterCount  int
	ParameterValues int
	Result          ResultShape
	Statement       StatementResult
}

// ExecutionHandoffTrace returns a non-executing summary of the native handoff request.
func (r ExecutionRequest) ExecutionHandoffTrace() ExecutionHandoffTrace {
	prepared := r.Bound.Prepared
	root, nodes, strategies := summarizePhysicalHandoff(prepared.Physical.Root)
	return ExecutionHandoffTrace{
		RequestID:       r.Options.RequestID,
		SQL:             prepared.SQL,
		Kind:            prepared.Kind,
		Supported:       r.SupportedForExecution(),
		Diagnostics:     cloneDiagnosticSet(r.Diagnostics),
		Options:         r.Options,
		AccessIntent:    prepared.AccessIntent(),
		Lifecycle:       clientPlanLifecycleKind(prepared.Kind),
		LifecycleSteps:  clientPlanLifecycleStepCount(prepared.Kind),
		Scope:           clonePhysicalScope(prepared.Scope),
		PhysicalRoot:    root,
		PhysicalNodes:   nodes,
		Strategies:      strategies,
		RequiredFields:  qualifiedFieldNames(prepared.Query.RequiredFields()),
		ResultColumns:   append([]ResultColumn(nil), r.ResultColumns...),
		Access:          cloneAccessRequirements(r.Access),
		SessionActions:  cloneSessionActions(r.SessionActions),
		ParameterCount:  len(prepared.Parameters),
		ParameterValues: len(r.Bound.Parameters.Bindings),
		Result:          cloneResultShape(r.Result),
		Statement:       cloneStatementResult(r.Statement),
	}
}

func summarizePhysicalHandoff(root PhysicalNode) (PhysicalNodeKind, int, []PhysicalStrategy) {
	var rootKind PhysicalNodeKind
	strategies := newPhysicalStrategyCollector()
	nodes := 0
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		nodes++
		if rootKind == "" {
			rootKind = node.PhysicalKind()
		}
		for _, strategy := range physicalNodeStrategies(node) {
			strategies.add(strategy)
		}
		return true
	})
	return rootKind, nodes, strategies.values
}

func physicalNodeStrategies(node PhysicalNode) []PhysicalStrategy {
	switch n := node.(type) {
	case PhysicalUnaryNode:
		return append([]PhysicalStrategy(nil), n.Strategies...)
	case *PhysicalUnaryNode:
		if n != nil {
			return append([]PhysicalStrategy(nil), n.Strategies...)
		}
	case PhysicalJoinNode:
		return append([]PhysicalStrategy(nil), n.Strategies...)
	case *PhysicalJoinNode:
		if n != nil {
			return append([]PhysicalStrategy(nil), n.Strategies...)
		}
	}
	return nil
}

func cloneResultShape(shape ResultShape) ResultShape {
	cloned := shape
	cloned.Columns = append([]FieldRef(nil), shape.Columns...)
	cloned.Hidden = append([]FieldRef(nil), shape.Hidden...)
	cloned.OrderBy = append([]Expr(nil), shape.OrderBy...)
	cloned.Statement = cloneStatementResult(shape.Statement)
	return cloned
}
