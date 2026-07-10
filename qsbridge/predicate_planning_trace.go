package qsbridge

// PredicateTraceSource identifies where a traced predicate lives in QueryIR.
type PredicateTraceSource string

const (
	// PredicateTraceWhere identifies a predicate from QueryIR.Predicates.
	PredicateTraceWhere PredicateTraceSource = "where"
	// PredicateTraceHaving identifies a predicate from QueryIR.Having.
	PredicateTraceHaving PredicateTraceSource = "having"
	// PredicateTraceJoinOn identifies a predicate from a join edge's ON clause.
	PredicateTraceJoinOn PredicateTraceSource = "join_on"
)

// PredicatePlanningTrace summarizes predicate placement and native capability evidence.
//
// The trace is descriptive rather than prescriptive. It records the placement
// selected by binding or parser adapters plus the field encoding evidence that
// later optimizer and executor layers can use to explain or revise that choice.
type PredicatePlanningTrace struct {
	SQL         string
	Kind        QueryKind
	Supported   bool
	Diagnostics DiagnosticSet
	Predicates  []PredicatePlanningStep
}

// PredicatePlanningStep records one predicate's placement and catalog-backed evidence.
type PredicatePlanningStep struct {
	Index                 int
	Source                PredicateTraceSource
	Scope                 PredicateScope
	Placement             PredicatePlacement
	Operator              BinaryOp
	Supported             bool
	Unsupported           string
	Fields                []string
	FieldEvidence         []CatalogPlanningFieldTrace
	ExplicitCapabilities  []PlanCapability
	InferredCapabilities  []PlanCapability
	DiagnosticCodes       []DiagnosticCode
	PredicateCapabilities []PredicateCapability
}

// PredicatePlanningTrace returns predicate placement and encoding evidence for the plan.
func (r PlanResult) PredicatePlanningTrace() PredicatePlanningTrace {
	trace := PredicatePlanningTrace{
		SQL:         r.SQL,
		Kind:        r.Query.Kind,
		Supported:   r.Supported && !r.Diagnostics.BlocksNative(),
		Diagnostics: cloneDiagnosticSet(r.Diagnostics),
	}
	if trace.Kind == "" {
		trace.Kind = r.Unbound.Kind
	}
	trace.Predicates = appendPredicatePlanningSteps(trace.Predicates, PredicateTraceWhere, r.Query.Predicates)
	trace.Predicates = appendPredicatePlanningSteps(trace.Predicates, PredicateTraceHaving, r.Query.Having)
	for _, edge := range r.Query.Joins {
		trace.Predicates = appendPredicatePlanningSteps(trace.Predicates, PredicateTraceJoinOn, edge.On)
	}
	return trace
}

func appendPredicatePlanningSteps(steps []PredicatePlanningStep, source PredicateTraceSource, predicates []Predicate) []PredicatePlanningStep {
	for _, predicate := range predicates {
		steps = append(steps, predicatePlanningStep(len(steps), source, predicate))
	}
	return steps
}

func predicatePlanningStep(index int, source PredicateTraceSource, predicate Predicate) PredicatePlanningStep {
	fields := FieldRefs(predicate.Expr)
	step := PredicatePlanningStep{
		Index:                index,
		Source:               source,
		Scope:                predicate.Scope,
		Placement:            predicate.Placement,
		Operator:             predicateOperator(predicate.Expr),
		Supported:            predicate.Supported(),
		Unsupported:          predicate.Unsupported,
		Fields:               qualifiedFieldNames(fields),
		FieldEvidence:        make([]CatalogPlanningFieldTrace, 0, len(fields)),
		ExplicitCapabilities: append([]PlanCapability(nil), predicate.Capabilities...),
		InferredCapabilities: predicateInferredCapabilities(predicate),
	}
	if !predicate.Supported() {
		step.DiagnosticCodes = []DiagnosticCode{PredicateDiagnostic(predicate).Code}
	}
	for _, field := range fields {
		step.FieldEvidence = append(step.FieldEvidence, catalogPlanningFieldTrace(field))
		step.PredicateCapabilities = appendPredicateCapabilities(step.PredicateCapabilities, field.Encoding.PredicateCapabilities)
	}
	return step
}

func predicateOperator(expr Expr) BinaryOp {
	binary, ok := asBinaryExpr(expr)
	if !ok {
		return ""
	}
	return binary.Op
}

func predicateInferredCapabilities(predicate Predicate) []PlanCapability {
	collector := newCapabilityCollector()
	for _, capability := range EncodingPredicateCapabilities(predicate) {
		collector.add(capability)
	}
	capability, ok := StringEnumPredicateCapability(predicate)
	collector.addIf(ok, capability)
	switch predicate.Placement {
	case PredicateResidualScan:
		collector.add(CapabilityResidualScan)
	case PredicateResidualJoin:
		if !predicate.Supported() {
			collector.add(CapabilityUnsupportedMixedTableResidual)
		}
	}
	return collector.values
}

func appendPredicateCapabilities(target []PredicateCapability, values []PredicateCapability) []PredicateCapability {
	seen := make(map[PredicateCapability]struct{}, len(target)+len(values))
	for _, capability := range target {
		seen[capability] = struct{}{}
	}
	for _, capability := range values {
		if capability == "" {
			continue
		}
		if _, ok := seen[capability]; ok {
			continue
		}
		seen[capability] = struct{}{}
		target = append(target, capability)
	}
	return target
}
