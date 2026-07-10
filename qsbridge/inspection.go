package qsbridge

// InspectionReport is the management-facing snapshot of native planning state.
//
// It intentionally duplicates some high-level facts from classification and
// explain artifacts so operational tools can read one stable envelope without
// walking every nested structure first.
type InspectionReport struct {
	Query          QueryInspection
	Supported      bool
	Capabilities   []PlanCapability
	Diagnostics    DiagnosticSet
	Classification NativeClassification
	Optimization   OptimizationTrace
	Logical        PlanExplanation
	Physical       PhysicalPlanExplanation
}

// QueryInspection summarizes the query shape at the report boundary.
type QueryInspection struct {
	Kind                QueryKind
	Sources             []string
	Fields              []string
	Access              []AccessInspection
	FieldEncodings      []FieldEncodingInspection
	Parameters          []ParameterRef
	ResultColumns       []ResultColumn
	FunctionUsages      []FunctionUsage
	SubqueryIntents     []SubqueryPlanIntentReport
	SubqueryHelperPlans []SubqueryHelperPlanReport
	NativeSubquerySteps []NativeSubqueryStepReport
	Blockers            []NativeBlocker
	Statement           StatementResult
	Mutation            MutationInspection
	JoinEdges           []JoinInspection
	MembershipEdges     []MembershipInspection
	Scan                ScanInspection
	ShardWindow         ShardWindowInspection
	Predicates          int
	Joins               int
	Memberships         int
	GroupBy             int
	Aggregates          int
	OrderBy             int
	Functions           int
	NativeBlockers      int
	Result              ResultShape
}

// ScanStrategy describes the broad table-access shape recognized by inspection.
type ScanStrategy string

const (
	// ScanStrategyUnknown means scan shape has not been classified.
	ScanStrategyUnknown ScanStrategy = ""
	// ScanStrategyFiltered means at least one predicate or membership filter constrains the scan.
	ScanStrategyFiltered ScanStrategy = "filtered"
	// ScanStrategyFullTable means the query has no filter that narrows source rows.
	ScanStrategyFullTable ScanStrategy = "full_table"
)

// ScanInspection summarizes whether the plan shape implies a full-table scan.
type ScanInspection struct {
	Strategy        ScanStrategy
	FullTable       bool
	Reason          string
	Tables          []string
	PredicateCount  int
	MembershipCount int
	JoinCount       int
}

// RelationshipStrategy describes how a join edge is expected to use relationship storage.
type RelationshipStrategy string

const (
	// RelationshipStrategyUnknown means relationship execution strategy has not been classified.
	RelationshipStrategyUnknown RelationshipStrategy = ""
	// RelationshipStrategyVectorReduction means relation-vector storage can reduce or expand found sets.
	RelationshipStrategyVectorReduction RelationshipStrategy = "relationship_vector_reduction"
	// RelationshipStrategyPeerEquality means the edge is ordinary equality without relation-vector metadata.
	RelationshipStrategyPeerEquality RelationshipStrategy = "peer_equality"
	// RelationshipStrategyUnsupported means the edge is known but unavailable to native execution.
	RelationshipStrategyUnsupported RelationshipStrategy = "unsupported"
)

// ShardWindowInspection records time predicates that may line up with shard-window pruning.
type ShardWindowInspection struct {
	CandidatePredicates int
	Fields              []string
	Notes               []string
}

// AccessInspection summarizes one authorization requirement for management inspection.
type AccessInspection struct {
	Privilege AccessPrivilege
	Table     string
	Fields    []string
}

// FieldEncodingInspection summarizes a required field's physical planning traits.
type FieldEncodingInspection struct {
	Field                  string
	Kind                   EncodingKind
	LegacyName             string
	Multiplicity           ValueMultiplicity
	Rehydration            RehydrationKind
	RequiresLookup         bool
	Searchable             bool
	PredicateCapabilities  []PredicateCapability
	ProjectionCapabilities []ProjectionCapability
}

// JoinInspection summarizes one join edge at the query inspection boundary.
type JoinInspection struct {
	Kind                   JoinKind
	Left                   string
	Right                  string
	Direction              JoinDirection
	Nulls                  NullExtension
	Cardinality            string
	Legal                  bool
	EncodingKind           RelationshipEncodingKind
	Capabilities           []RelationshipCapability
	Strategy               RelationshipStrategy
	UsesRelationshipVector bool
}

// MembershipInspection summarizes one semi/anti membership edge.
type MembershipInspection struct {
	Kind         MembershipKind
	Left         string
	Right        string
	Direction    JoinDirection
	Cardinality  string
	Legal        bool
	EncodingKind RelationshipEncodingKind
	Capabilities []RelationshipCapability
}

// MutationInspection summarizes write metadata without exposing row values.
type MutationInspection struct {
	Kind        MutationKind
	Target      string
	Columns     []string
	Rows        int
	Assignments int
	Predicates  int
}

// InspectQuery builds an inspection report with no explicit optimizer trace.
func InspectQuery(query QueryIR, scope PhysicalScope) InspectionReport {
	return InspectOptimizedQuery(query, OptimizationTrace{}, scope)
}

// InspectOptimizedQuery builds a complete inspection report for a query.
func InspectOptimizedQuery(query QueryIR, optimization OptimizationTrace, scope PhysicalScope) InspectionReport {
	logical := BuildLogicalPlan(query)
	queryInspection := summarizeQueryInspection(query)
	queryInspection.SubqueryHelperPlans = SubqueryHelperPlanReports(logical.Root)
	queryInspection.NativeSubquerySteps = NativeSubqueryStepReports(logical.Root)
	logicalExplanation := ExplainOptimizedLogicalPlan(logical, optimization)
	physicalExplanation := ExplainPhysicalPlan(BuildPhysicalPlan(logical, scope))
	diagnostics := mergeDiagnosticSets(
		logicalExplanation.Diagnostics,
		logicalExplanation.Optimization.Diagnostics,
		physicalExplanation.Diagnostics,
	)
	return InspectionReport{
		Query:          queryInspection,
		Supported:      logicalExplanation.Supported && physicalExplanation.Supported && !diagnostics.BlocksNative(),
		Capabilities:   append([]PlanCapability(nil), logicalExplanation.Capabilities...),
		Diagnostics:    diagnostics,
		Classification: logical.Classification,
		Optimization:   logicalExplanation.Optimization,
		Logical:        logicalExplanation,
		Physical:       physicalExplanation,
	}
}

func summarizeQueryInspection(query QueryIR) QueryInspection {
	requiredFields := query.RequiredFields()
	return QueryInspection{
		Kind:            query.Kind,
		Sources:         tableInstanceNames(query.Sources),
		Fields:          qualifiedFieldNames(requiredFields),
		Access:          summarizeAccessRequirements(query.RequiredAccess()),
		FieldEncodings:  summarizeFieldEncodings(requiredFields),
		Parameters:      query.RequiredParameters(),
		ResultColumns:   query.ResultColumns(),
		FunctionUsages:  query.FunctionUsages(),
		SubqueryIntents: summarizeSubqueryIntents(query.Subqueries),
		Blockers:        append([]NativeBlocker(nil), query.Blockers...),
		Statement:       query.StatementResult(),
		Mutation:        summarizeMutation(query.Mutation),
		JoinEdges:       summarizeJoinEdges(query.Joins),
		MembershipEdges: summarizeMembershipEdges(query.Memberships),
		Scan:            summarizeScan(query),
		ShardWindow:     summarizeShardWindow(query),
		Predicates:      len(query.Predicates) + len(query.Having),
		Joins:           len(query.Joins),
		Memberships:     len(query.Memberships),
		GroupBy:         len(query.GroupBy),
		Aggregates:      len(query.Aggregates),
		OrderBy:         len(query.OrderBy),
		Functions:       len(query.FunctionUsages()),
		NativeBlockers:  len(query.Blockers),
		Result:          query.Result,
	}
}

func summarizeAccessRequirements(requirements []AccessRequirement) []AccessInspection {
	summaries := make([]AccessInspection, 0, len(requirements))
	for _, requirement := range requirements {
		summaries = append(summaries, AccessInspection{
			Privilege: requirement.Privilege,
			Table:     requirement.Table.DisplayName(),
			Fields:    qualifiedFieldNames(requirement.Fields),
		})
	}
	return summaries
}

func summarizeSubqueryIntents(intents []SubqueryPlanIntent) []SubqueryPlanIntentReport {
	if len(intents) == 0 {
		return nil
	}
	reports := make([]SubqueryPlanIntentReport, 0, len(intents))
	for _, intent := range intents {
		reports = append(reports, intent.Report())
	}
	return reports
}

// summarizeMutation exposes mutation shape without copying literal row values.
func summarizeMutation(mutation MutationShape) MutationInspection {
	return MutationInspection{
		Kind:        mutation.Kind,
		Target:      mutation.Target.DisplayName(),
		Columns:     qualifiedFieldNames(mutation.Columns),
		Rows:        len(mutation.Rows),
		Assignments: len(mutation.Assignments),
		Predicates:  len(mutation.Predicates),
	}
}

// summarizeFieldEncodings exposes required-field physical traits for management inspection.
func summarizeFieldEncodings(fields []FieldRef) []FieldEncodingInspection {
	summaries := make([]FieldEncodingInspection, 0, len(fields))
	for _, field := range fields {
		encoding := field.Encoding
		summaries = append(summaries, FieldEncodingInspection{
			Field:                  field.QualifiedName(),
			Kind:                   encoding.Kind,
			LegacyName:             encoding.LegacyName,
			Multiplicity:           encoding.EffectiveMultiplicity(),
			Rehydration:            encoding.Rehydration.Kind,
			RequiresLookup:         encoding.RequiresLookup(),
			Searchable:             encoding.Searchable(),
			PredicateCapabilities:  append([]PredicateCapability(nil), encoding.PredicateCapabilities...),
			ProjectionCapabilities: append([]ProjectionCapability(nil), encoding.ProjectionCapabilities...),
		})
	}
	return summaries
}

func summarizeScan(query QueryIR) ScanInspection {
	inspection := ScanInspection{
		Tables:          tableInstanceNames(query.Sources),
		PredicateCount:  queryFilterPredicateCount(query),
		MembershipCount: len(query.Memberships),
		JoinCount:       len(query.Joins),
	}
	if query.Kind != QueryKindSelect && query.Kind != QueryKindUpdate && query.Kind != QueryKindDelete {
		inspection.Strategy = ScanStrategyUnknown
		inspection.Reason = "statement kind does not scan source rows"
		return inspection
	}
	if len(query.Sources) == 0 {
		inspection.Strategy = ScanStrategyUnknown
		inspection.Reason = "no source tables"
		return inspection
	}
	if inspection.PredicateCount == 0 && inspection.MembershipCount == 0 {
		inspection.Strategy = ScanStrategyFullTable
		inspection.FullTable = true
		inspection.Reason = "no WHERE, HAVING, mutation, or membership filters narrow source rows"
		return inspection
	}
	inspection.Strategy = ScanStrategyFiltered
	inspection.Reason = "predicate or membership filters narrow source rows"
	return inspection
}

func queryFilterPredicateCount(query QueryIR) int {
	return len(query.Predicates) + len(query.Having) + len(query.Mutation.Predicates)
}

func relationshipInspectionStrategy(edge JoinEdge) (RelationshipStrategy, bool) {
	if !edge.Supported() {
		return RelationshipStrategyUnsupported, false
	}
	if relationshipJoinNeedsVectorExecution(edge) {
		return RelationshipStrategyVectorReduction, true
	}
	if edge.Direction == JoinPeerEquality || edge.Encoding.Kind == RelationshipEncodingUnknown {
		return RelationshipStrategyPeerEquality, false
	}
	return RelationshipStrategyUnknown, false
}

func summarizeShardWindow(query QueryIR) ShardWindowInspection {
	fields := newFieldNameSet()
	candidatePredicates := 0
	addPredicate := func(predicate Predicate) {
		if predicateHasTimeWindowCandidate(predicate, fields) {
			candidatePredicates++
		}
	}
	for _, predicate := range query.Predicates {
		addPredicate(predicate)
	}
	for _, predicate := range query.Having {
		addPredicate(predicate)
	}
	for _, predicate := range query.Mutation.Predicates {
		addPredicate(predicate)
	}
	for _, edge := range query.Joins {
		for _, predicate := range edge.On {
			addPredicate(predicate)
		}
	}
	for _, edge := range query.Memberships {
		for _, predicate := range edge.Predicates {
			addPredicate(predicate)
		}
	}
	inspection := ShardWindowInspection{
		CandidatePredicates: candidatePredicates,
		Fields:              fields.names(),
	}
	if candidatePredicates > 0 {
		inspection.Notes = append(inspection.Notes, "time predicates may align with shard-window pruning when bounds match table shard granularity")
		if len(query.Joins) > 0 || len(query.Memberships) > 0 {
			inspection.Notes = append(inspection.Notes, "relationship-vector follow-up retrieval should preserve time-derived found sets before widening scan windows")
		}
	}
	return inspection
}

func predicateHasTimeWindowCandidate(predicate Predicate, fields *fieldNameSet) bool {
	hasCandidate := false
	for _, field := range FieldRefs(predicate.Expr) {
		if field.Type == DataTypeTime || field.Index == IndexDateTime {
			fields.add(field.QualifiedName())
			hasCandidate = true
		}
	}
	return hasCandidate
}

type fieldNameSet struct {
	seen  map[string]struct{}
	items []string
}

func newFieldNameSet() *fieldNameSet {
	return &fieldNameSet{seen: make(map[string]struct{})}
}

func (s *fieldNameSet) add(name string) {
	if name == "" {
		return
	}
	if _, ok := s.seen[name]; ok {
		return
	}
	s.seen[name] = struct{}{}
	s.items = append(s.items, name)
}

func (s *fieldNameSet) names() []string {
	return append([]string(nil), s.items...)
}

// summarizeJoinEdges exposes join edge shape for management inspection.
func summarizeJoinEdges(edges []JoinEdge) []JoinInspection {
	summaries := make([]JoinInspection, 0, len(edges))
	for _, edge := range edges {
		strategy, usesVector := relationshipInspectionStrategy(edge)
		summaries = append(summaries, JoinInspection{
			Kind:                   joinKindOrInner(edge.Kind),
			Left:                   edge.Left.QualifiedName(),
			Right:                  edge.Right.QualifiedName(),
			Direction:              edge.Direction,
			Nulls:                  edge.Nulls,
			Cardinality:            edge.Cardinality,
			Legal:                  edge.Supported(),
			EncodingKind:           edge.Encoding.Kind,
			Capabilities:           append([]RelationshipCapability(nil), edge.Encoding.Capabilities...),
			Strategy:               strategy,
			UsesRelationshipVector: usesVector,
		})
	}
	return summaries
}

// summarizeMembershipEdges exposes semi/anti edge shape for management inspection.
func summarizeMembershipEdges(edges []MembershipEdge) []MembershipInspection {
	summaries := make([]MembershipInspection, 0, len(edges))
	for _, edge := range edges {
		summaries = append(summaries, MembershipInspection{
			Kind:         edge.Kind,
			Left:         edge.Left.QualifiedName(),
			Right:        edge.Right.QualifiedName(),
			Direction:    edge.Direction,
			Cardinality:  edge.Cardinality,
			Legal:        edge.Supported(),
			EncodingKind: edge.Encoding.Kind,
			Capabilities: append([]RelationshipCapability(nil), edge.Encoding.Capabilities...),
		})
	}
	return summaries
}

func tableInstanceNames(tables []TableInstance) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.DisplayName())
	}
	return names
}

func mergeDiagnosticSets(sets ...DiagnosticSet) DiagnosticSet {
	merged := make(DiagnosticSet, 0)
	seen := make(map[string]struct{})
	for _, set := range sets {
		for _, diagnostic := range set {
			key := diagnosticKey(diagnostic)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, diagnostic)
		}
	}
	return merged
}

func diagnosticKey(diagnostic Diagnostic) string {
	return string(diagnostic.Code) +
		"\x00" + string(diagnostic.Severity) +
		"\x00" + string(diagnostic.Phase) +
		"\x00" + diagnostic.Message
}
