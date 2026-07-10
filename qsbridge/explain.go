package qsbridge

import (
	"fmt"
	"strings"
)

// ExplainSection identifies an optional section in a structured explain response.
type ExplainSection string

const (
	// ExplainSectionLogical includes logical plan nodes.
	ExplainSectionLogical ExplainSection = "logical"
	// ExplainSectionPhysical includes physical plan nodes.
	ExplainSectionPhysical ExplainSection = "physical"
	// ExplainSectionOptimizer includes optimizer rewrite audit records.
	ExplainSectionOptimizer ExplainSection = "optimizer"
	// ExplainSectionOptimizerSummary includes aggregate optimizer rewrite counts.
	ExplainSectionOptimizerSummary ExplainSection = "optimizer_summary"
	// ExplainSectionDiagnostics includes merged planning diagnostics.
	ExplainSectionDiagnostics ExplainSection = "diagnostics"
	// ExplainSectionFunctions includes query-local function usage inventory.
	ExplainSectionFunctions ExplainSection = "functions"
	// ExplainSectionNativeBlockers includes native blocker inventory.
	ExplainSectionNativeBlockers ExplainSection = "native_blockers"
)

// ExplainOptions selects optional structured explain sections for adapters.
type ExplainOptions struct {
	IncludeLogical          bool
	IncludePhysical         bool
	IncludeOptimizer        bool
	IncludeOptimizerSummary bool
	IncludeDiagnostics      bool
	IncludeFunctions        bool
	IncludeNativeBlockers   bool
}

// ExplainBundle is a structured explain response shaped by ExplainOptions.
type ExplainBundle struct {
	Options             ExplainOptions
	Supported           bool
	AccessIntent        PhysicalAccessIntent
	Lifecycle           ClientPlanLifecycleKind
	LifecycleSteps      int
	Sections            []ExplainSection
	Logical             PlanExplanation
	Physical            PhysicalPlanExplanation
	Optimization        OptimizationTrace
	OptimizationSummary OptimizationSummary
	Diagnostics         DiagnosticSet
	FunctionUsages      []FunctionUsage
	NativeBlockers      []NativeBlocker
}

// DefaultExplainOptions returns the compact default explain shape.
func DefaultExplainOptions() ExplainOptions {
	return ExplainOptions{IncludeLogical: true}
}

// VerboseExplainOptions returns all currently available structured explain sections.
func VerboseExplainOptions() ExplainOptions {
	return ExplainOptions{
		IncludeLogical:          true,
		IncludePhysical:         true,
		IncludeOptimizer:        true,
		IncludeOptimizerSummary: true,
		IncludeDiagnostics:      true,
		IncludeFunctions:        true,
		IncludeNativeBlockers:   true,
	}
}

// Empty reports whether no explain sections were requested.
func (o ExplainOptions) Empty() bool {
	return !o.IncludeLogical &&
		!o.IncludePhysical &&
		!o.IncludeOptimizer &&
		!o.IncludeOptimizerSummary &&
		!o.IncludeDiagnostics &&
		!o.IncludeFunctions &&
		!o.IncludeNativeBlockers
}

// Effective returns the default compact explain shape when no sections are requested.
func (o ExplainOptions) Effective() ExplainOptions {
	if o.Empty() {
		return DefaultExplainOptions()
	}
	return o
}

// Includes reports whether section is selected after applying default semantics.
func (o ExplainOptions) Includes(section ExplainSection) bool {
	o = o.Effective()
	switch section {
	case ExplainSectionLogical:
		return o.IncludeLogical
	case ExplainSectionPhysical:
		return o.IncludePhysical
	case ExplainSectionOptimizer:
		return o.IncludeOptimizer
	case ExplainSectionOptimizerSummary:
		return o.IncludeOptimizerSummary
	case ExplainSectionDiagnostics:
		return o.IncludeDiagnostics
	case ExplainSectionFunctions:
		return o.IncludeFunctions
	case ExplainSectionNativeBlockers:
		return o.IncludeNativeBlockers
	default:
		return false
	}
}

// Sections returns selected sections in stable display order.
func (o ExplainOptions) Sections() []ExplainSection {
	o = o.Effective()
	sections := make([]ExplainSection, 0, 7)
	for _, section := range []ExplainSection{
		ExplainSectionLogical,
		ExplainSectionPhysical,
		ExplainSectionOptimizer,
		ExplainSectionOptimizerSummary,
		ExplainSectionDiagnostics,
		ExplainSectionFunctions,
		ExplainSectionNativeBlockers,
	} {
		if o.Includes(section) {
			sections = append(sections, section)
		}
	}
	return sections
}

// ExplainInspectionReport selects structured explain sections from a prepared inspection report.
func ExplainInspectionReport(report InspectionReport, options ExplainOptions) ExplainBundle {
	options = options.Effective()
	bundle := ExplainBundle{
		Options:        options,
		Supported:      report.Supported,
		AccessIntent:   PhysicalIntentForQueryKind(report.Query.Kind),
		Lifecycle:      clientPlanLifecycleKind(report.Query.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(report.Query.Kind),
		Sections:       options.Sections(),
	}
	if options.IncludeLogical {
		bundle.Logical = clonePlanExplanation(report.Logical)
	}
	if options.IncludePhysical {
		bundle.Physical = clonePhysicalPlanExplanation(report.Physical)
	}
	if options.IncludeOptimizer {
		bundle.Optimization = report.Optimization.Clone()
	}
	if options.IncludeOptimizerSummary {
		bundle.OptimizationSummary = report.Optimization.Summary()
	}
	if options.IncludeDiagnostics {
		bundle.Diagnostics = cloneDiagnosticSet(report.Diagnostics)
	}
	if options.IncludeFunctions {
		bundle.FunctionUsages = append([]FunctionUsage(nil), report.Query.FunctionUsages...)
	}
	if options.IncludeNativeBlockers {
		bundle.NativeBlockers = append([]NativeBlocker(nil), report.Query.Blockers...)
	}
	return bundle
}

// PlanExplanation is a stable, JSON-friendly description of a logical plan.
type PlanExplanation struct {
	Supported    bool
	Capabilities []PlanCapability
	Diagnostics  DiagnosticSet
	Optimization OptimizationTrace
	Nodes        []PlanNodeExplanation
}

// Text returns a compact single-line logical plan summary.
func (e PlanExplanation) Text() string {
	parts := make([]string, 0, len(e.Nodes))
	for _, node := range e.Nodes {
		parts = append(parts, node.Summary)
	}
	return strings.Join(parts, " -> ")
}

// HasCapability reports whether the explanation includes capability.
func (e PlanExplanation) HasCapability(capability PlanCapability) bool {
	for _, current := range e.Capabilities {
		if current == capability {
			return true
		}
	}
	return false
}

// PlanNodeExplanation is a stable inspection record for one logical plan node.
type PlanNodeExplanation struct {
	ID                  int
	ParentID            int
	Depth               int
	Kind                PlanNodeKind
	Summary             string
	Source              string
	Fields              []string
	Predicates          PredicateSummary
	Membership          MembershipSummary
	ScalarSubquery      ScalarSubquerySummary
	CorrelatedAggregate CorrelatedAggregateSubquerySummary
	Join                JoinSummary
	Aggregate           AggregateSummary
	Projection          ProjectionSummary
	Sort                SortSummary
	Limit               LimitSummary
	Statement           StatementSummary
	Diagnostics         []DiagnosticCode
}

// PredicateSummary records predicate counts by native placement.
type PredicateSummary struct {
	Total        int
	Pushdown     int
	ResidualScan int
	ResidualJoin int
	Membership   int
	Unsupported  int
	Capabilities []PlanCapability
}

// MembershipSummary records semi/anti membership edge counts.
type MembershipSummary struct {
	Total        int
	Semi         int
	Anti         int
	Legal        int
	Capabilities []PlanCapability
}

// ScalarSubquerySummary records scalar subquery placeholder output shape.
type ScalarSubquerySummary struct {
	Total       int
	OutputNames []string
	HelperPlans []SubqueryHelperPlanReport
	NativeSteps []NativeSubqueryStepReport
}

// CorrelatedAggregateSubquerySummary records correlated aggregate placeholder shape.
type CorrelatedAggregateSubquerySummary struct {
	Total              int
	AggregateFunctions []string
	InnerKeyRefs       []string
	OuterKeyRefs       []string
	HelperKinds        []string
	HelperPlans        []SubqueryHelperPlanReport
	NativeSteps        []NativeSubqueryStepReport
}

// JoinSummary records the resolved join edge in explainable form.
type JoinSummary struct {
	Kind         JoinKind
	Direction    JoinDirection
	Nulls        NullExtension
	Left         string
	Right        string
	On           PredicateSummary
	Legal        bool
	Unsupported  string
	Capabilities []PlanCapability
}

// AggregateSummary records grouped aggregate shape.
type AggregateSummary struct {
	GroupBy    int
	Aggregates int
	Having     PredicateSummary
}

// ProjectionSummary records projection shape.
type ProjectionSummary struct {
	Columns int
	Hidden  int
}

// SortSummary records sort shape.
type SortSummary struct {
	Keys int
}

// LimitSummary records limit and offset shape.
type LimitSummary struct {
	Limit  int
	Offset int
}

// StatementSummary records OK/affected-rows style statement metadata.
type StatementSummary struct {
	Kind         QueryKind
	AffectedRows uint64
	LastInsertID uint64
	Warnings     uint16
	Status       string
	Mutation     MutationInspection
}

// PhysicalPlanExplanation is a stable, JSON-friendly description of a physical plan.
type PhysicalPlanExplanation struct {
	Supported   bool
	Diagnostics DiagnosticSet
	Nodes       []PhysicalNodeExplanation
}

// Text returns a compact single-line physical plan summary.
func (e PhysicalPlanExplanation) Text() string {
	parts := make([]string, 0, len(e.Nodes))
	for _, node := range e.Nodes {
		parts = append(parts, node.Summary)
	}
	return strings.Join(parts, " -> ")
}

// PhysicalNodeExplanation is a stable inspection record for one physical plan node.
type PhysicalNodeExplanation struct {
	ID          int
	ParentID    int
	Depth       int
	Kind        PhysicalNodeKind
	Summary     string
	Source      string
	Fields      []string
	Strategies  []PhysicalStrategy
	Scope       PhysicalScopeSummary
	Join        JoinSummary
	Statement   StatementSummary
	Diagnostics []DiagnosticCode
}

// PhysicalScopeSummary records shard, replica, routing, placement, and cache scope.
type PhysicalScopeSummary struct {
	AllShards bool
	Shards    []string
	Replicas  []string
	Routing   string
	Placement PlacementPolicy
	Cache     CacheScope
}

// ExplainLogicalPlan builds a structured explanation for logical without optimizer audit records.
func ExplainLogicalPlan(logical LogicalPlan) PlanExplanation {
	return ExplainOptimizedLogicalPlan(logical, OptimizationTrace{})
}

// ExplainOptimizedLogicalPlan builds a structured explanation including optimizer audit records.
func ExplainOptimizedLogicalPlan(logical LogicalPlan, optimization OptimizationTrace) PlanExplanation {
	explainer := logicalExplainer{
		nextID: 1,
		nodes:  make([]PlanNodeExplanation, 0),
	}
	explainer.walk(logical.Root, 0, 0)
	optimization = optimization.Clone()
	if optimization.Supported == false && !optimization.Diagnostics.BlocksNative() {
		optimization.Supported = true
	}
	return PlanExplanation{
		Supported:    logical.Classification.Supported && optimization.Supported,
		Capabilities: append([]PlanCapability(nil), logical.Classification.Capabilities...),
		Diagnostics:  append(DiagnosticSet(nil), logical.Classification.Diagnostics...),
		Optimization: optimization,
		Nodes:        explainer.nodes,
	}
}

// ExplainPhysicalPlan builds a structured explanation for a physical plan scaffold.
func ExplainPhysicalPlan(physical PhysicalPlan) PhysicalPlanExplanation {
	explainer := physicalExplainer{
		nextID: 1,
		nodes:  make([]PhysicalNodeExplanation, 0),
	}
	explainer.walk(physical.Root, 0, 0)
	diagnostics := PhysicalPlanDiagnostics(physical.Root)
	return PhysicalPlanExplanation{
		Supported:   !diagnostics.BlocksNative(),
		Diagnostics: diagnostics,
		Nodes:       explainer.nodes,
	}
}

type logicalExplainer struct {
	nextID int
	nodes  []PlanNodeExplanation
}

func (e *logicalExplainer) walk(node LogicalNode, parentID int, depth int) {
	if node == nil {
		return
	}
	id := e.nextID
	e.nextID++

	explained := explainLogicalNode(id, parentID, depth, node)
	e.nodes = append(e.nodes, explained)
	for _, child := range node.ChildNodes() {
		e.walk(child, id, depth+1)
	}
}

type physicalExplainer struct {
	nextID int
	nodes  []PhysicalNodeExplanation
}

func (e *physicalExplainer) walk(node PhysicalNode, parentID int, depth int) {
	if node == nil {
		return
	}
	id := e.nextID
	e.nextID++
	e.nodes = append(e.nodes, explainPhysicalNode(id, parentID, depth, node))
	for _, child := range node.PhysicalChildren() {
		e.walk(child, id, depth+1)
	}
}

func explainLogicalNode(id int, parentID int, depth int, node LogicalNode) PlanNodeExplanation {
	base := PlanNodeExplanation{
		ID:          id,
		ParentID:    parentID,
		Depth:       depth,
		Kind:        node.NodeKind(),
		Diagnostics: diagnosticCodes(node.NodeDiagnostics()),
	}
	switch n := node.(type) {
	case StatementNode:
		base.Statement = summarizeStatement(n.Kind, n.Result, n.Mutation)
		base.Summary = fmt.Sprintf("statement(%s,affected=%d,warnings=%d%s)", n.Kind, n.Result.AffectedRows, n.Result.Warnings, mutationSummaryText(base.Statement.Mutation))
	case ScanNode:
		base.Source = n.Source.RefName()
		base.Fields = qualifiedFieldNames(n.Fields)
		base.Summary = fmt.Sprintf("scan(%s,fields=%d)", scanSourceName(n.Source), len(n.Fields))
	case FilterNode:
		base.Predicates = summarizePredicates(n.Predicates)
		base.Summary = fmt.Sprintf(
			"filter(predicates=%d,pushdown=%d,residual_scan=%d,residual_join=%d)",
			base.Predicates.Total,
			base.Predicates.Pushdown,
			base.Predicates.ResidualScan,
			base.Predicates.ResidualJoin,
		)
	case MembershipNode:
		base.Membership = summarizeMemberships(n.Memberships)
		base.Summary = fmt.Sprintf(
			"membership(total=%d,semi=%d,anti=%d)",
			base.Membership.Total,
			base.Membership.Semi,
			base.Membership.Anti,
		)
	case ScalarSubqueryNode:
		base.ScalarSubquery = summarizeScalarSubqueries(n)
		base.Summary = fmt.Sprintf("scalar_subquery(outputs=%d)", base.ScalarSubquery.Total)
	case CorrelatedAggregateSubqueryNode:
		base.CorrelatedAggregate = summarizeCorrelatedAggregateSubqueries(n)
		base.Summary = fmt.Sprintf("correlated_aggregate_subquery(intents=%d,helpers=%d)", base.CorrelatedAggregate.Total, len(base.CorrelatedAggregate.HelperKinds))
	case ProjectNode:
		base.Projection = ProjectionSummary{Columns: len(n.Columns), Hidden: len(n.Result.Hidden)}
		base.Summary = fmt.Sprintf("project(columns=%d,hidden=%d)", base.Projection.Columns, base.Projection.Hidden)
	case JoinNode:
		base.Join = JoinSummary{
			Kind:         joinKindOrInner(n.Edge.Kind),
			Direction:    n.Edge.Direction,
			Nulls:        n.Edge.Nulls,
			Left:         n.Edge.Left.QualifiedName(),
			Right:        n.Edge.Right.QualifiedName(),
			On:           summarizePredicates(n.Edge.On),
			Legal:        n.Edge.Legal,
			Unsupported:  n.Edge.Unsupported,
			Capabilities: summarizeJoinCapabilities(n.Edge),
		}
		base.Summary = fmt.Sprintf(
			"join(%s,%s=%s,on=%d)",
			base.Join.Kind,
			base.Join.Left,
			base.Join.Right,
			base.Join.On.Total,
		)
	case AggregateNode:
		base.Aggregate = AggregateSummary{
			GroupBy:    len(n.GroupBy),
			Aggregates: len(n.Aggregates),
			Having:     summarizePredicates(n.Having),
		}
		base.Summary = fmt.Sprintf(
			"aggregate(group=%d,aggregates=%d,having=%d)",
			base.Aggregate.GroupBy,
			base.Aggregate.Aggregates,
			base.Aggregate.Having.Total,
		)
	case SortNode:
		base.Sort = SortSummary{Keys: len(n.OrderBy)}
		base.Summary = fmt.Sprintf("sort(keys=%d)", base.Sort.Keys)
	case LimitNode:
		base.Limit = LimitSummary{Limit: n.Limit, Offset: n.Offset}
		base.Summary = fmt.Sprintf("limit(limit=%d,offset=%d)", n.Limit, n.Offset)
	case UnsupportedNode:
		base.Summary = fmt.Sprintf("unsupported(diagnostics=%d)", len(base.Diagnostics))
	default:
		base.Summary = string(node.NodeKind())
	}
	return base
}

func explainPhysicalNode(id int, parentID int, depth int, node PhysicalNode) PhysicalNodeExplanation {
	base := PhysicalNodeExplanation{
		ID:          id,
		ParentID:    parentID,
		Depth:       depth,
		Kind:        node.PhysicalKind(),
		Scope:       summarizePhysicalScope(node.PhysicalScope()),
		Diagnostics: diagnosticCodes(node.PhysicalDiagnostics()),
	}
	scopeText := physicalScopeText(base.Scope)
	switch n := node.(type) {
	case PhysicalStatementNode:
		base.Statement = summarizeStatement(n.Kind, n.Result, n.Mutation)
		base.Summary = fmt.Sprintf("physical_statement(%s,affected=%d,warnings=%d%s,%s)", n.Kind, n.Result.AffectedRows, n.Result.Warnings, mutationSummaryText(base.Statement.Mutation), scopeText)
	case PhysicalScanNode:
		base.Source = n.Source.RefName()
		base.Fields = qualifiedFieldNames(n.Fields)
		base.Summary = fmt.Sprintf("physical_scan(%s,fields=%d,%s)", scanSourceName(n.Source), len(n.Fields), scopeText)
	case PhysicalUnaryNode:
		base.Strategies = append([]PhysicalStrategy(nil), n.Strategies...)
		base.Summary = fmt.Sprintf("%s(%s)", n.Kind, scopeText)
	case PhysicalAggregateNode:
		base.Strategies = append([]PhysicalStrategy(nil), n.Strategies...)
		base.Summary = fmt.Sprintf("physical_aggregate(aggregates=%d,group=%d,having=%d,%s)", len(n.Aggregates), len(n.GroupBy), len(n.Having), scopeText)
	case PhysicalJoinNode:
		base.Strategies = append([]PhysicalStrategy(nil), n.Strategies...)
		base.Join = JoinSummary{
			Kind:         joinKindOrInner(n.Edge.Kind),
			Direction:    n.Edge.Direction,
			Nulls:        n.Edge.Nulls,
			Left:         n.Edge.Left.QualifiedName(),
			Right:        n.Edge.Right.QualifiedName(),
			On:           summarizePredicates(n.Edge.On),
			Legal:        n.Edge.Legal,
			Unsupported:  n.Edge.Unsupported,
			Capabilities: summarizeJoinCapabilities(n.Edge),
		}
		base.Summary = fmt.Sprintf(
			"physical_join(%s,%s=%s,on=%d,%s)",
			base.Join.Kind,
			base.Join.Left,
			base.Join.Right,
			base.Join.On.Total,
			scopeText,
		)
	default:
		base.Summary = fmt.Sprintf("%s(%s)", node.PhysicalKind(), scopeText)
	}
	return base
}

func summarizeStatement(kind QueryKind, result StatementResult, mutation MutationShape) StatementSummary {
	return StatementSummary{
		Kind:         kind,
		AffectedRows: result.AffectedRows,
		LastInsertID: result.LastInsertID,
		Warnings:     result.Warnings,
		Status:       result.Status,
		Mutation:     summarizeMutation(mutation),
	}
}

func mutationSummaryText(mutation MutationInspection) string {
	if mutation.Kind == MutationUnknown {
		return ""
	}
	return fmt.Sprintf(
		",mutation=%s,rows=%d,assignments=%d,predicates=%d",
		mutation.Kind,
		mutation.Rows,
		mutation.Assignments,
		mutation.Predicates,
	)
}

func summarizePhysicalScope(scope PhysicalScope) PhysicalScopeSummary {
	return PhysicalScopeSummary{
		AllShards: scope.Shards.All,
		Shards:    shardStrings(scope.Shards.Shards),
		Replicas:  replicaStrings(scope.Replicas),
		Routing:   string(scope.Routing),
		Placement: scope.Placement,
		Cache:     scope.Cache,
	}
}

func physicalScopeText(scope PhysicalScopeSummary) string {
	parts := make([]string, 0, 5)
	if scope.AllShards {
		parts = append(parts, "shards=all")
	} else {
		parts = append(parts, fmt.Sprintf("shards=%d", len(scope.Shards)))
	}
	if len(scope.Replicas) > 0 {
		parts = append(parts, fmt.Sprintf("replicas=%d", len(scope.Replicas)))
	}
	if scope.Routing != "" {
		parts = append(parts, "routing="+scope.Routing)
	}
	if scope.Placement != "" {
		parts = append(parts, "placement="+string(scope.Placement))
	}
	if scope.Cache != "" {
		parts = append(parts, "cache="+string(scope.Cache))
	}
	return strings.Join(parts, ",")
}

func summarizeScalarSubqueries(node ScalarSubqueryNode) ScalarSubquerySummary {
	return ScalarSubquerySummary{
		Total:       len(node.Intents),
		OutputNames: node.ScalarOutputNames(),
		HelperPlans: subqueryHelperPlanReportsForIntents(node.Intents),
		NativeSteps: nativeSubqueryStepReportsForIntents(node.Intents),
	}
}

func summarizeCorrelatedAggregateSubqueries(node CorrelatedAggregateSubqueryNode) CorrelatedAggregateSubquerySummary {
	inner, outer := node.CorrelatedAggregateKeyRefs()
	return CorrelatedAggregateSubquerySummary{
		Total:              len(node.Intents),
		AggregateFunctions: node.CorrelatedAggregateFunctions(),
		InnerKeyRefs:       inner,
		OuterKeyRefs:       outer,
		HelperKinds:        correlatedAggregateHelperKinds(node.Intents),
		HelperPlans:        subqueryHelperPlanReportsForIntents(node.Intents),
		NativeSteps:        nativeSubqueryStepReportsForIntents(node.Intents),
	}
}

func correlatedAggregateHelperKinds(intents []SubqueryPlanIntent) []string {
	kinds := make([]string, 0)
	for _, intent := range intents {
		kinds = append(kinds, intent.HelperKinds()...)
	}
	return kinds
}

func summarizeMemberships(edges []MembershipEdge) MembershipSummary {
	summary := MembershipSummary{Total: len(edges)}
	capabilities := newCapabilityCollector()
	for _, edge := range edges {
		if edge.Supported() {
			summary.Legal++
		}
		switch edge.Kind {
		case MembershipSemi:
			summary.Semi++
		case MembershipAnti:
			summary.Anti++
		}
		for _, capability := range edge.Capabilities() {
			capabilities.add(capability)
		}
		for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
			capabilities.add(capability)
		}
	}
	summary.Capabilities = capabilities.values
	return summary
}

// summarizeJoinCapabilities returns join capability evidence in stable first-seen order.
func summarizeJoinCapabilities(edge JoinEdge) []PlanCapability {
	capabilities := newCapabilityCollector()
	for _, capability := range edge.Capabilities() {
		capabilities.add(capability)
	}
	for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
		capabilities.add(capability)
	}
	return capabilities.values
}

func summarizePredicates(predicates []Predicate) PredicateSummary {
	summary := PredicateSummary{Total: len(predicates)}
	capabilities := newCapabilityCollector()
	for _, predicate := range predicates {
		switch predicate.Placement {
		case PredicatePushdown:
			summary.Pushdown++
		case PredicateResidualScan:
			summary.ResidualScan++
		case PredicateResidualJoin:
			summary.ResidualJoin++
		case PredicateMembership:
			summary.Membership++
		case PredicateUnsupported:
			summary.Unsupported++
		}
		if !predicate.Supported() && predicate.Placement != PredicateUnsupported {
			summary.Unsupported++
		}
		for _, capability := range predicate.Capabilities {
			capabilities.add(capability)
		}
		for _, capability := range EncodingPredicateCapabilities(predicate) {
			capabilities.add(capability)
		}
		if predicate.Placement == PredicatePushdown {
			capability, ok := StringEnumPredicateCapability(predicate)
			capabilities.addIf(ok, capability)
		}
	}
	summary.Capabilities = capabilities.values
	return summary
}

func diagnosticCodes(diagnostics DiagnosticSet) []DiagnosticCode {
	if len(diagnostics) == 0 {
		return nil
	}
	return diagnostics.Codes()
}

func qualifiedFieldNames(fields []FieldRef) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.QualifiedName())
	}
	return names
}

func shardStrings(shards []ShardID) []string {
	values := make([]string, 0, len(shards))
	for _, shard := range shards {
		values = append(values, string(shard))
	}
	return values
}

func replicaStrings(replicas []ReplicaID) []string {
	values := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		values = append(values, string(replica))
	}
	return values
}

func scanSourceName(source TableInstance) string {
	if ref := source.RefName(); ref != "" {
		return ref
	}
	return string(source.ID)
}
