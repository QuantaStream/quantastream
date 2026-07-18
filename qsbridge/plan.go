package qsbridge

// PlanNodeKind identifies one logical plan operation.
type PlanNodeKind string

const (
	// PlanNodeStatement represents a non-row statement result boundary.
	PlanNodeStatement PlanNodeKind = "statement"
	// PlanNodeScan reads one table instance.
	PlanNodeScan PlanNodeKind = "scan"
	// PlanNodeConstant produces one synthetic row for projection-only SELECTs.
	PlanNodeConstant PlanNodeKind = "constant"
	// PlanNodeFilter applies predicates to input rows.
	PlanNodeFilter PlanNodeKind = "filter"
	// PlanNodeMembership applies semi/anti membership edges to input rows.
	PlanNodeMembership PlanNodeKind = "membership"
	// PlanNodeProject shapes visible and hidden output columns.
	PlanNodeProject PlanNodeKind = "project"
	// PlanNodeJoin combines two inputs with a join edge.
	PlanNodeJoin PlanNodeKind = "join"
	// PlanNodeAggregate computes grouped or global aggregate slots.
	PlanNodeAggregate PlanNodeKind = "aggregate"
	// PlanNodeScalarSubquery records scalar subquery inputs required by the plan.
	PlanNodeScalarSubquery PlanNodeKind = "scalar_subquery"
	// PlanNodeCorrelatedAggregateSubquery records correlated aggregate subquery inputs required by the plan.
	PlanNodeCorrelatedAggregateSubquery PlanNodeKind = "correlated_aggregate_subquery"
	// PlanNodeSort orders rows by sort expressions.
	PlanNodeSort PlanNodeKind = "sort"
	// PlanNodeLimit applies LIMIT/OFFSET semantics.
	PlanNodeLimit PlanNodeKind = "limit"
	// PlanNodeUnsupported preserves a logical shape that cannot run natively.
	PlanNodeUnsupported PlanNodeKind = "unsupported"
)

// StatementNode represents an OK/affected-rows style statement result.
type StatementNode struct {
	Kind     QueryKind
	Result   StatementResult
	Mutation MutationShape
	Diags    DiagnosticSet
}

// NodeKind reports that StatementNode is a statement node.
func (StatementNode) NodeKind() PlanNodeKind {
	return PlanNodeStatement
}

// ChildNodes returns no children for a statement node.
func (StatementNode) ChildNodes() []LogicalNode {
	return nil
}

// NodeDiagnostics returns diagnostics attached to the statement node.
func (n StatementNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// LogicalNode is one node in a logical, executor-independent plan tree.
type LogicalNode interface {
	NodeKind() PlanNodeKind
	ChildNodes() []LogicalNode
	NodeDiagnostics() DiagnosticSet
}

// LogicalPlan is an executor-independent plan tree plus classification metadata.
type LogicalPlan struct {
	Root           LogicalNode
	Classification NativeClassification
	Result         ResultShape
}

// ScanNode reads one bound table instance.
type ScanNode struct {
	Source TableInstance
	Fields []FieldRef
	Diags  DiagnosticSet
}

// NodeKind reports that ScanNode is a scan node.
func (ScanNode) NodeKind() PlanNodeKind {
	return PlanNodeScan
}

// ChildNodes returns no children for a scan node.
func (ScanNode) ChildNodes() []LogicalNode {
	return nil
}

// NodeDiagnostics returns diagnostics attached to the scan node.
func (n ScanNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// ConstantNode produces synthetic rows for SELECT statements without a table source.
type ConstantNode struct {
	Rows  int
	Diags DiagnosticSet
}

// NodeKind reports that ConstantNode is a constant source.
func (ConstantNode) NodeKind() PlanNodeKind {
	return PlanNodeConstant
}

// ChildNodes returns no children for a constant source.
func (ConstantNode) ChildNodes() []LogicalNode {
	return nil
}

// NodeDiagnostics returns diagnostics attached to the constant source.
func (n ConstantNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// FilterNode applies predicates to one input.
type FilterNode struct {
	Input      LogicalNode
	Predicates []Predicate
	Diags      DiagnosticSet
}

// NodeKind reports that FilterNode is a filter node.
func (FilterNode) NodeKind() PlanNodeKind {
	return PlanNodeFilter
}

// ChildNodes returns the filter input.
func (n FilterNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the filter node.
func (n FilterNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// MembershipNode applies semi/anti membership edges to one input.
type MembershipNode struct {
	Input       LogicalNode
	Memberships []MembershipEdge
	Diags       DiagnosticSet
}

// NodeKind reports that MembershipNode is a membership node.
func (MembershipNode) NodeKind() PlanNodeKind {
	return PlanNodeMembership
}

// ChildNodes returns the membership input.
func (n MembershipNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the membership node.
func (n MembershipNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// ProjectNode shapes the client-visible and hidden projection.
type ProjectNode struct {
	Input   LogicalNode
	Columns []ProjectionColumn
	Result  ResultShape
	Diags   DiagnosticSet
}

// NodeKind reports that ProjectNode is a projection node.
func (ProjectNode) NodeKind() PlanNodeKind {
	return PlanNodeProject
}

// ChildNodes returns the projection input.
func (n ProjectNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the projection node.
func (n ProjectNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// JoinNode combines two inputs with a resolved join edge.
type JoinNode struct {
	Left  LogicalNode
	Right LogicalNode
	Edge  JoinEdge
	Diags DiagnosticSet
}

// NodeKind reports that JoinNode is a join node.
func (JoinNode) NodeKind() PlanNodeKind {
	return PlanNodeJoin
}

// ChildNodes returns the left and right join inputs.
func (n JoinNode) ChildNodes() []LogicalNode {
	children := make([]LogicalNode, 0, 2)
	if n.Left != nil {
		children = append(children, n.Left)
	}
	if n.Right != nil {
		children = append(children, n.Right)
	}
	return children
}

// NodeDiagnostics returns diagnostics attached to the join node.
func (n JoinNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// AggregateNode computes grouped or global aggregate values.
type AggregateNode struct {
	Input      LogicalNode
	GroupBy    []Expr
	Aggregates []Aggregate
	Having     []Predicate
	Diags      DiagnosticSet
}

// NodeKind reports that AggregateNode is an aggregate node.
func (AggregateNode) NodeKind() PlanNodeKind {
	return PlanNodeAggregate
}

// ChildNodes returns the aggregate input.
func (n AggregateNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the aggregate node.
func (n AggregateNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// ScalarSubqueryNode records scalar subquery inputs before they are planner-native execution nodes.
type ScalarSubqueryNode struct {
	Input   LogicalNode
	Intents []SubqueryPlanIntent
	Diags   DiagnosticSet
}

// NodeKind reports that ScalarSubqueryNode is a scalar subquery placeholder node.
func (ScalarSubqueryNode) NodeKind() PlanNodeKind {
	return PlanNodeScalarSubquery
}

// ChildNodes returns the scalar subquery input.
func (n ScalarSubqueryNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the scalar subquery node.
func (n ScalarSubqueryNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// ScalarOutputNames returns named scalar inputs carried by the node.
func (n ScalarSubqueryNode) ScalarOutputNames() []string {
	names := make([]string, 0, len(n.Intents))
	for _, intent := range n.Intents {
		if intent.Scalar == nil {
			continue
		}
		names = append(names, intent.Scalar.OutputName)
	}
	return names
}

// CorrelatedAggregateSubqueryNode records correlated aggregate subquery inputs before they are planner-native execution nodes.
type CorrelatedAggregateSubqueryNode struct {
	Input   LogicalNode
	Intents []SubqueryPlanIntent
	Diags   DiagnosticSet
}

// NodeKind reports that CorrelatedAggregateSubqueryNode is a correlated aggregate subquery placeholder node.
func (CorrelatedAggregateSubqueryNode) NodeKind() PlanNodeKind {
	return PlanNodeCorrelatedAggregateSubquery
}

// ChildNodes returns the correlated aggregate subquery input.
func (n CorrelatedAggregateSubqueryNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the correlated aggregate subquery node.
func (n CorrelatedAggregateSubqueryNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// CorrelatedAggregateFunctions returns aggregate function names carried by the node.
func (n CorrelatedAggregateSubqueryNode) CorrelatedAggregateFunctions() []string {
	names := make([]string, 0, len(n.Intents))
	for _, intent := range n.Intents {
		if intent.CorrelatedAggregate == nil {
			continue
		}
		names = append(names, intent.CorrelatedAggregate.AggregateFunction)
	}
	return names
}

// CorrelatedAggregateKeyRefs returns inner and outer key references carried by the node.
func (n CorrelatedAggregateSubqueryNode) CorrelatedAggregateKeyRefs() ([]string, []string) {
	inner := make([]string, 0, len(n.Intents))
	outer := make([]string, 0, len(n.Intents))
	for _, intent := range n.Intents {
		if intent.CorrelatedAggregate == nil {
			continue
		}
		inner = append(inner, correlatedAggregateFieldName(intent.CorrelatedAggregate.InnerKeyRef, intent.CorrelatedAggregate.InnerKey))
		outer = append(outer, correlatedAggregateFieldName(intent.CorrelatedAggregate.OuterKeyRef, intent.CorrelatedAggregate.OuterKey))
	}
	return inner, outer
}

// SortNode orders rows from its input.
type SortNode struct {
	Input   LogicalNode
	OrderBy []SortSpec
	Diags   DiagnosticSet
}

// NodeKind reports that SortNode is a sort node.
func (SortNode) NodeKind() PlanNodeKind {
	return PlanNodeSort
}

// ChildNodes returns the sort input.
func (n SortNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the sort node.
func (n SortNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// LimitNode applies LIMIT and OFFSET to its input.
type LimitNode struct {
	Input  LogicalNode
	Limit  int
	Offset int
	Diags  DiagnosticSet
}

// NodeKind reports that LimitNode is a limit node.
func (LimitNode) NodeKind() PlanNodeKind {
	return PlanNodeLimit
}

// ChildNodes returns the limit input.
func (n LimitNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the limit node.
func (n LimitNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// UnsupportedNode wraps a logical shape the native path cannot execute.
type UnsupportedNode struct {
	Input LogicalNode
	Diags DiagnosticSet
}

// NodeKind reports that UnsupportedNode is an unsupported node.
func (UnsupportedNode) NodeKind() PlanNodeKind {
	return PlanNodeUnsupported
}

// ChildNodes returns the unsupported node input when one exists.
func (n UnsupportedNode) ChildNodes() []LogicalNode {
	return singleChild(n.Input)
}

// NodeDiagnostics returns diagnostics attached to the unsupported node.
func (n UnsupportedNode) NodeDiagnostics() DiagnosticSet {
	return n.Diags
}

// BuildLogicalPlan lowers QueryIR into a conventional logical plan shape.
func BuildLogicalPlan(query QueryIR) LogicalPlan {
	classification := ClassifyNative(query)
	if query.Result.Kind == ResultStatement {
		root := LogicalNode(StatementNode{Kind: query.Kind, Result: query.Result.Statement, Mutation: query.Mutation})
		if classification.Diagnostics.BlocksNative() {
			root = UnsupportedNode{Input: root, Diags: classification.Diagnostics}
		}
		return LogicalPlan{
			Root:           root,
			Classification: classification,
			Result:         query.Result,
		}
	}
	root := buildSourcePlan(query, classification.Fields)
	if len(query.Memberships) > 0 {
		root = MembershipNode{
			Input:       root,
			Memberships: query.Memberships,
			Diags:       diagnosticsForMemberships(query.Memberships),
		}
	}
	if len(query.Predicates) > 0 {
		root = FilterNode{Input: root, Predicates: query.Predicates}
	}
	if scalarIntents := scalarSubqueryIntents(query.Subqueries); len(scalarIntents) > 0 {
		root = ScalarSubqueryNode{Input: root, Intents: scalarIntents}
	}
	if correlatedIntents := correlatedAggregateSubqueryIntents(query.Subqueries); len(correlatedIntents) > 0 {
		root = CorrelatedAggregateSubqueryNode{Input: root, Intents: correlatedIntents}
	}
	if len(query.GroupBy) > 0 || len(query.Aggregates) > 0 || len(query.Having) > 0 {
		root = AggregateNode{
			Input:      root,
			GroupBy:    query.GroupBy,
			Aggregates: query.Aggregates,
			Having:     query.Having,
		}
	}
	if len(query.Projection) > 0 || len(query.Result.Columns) > 0 || len(query.Result.Hidden) > 0 {
		root = ProjectNode{Input: root, Columns: query.Projection, Result: query.Result}
	}
	if len(query.OrderBy) > 0 {
		root = SortNode{Input: root, OrderBy: query.OrderBy}
	}
	if query.Result.Limit > 0 || query.Result.Offset > 0 {
		root = LimitNode{Input: root, Limit: query.Result.Limit, Offset: query.Result.Offset}
	}
	if classification.Diagnostics.BlocksNative() {
		root = UnsupportedNode{Input: root, Diags: classification.Diagnostics}
	}

	return LogicalPlan{
		Root:           root,
		Classification: classification,
		Result:         query.Result,
	}
}

func scalarSubqueryIntents(intents []SubqueryPlanIntent) []SubqueryPlanIntent {
	filtered := make([]SubqueryPlanIntent, 0)
	for _, intent := range intents {
		if intent.Kind == SubqueryIntentScalar && intent.Valid() {
			filtered = append(filtered, intent)
		}
	}
	return filtered
}

func correlatedAggregateSubqueryIntents(intents []SubqueryPlanIntent) []SubqueryPlanIntent {
	filtered := make([]SubqueryPlanIntent, 0)
	for _, intent := range intents {
		if intent.Kind == SubqueryIntentCorrelatedAggregate && intent.Valid() {
			filtered = append(filtered, intent)
		}
	}
	return filtered
}

func buildSourcePlan(query QueryIR, fields []FieldRef) LogicalNode {
	if len(query.Sources) == 0 {
		return ConstantNode{Rows: 1}
	}

	root := LogicalNode(ScanNode{Source: query.Sources[0], Fields: fieldsForSource(fields, query.Sources[0])})
	for i := 1; i < len(query.Sources); i++ {
		edge := JoinEdge{Legal: false, Unsupported: "missing join edge"}
		if i-1 < len(query.Joins) {
			edge = query.Joins[i-1]
		}
		root = JoinNode{
			Left:  root,
			Right: ScanNode{Source: query.Sources[i], Fields: fieldsForSource(fields, query.Sources[i])},
			Edge:  edge,
			Diags: diagnosticsForJoin(edge),
		}
	}
	return root
}

func fieldsForSource(fields []FieldRef, source TableInstance) []FieldRef {
	sourceFields := make([]FieldRef, 0)
	for _, field := range fields {
		if field.Table.ID == source.ID {
			sourceFields = append(sourceFields, field)
		}
	}
	return sourceFields
}

func diagnosticsForJoin(edge JoinEdge) DiagnosticSet {
	if edge.Supported() {
		return nil
	}
	return DiagnosticSet{JoinDiagnostic(edge)}
}

func diagnosticsForMemberships(edges []MembershipEdge) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	for _, edge := range edges {
		if edge.Supported() {
			continue
		}
		diagnostics = append(diagnostics, MembershipDiagnostic(edge))
	}
	return diagnostics
}

func singleChild(child LogicalNode) []LogicalNode {
	if child == nil {
		return nil
	}
	return []LogicalNode{child}
}

// WalkLogicalPlan visits each logical node in pre-order.
func WalkLogicalPlan(root LogicalNode, visit func(LogicalNode) bool) {
	if root == nil || visit == nil {
		return
	}
	if !visit(root) {
		return
	}
	for _, child := range root.ChildNodes() {
		WalkLogicalPlan(child, visit)
	}
}

// LogicalPlanDiagnostics returns diagnostics from the plan tree in traversal order.
func LogicalPlanDiagnostics(root LogicalNode) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		diagnostics = append(diagnostics, node.NodeDiagnostics()...)
		return true
	})
	return diagnostics
}
