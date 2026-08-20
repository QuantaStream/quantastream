package qsbridge

import "strings"

// TableInstanceID identifies one bound use of a table inside a query.
type TableInstanceID string

// TableInstance represents one table role in a bound query.
//
// Repeated uses of the same base table must have different TableInstance
// values. This keeps self-joins and repeated TPC-H roles such as nation and
// lineitem from collapsing into one base-table identity.
type TableInstance struct {
	ID     TableInstanceID
	Schema string
	Table  string
	Alias  string
	Role   string
}

// InlineRowSet carries a small materialized source through planning.
type InlineRowSet struct {
	Source TableInstance
	Fields []FieldDefinition
	Rows   []ResultRow
}

// RefName returns the SQL name that should be used to qualify fields.
func (t TableInstance) RefName() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Table
}

// DisplayName returns a stable human-readable table instance label.
func (t TableInstance) DisplayName() string {
	parts := make([]string, 0, 3)
	if t.Schema != "" {
		parts = append(parts, t.Schema)
	}
	if t.Table != "" {
		parts = append(parts, t.Table)
	}
	if t.Alias != "" && t.Alias != t.Table {
		parts = append(parts, "as "+t.Alias)
	}
	if len(parts) == 0 {
		return string(t.ID)
	}
	return strings.Join(parts, ".")
}

// IndexKind describes the primary physical access strategy for a field.
type IndexKind string

const (
	// IndexUnknown means the field has not been classified for native access.
	IndexUnknown IndexKind = ""
	// IndexBitmap identifies a bitmap-backed field.
	IndexBitmap IndexKind = "bitmap"
	// IndexBSI identifies an integer or numeric BSI-backed field.
	IndexBSI IndexKind = "bsi"
	// IndexDateTime identifies a Date/DateTime BSI-backed field with time pruning.
	IndexDateTime IndexKind = "datetime"
	// IndexStringEnum identifies a dictionary-backed StringEnum field.
	IndexStringEnum IndexKind = "string_enum"
	// IndexBackingString identifies a backing-string field that may need residual evaluation.
	IndexBackingString IndexKind = "backing_string"
)

// FieldRole marks how a bound field participates in a plan.
type FieldRole uint64

const (
	// FieldRoleVisible marks a client-visible output field.
	FieldRoleVisible FieldRole = 1 << iota
	// FieldRoleHidden marks a planner-added projection field.
	FieldRoleHidden
	// FieldRoleJoinInput marks a field required by a join edge.
	FieldRoleJoinInput
	// FieldRoleGroupKey marks a field required as a grouping key.
	FieldRoleGroupKey
	// FieldRoleSortKey marks a field required by ordering.
	FieldRoleSortKey
	// FieldRoleResidualInput marks a field required by residual evaluation.
	FieldRoleResidualInput
	// FieldRoleMutationTarget marks a field written by INSERT/UPDATE/DELETE planning.
	FieldRoleMutationTarget
	// FieldRoleMutationValue marks a field read while computing mutation values.
	FieldRoleMutationValue
)

// Has reports whether role contains every flag in want.
func (role FieldRole) Has(want FieldRole) bool {
	return role&want == want
}

// FieldRef is a field resolved against a specific TableInstance.
type FieldRef struct {
	Table        TableInstance
	Name         string
	PhysicalName string
	Type         DataType
	Index        IndexKind
	Nullable     bool
	PrimaryKey   bool
	Roles        FieldRole
	Encoding     EncodingProfile
	Dictionary   DictionaryDefinition
}

// QualifiedName returns the alias-qualified or table-qualified field name.
func (f FieldRef) QualifiedName() string {
	if ref := f.Table.RefName(); ref != "" {
		return ref + "." + f.Name
	}
	return f.Name
}

// SupportsDictionaryCapability reports whether the field dictionary advertises capability.
func (f FieldRef) SupportsDictionaryCapability(capability DictionaryCapability) bool {
	return f.Dictionary.Supports(capability)
}

// SupportsPredicateCapability reports whether the field encoding advertises capability.
func (f FieldRef) SupportsPredicateCapability(capability PredicateCapability) bool {
	return f.Encoding.SupportsPredicate(capability)
}

// Expr is a typed scalar expression used by native planning.
type Expr interface {
	ExpressionKind() ExprKind
}

// ExprKind names the broad category of a scalar expression.
type ExprKind string

const (
	// ExprLiteral is a scalar literal.
	ExprLiteral ExprKind = "literal"
	// ExprField is a field reference expression.
	ExprField ExprKind = "field"
	// ExprParameter is a prepared-statement placeholder expression.
	ExprParameter ExprKind = "parameter"
	// ExprList is a scalar expression list such as the right side of IN.
	ExprList ExprKind = "list"
	// ExprCall is a function call expression.
	ExprCall ExprKind = "call"
	// ExprBinary is a binary operator expression.
	ExprBinary ExprKind = "binary"
	// ExprSearchedCase is a searched CASE expression.
	ExprSearchedCase ExprKind = "searched_case"
	// ExprAggregateRef is a reference to an aggregate slot.
	ExprAggregateRef ExprKind = "aggregate_ref"
	// ExprScalarSubquery is a scalar subquery expression awaiting materialization.
	ExprScalarSubquery ExprKind = "scalar_subquery"
	// ExprExistsSubquery is an EXISTS subquery expression awaiting gate materialization.
	ExprExistsSubquery ExprKind = "exists_subquery"
)

// ParameterRef identifies one prepared-statement placeholder.
type ParameterRef struct {
	Index    int
	Name     string
	Type     DataType
	Nullable bool
}

// PredicatePlacement describes where a predicate can be evaluated.
type PredicatePlacement string

const (
	// PredicatePushdown means the predicate can be lowered to indexed primitives.
	PredicatePushdown PredicatePlacement = "pushdown"
	// PredicateResidualScan means the predicate runs over rows from one table.
	PredicateResidualScan PredicatePlacement = "residual_scan"
	// PredicateResidualJoin means the predicate runs after join materialization.
	PredicateResidualJoin PredicatePlacement = "residual_join"
	// PredicateMembership means the predicate is implemented as semi/anti membership.
	PredicateMembership PredicatePlacement = "membership"
	// PredicateUnsupported means the native path recognizes but cannot execute the predicate.
	PredicateUnsupported PredicatePlacement = "unsupported"
)

// PredicateScope records where a predicate appeared in SQL.
type PredicateScope string

const (
	// PredicateScopeUnknown means predicate origin has not been recorded.
	PredicateScopeUnknown PredicateScope = ""
	// PredicateScopeWhere means the predicate came from the WHERE clause.
	PredicateScopeWhere PredicateScope = "where"
	// PredicateScopeOn means the predicate came from a JOIN ON clause.
	PredicateScopeOn PredicateScope = "on"
	// PredicateScopeHaving means the predicate came from the HAVING clause.
	PredicateScopeHaving PredicateScope = "having"
	// PredicateScopeProjection means the expression came from the SELECT list.
	PredicateScopeProjection PredicateScope = "projection"
)

// PredicateCombinator records how a predicate combines with its siblings.
type PredicateCombinator string

const (
	// PredicateCombinatorAnd means the predicate is part of the normal conjunctive filter.
	PredicateCombinatorAnd PredicateCombinator = "and"
	// PredicateCombinatorOr means the predicate is part of a same-level disjunctive filter.
	PredicateCombinatorOr PredicateCombinator = "or"
)

// PlanCapability names a capability or blocker attached to a native plan node.
type PlanCapability string

const (
	// CapabilityBitmapPushdown identifies bitmap predicate pushdown.
	CapabilityBitmapPushdown PlanCapability = "BitmapPushdown"
	// CapabilityBSIPushdown identifies BSI predicate pushdown.
	CapabilityBSIPushdown PlanCapability = "BSIPushdown"
	// CapabilityResidualScan identifies residual single-table evaluation.
	CapabilityResidualScan PlanCapability = "ResidualScan"
	// CapabilityGroupedJoin identifies grouped join execution.
	CapabilityGroupedJoin PlanCapability = "GroupedJoin"
	// CapabilityScalarSubquery identifies scalar subquery execution.
	CapabilityScalarSubquery PlanCapability = "ScalarSubquery"
	// CapabilityExistsSubquery identifies EXISTS subquery gate execution.
	CapabilityExistsSubquery PlanCapability = "ExistsSubquery"
	// CapabilityOuterJoin identifies preserved-side join semantics.
	CapabilityOuterJoin PlanCapability = "OuterJoin"
	// CapabilityNullExtension identifies null-filled rows for unmatched outer join input.
	CapabilityNullExtension PlanCapability = "NullExtension"
	// CapabilitySemiMembership identifies IN/EXISTS-style membership filtering.
	CapabilitySemiMembership PlanCapability = "SemiMembership"
	// CapabilityAntiMembership identifies NOT IN/NOT EXISTS-style membership filtering.
	CapabilityAntiMembership PlanCapability = "AntiMembership"
	// CapabilityParentToChildExpansion identifies parent-to-child relationship expansion.
	CapabilityParentToChildExpansion PlanCapability = "ParentToChildExpansion"
	// CapabilityCancellationAware identifies cancellation-aware execution.
	CapabilityCancellationAware PlanCapability = "CancellationAware"
	// CapabilityBitmapDifference identifies anti-membership lowering to bitmap difference.
	CapabilityBitmapDifference PlanCapability = "BitmapDifference"
	// CapabilityRelationshipParentLookup identifies child-to-parent traversal through relation storage.
	CapabilityRelationshipParentLookup PlanCapability = "RelationshipParentLookup"
	// CapabilityRelationshipChildExpansion identifies parent-to-child traversal through relation storage.
	CapabilityRelationshipChildExpansion PlanCapability = "RelationshipChildExpansion"
	// CapabilityRelationshipJoinReduction identifies found-set reduction through relation storage.
	CapabilityRelationshipJoinReduction PlanCapability = "RelationshipJoinReduction"
	// CapabilityRelationshipSemiJoin identifies relation-backed semi-join membership.
	CapabilityRelationshipSemiJoin PlanCapability = "RelationshipSemiJoin"
	// CapabilityRelationshipAntiJoinDifference identifies relation-backed anti-join difference.
	CapabilityRelationshipAntiJoinDifference PlanCapability = "RelationshipAntiJoinDifference"
	// CapabilityUnsupportedMixedTableResidual marks an unsupported mixed-table residual.
	CapabilityUnsupportedMixedTableResidual PlanCapability = "UnsupportedMixedTableResidual"
	// CapabilityStringEnumEquality identifies exact StringEnum dictionary equality.
	CapabilityStringEnumEquality PlanCapability = "StringEnumEquality"
	// CapabilityStringEnumPrefixLike identifies prefix LIKE over StringEnum dictionary labels.
	CapabilityStringEnumPrefixLike PlanCapability = "StringEnumPrefixLike"
	// CapabilityStringEnumContainsLike identifies contains LIKE over StringEnum dictionary labels.
	CapabilityStringEnumContainsLike PlanCapability = "StringEnumContainsLike"
	// CapabilityStringEnumMembership identifies IN membership over StringEnum dictionary labels.
	CapabilityStringEnumMembership PlanCapability = "StringEnumMembership"
	// CapabilityNativeTopN identifies native top-N frequency aggregation.
	CapabilityNativeTopN PlanCapability = "NativeTopN"
	// CapabilityNativeSameRowBSIComparison identifies same-row BSI field-vs-field comparison.
	CapabilityNativeSameRowBSIComparison PlanCapability = "NativeSameRowBSIComparison"
	// CapabilityEncodingEquality identifies equality support advertised by the field encoding profile.
	CapabilityEncodingEquality PlanCapability = "EncodingEquality"
	// CapabilityEncodingMembership identifies membership support advertised by the field encoding profile.
	CapabilityEncodingMembership PlanCapability = "EncodingMembership"
	// CapabilityEncodingRange identifies range support advertised by the field encoding profile.
	CapabilityEncodingRange PlanCapability = "EncodingRange"
	// CapabilityEncodingPrefix identifies prefix support advertised by the field encoding profile.
	CapabilityEncodingPrefix PlanCapability = "EncodingPrefix"
	// CapabilityEncodingContains identifies contains support advertised by the field encoding profile.
	CapabilityEncodingContains PlanCapability = "EncodingContains"
)

// Predicate is a boolean expression classified for native execution.
type Predicate struct {
	Expr         Expr
	Placement    PredicatePlacement
	Scope        PredicateScope
	Combinator   PredicateCombinator
	Capabilities []PlanCapability
	Unsupported  string
}

// Supported reports whether the predicate is executable by the native path.
func (p Predicate) Supported() bool {
	return p.Placement != PredicateUnsupported && p.Unsupported == ""
}

// JoinDirection describes how a relationship edge can be traversed.
type JoinDirection string

const (
	// JoinDirectionUnknown means direction has not been classified.
	JoinDirectionUnknown JoinDirection = ""
	// JoinChildToParent is the currently common Quanta relationship direction.
	JoinChildToParent JoinDirection = "child_to_parent"
	// JoinParentToChild requires explicit expansion support.
	JoinParentToChild JoinDirection = "parent_to_child"
	// JoinPeerEquality is a normal equality edge without known relationship direction.
	JoinPeerEquality JoinDirection = "peer_equality"
)

// JoinKind describes row-production semantics for a join edge.
type JoinKind string

const (
	// JoinKindUnknown means join semantics have not been classified.
	JoinKindUnknown JoinKind = ""
	// JoinKindInner emits only matched rows.
	JoinKindInner JoinKind = "inner"
	// JoinKindLeftOuter preserves unmatched left-side rows.
	JoinKindLeftOuter JoinKind = "left_outer"
	// JoinKindRightOuter preserves unmatched right-side rows.
	JoinKindRightOuter JoinKind = "right_outer"
	// JoinKindFullOuter preserves unmatched rows from both sides.
	JoinKindFullOuter JoinKind = "full_outer"
)

// NullExtension describes which side of an outer join may be null-filled.
type NullExtension string

const (
	// NullExtensionNone means no side is null-extended.
	NullExtensionNone NullExtension = ""
	// NullExtensionLeft means unmatched rows from the right side null-fill left fields.
	NullExtensionLeft NullExtension = "left"
	// NullExtensionRight means unmatched rows from the left side null-fill right fields.
	NullExtensionRight NullExtension = "right"
	// NullExtensionBoth means full outer join semantics may null-fill either side.
	NullExtensionBoth NullExtension = "both"
)

// JoinEdge is a declared relationship or comparison edge between table instances.
type JoinEdge struct {
	Left        FieldRef
	Right       FieldRef
	Operator    BinaryOp
	Kind        JoinKind
	Nulls       NullExtension
	On          []Predicate
	Direction   JoinDirection
	Cardinality string
	Encoding    RelationshipEncodingProfile
	Legal       bool
	Unsupported string
}

// Capabilities returns native capabilities implied by the join edge.
func (e JoinEdge) Capabilities() []PlanCapability {
	switch e.Kind {
	case JoinKindLeftOuter, JoinKindRightOuter:
		return []PlanCapability{CapabilityOuterJoin, CapabilityNullExtension, CapabilityBitmapDifference}
	case JoinKindFullOuter:
		return []PlanCapability{CapabilityOuterJoin, CapabilityNullExtension, CapabilityBitmapDifference}
	default:
		return nil
	}
}

// MembershipKind describes whether membership keeps or excludes matching rows.
type MembershipKind string

const (
	// MembershipKindUnknown means the membership kind has not been classified.
	MembershipKindUnknown MembershipKind = ""
	// MembershipSemi keeps left-side rows with matching right-side values.
	MembershipSemi MembershipKind = "semi"
	// MembershipAnti excludes left-side rows with matching right-side values.
	MembershipAnti MembershipKind = "anti"
)

// MembershipEdge filters one field's row set using another field's values.
//
// Left is the filtered side. Anti membership is therefore naturally lowered as
// left minus matching right values when the physical planner can use bitmaps.
type MembershipEdge struct {
	Left            FieldRef
	Right           FieldRef
	RightInlineRows *InlineRowSet
	LeftTuple       []Expr
	RightTuple      []Expr
	Kind            MembershipKind
	Direction       JoinDirection
	Cardinality     string
	Encoding        RelationshipEncodingProfile
	Predicates      []Predicate
	Legal           bool
	Unsupported     string
}

// Supported reports whether the membership edge can be used by the native path.
func (e MembershipEdge) Supported() bool {
	if !e.Legal || e.Unsupported != "" {
		return false
	}
	if e.IsTuple() && (len(e.LeftTuple) == 0 || len(e.LeftTuple) != len(e.RightTuple)) {
		return false
	}
	for _, predicate := range e.Predicates {
		if !predicate.Supported() {
			return false
		}
	}
	return true
}

// IsTuple reports whether the membership compares row-value tuples instead of
// a single left/right field pair.
func (e MembershipEdge) IsTuple() bool {
	return len(e.LeftTuple) > 0 || len(e.RightTuple) > 0
}

// Capabilities returns native capabilities implied by the membership edge.
func (e MembershipEdge) Capabilities() []PlanCapability {
	switch e.Kind {
	case MembershipAnti:
		return []PlanCapability{CapabilityAntiMembership, CapabilityBitmapDifference}
	case MembershipSemi:
		return []PlanCapability{CapabilitySemiMembership}
	default:
		return nil
	}
}

// ResidualExpr is an expression evaluated from projected row values.
type ResidualExpr struct {
	Expr   Expr
	Inputs []FieldRef
}

// AggregateMode describes distinct and non-distinct aggregate behavior.
type AggregateMode string

const (
	// AggregateAll keeps normal aggregate semantics.
	AggregateAll AggregateMode = "all"
	// AggregateDistinct applies distinct semantics to aggregate input values.
	AggregateDistinct AggregateMode = "distinct"
)

// Aggregate is a grouped or global aggregate slot.
type Aggregate struct {
	Function      string
	Mode          AggregateMode
	Input         Expr
	Filter        Expr
	Alias         string
	Type          DataType
	Origin        FunctionOrigin
	Placement     FunctionPlacement
	Deterministic bool
}

// ResultKind classifies the client-visible result contract.
type ResultKind string

const (
	// ResultQuery returns rows and column metadata.
	ResultQuery ResultKind = "query"
	// ResultStatement returns an OK/affected-rows style response.
	ResultStatement ResultKind = "statement"
)

// ResultShape describes the planned result visible to the client.
type ResultShape struct {
	Kind         ResultKind
	Columns      []FieldRef
	Hidden       []FieldRef
	OrderBy      []Expr
	Limit        int
	HasLimit     bool
	Offset       int
	Distinct     bool
	Materialized bool
	Statement    StatementResult
}

// HasResultLimit reports whether SQL specified LIMIT, including LIMIT 0.
func (r ResultShape) HasResultLimit() bool {
	return r.HasLimit || r.Limit > 0
}

// AppliesResultWindow reports whether LIMIT/OFFSET semantics must be applied.
func (r ResultShape) AppliesResultWindow() bool {
	return r.HasResultLimit() || r.Offset > 0
}

// StatementNoticeLevel classifies statement warning or note detail.
type StatementNoticeLevel string

const (
	// StatementNoticeWarning means the statement produced a warning.
	StatementNoticeWarning StatementNoticeLevel = "warning"
	// StatementNoticeNote means the statement produced a non-fatal note.
	StatementNoticeNote StatementNoticeLevel = "note"
	// StatementNoticeError means statement diagnostics should be exposed through warning metadata.
	StatementNoticeError StatementNoticeLevel = "error"
)

// StatementNotice describes one protocol-neutral warning or note.
type StatementNotice struct {
	Level    StatementNoticeLevel
	Code     string
	SQLState string
	Message  string
}

// StatementResult describes MySQL OK-packet style metadata for non-row results.
type StatementResult struct {
	AffectedRows   uint64
	LastInsertID   uint64
	Warnings       uint16
	Notices        []StatementNotice
	Status         string
	SessionActions []SessionAction
}

// SessionActionKind describes a session mutation requested by a statement.
type SessionActionKind string

const (
	// SessionActionUseSchema asks the protocol/session owner to change the current schema.
	SessionActionUseSchema SessionActionKind = "use_schema"
	// SessionActionSetVariable asks the protocol/session owner to set a session variable.
	SessionActionSetVariable SessionActionKind = "set_variable"
	// SessionActionSetSQLMode asks the protocol/session owner to replace SQL modes.
	SessionActionSetSQLMode SessionActionKind = "set_sql_mode"
	// SessionActionSetTimeZone asks the protocol/session owner to set the session time zone.
	SessionActionSetTimeZone SessionActionKind = "set_time_zone"
	// SessionActionBeginTransaction asks the protocol/session owner to begin a transaction.
	SessionActionBeginTransaction SessionActionKind = "begin_transaction"
	// SessionActionCommitTransaction asks the protocol/session owner to commit a transaction.
	SessionActionCommitTransaction SessionActionKind = "commit_transaction"
	// SessionActionRollbackTransaction asks the protocol/session owner to roll back a transaction.
	SessionActionRollbackTransaction SessionActionKind = "rollback_transaction"
	// SessionActionResetConnection asks the protocol/session owner to reset session state.
	SessionActionResetConnection SessionActionKind = "reset_connection"
	// SessionActionChangeUser asks the protocol/session owner to replace the authenticated principal.
	SessionActionChangeUser SessionActionKind = "change_user"
	// SessionActionCreateTemporaryTable asks the protocol/session owner to add connection-local table metadata.
	SessionActionCreateTemporaryTable SessionActionKind = "create_temporary_table"
	// SessionActionDropTemporaryTable asks the protocol/session owner to remove connection-local table metadata.
	SessionActionDropTemporaryTable SessionActionKind = "drop_temporary_table"
	// SessionActionInsertTemporaryRows asks the protocol/session owner to append connection-local temporary table rows.
	SessionActionInsertTemporaryRows SessionActionKind = "insert_temporary_rows"
)

// TemporaryTableRow stores one connection-local temporary table row.
type TemporaryTableRow struct {
	Values ResultRow
}

// TemporaryTableData stores connection-local row payloads for a temporary table.
type TemporaryTableData struct {
	Rows []TemporaryTableRow
}

// SessionAction records a requested session change without applying it.
type SessionAction struct {
	Kind  SessionActionKind
	Name  string
	Value string
	Table TableDefinition
	Rows  []TemporaryTableRow
}

// MutationKind classifies the write operation represented by MutationShape.
type MutationKind string

const (
	// MutationUnknown means the statement does not carry mutation metadata.
	MutationUnknown MutationKind = ""
	// MutationInsert describes an INSERT target and row-value shape.
	MutationInsert MutationKind = "insert"
	// MutationUpdate describes an UPDATE target, assignments, and predicates.
	MutationUpdate MutationKind = "update"
	// MutationDelete describes a DELETE target and predicates.
	MutationDelete MutationKind = "delete"
	// MutationTruncate describes a whole-table TRUNCATE target.
	MutationTruncate MutationKind = "truncate"
	// MutationCreateTable describes activating a YAML-backed table schema.
	MutationCreateTable MutationKind = "create_table"
	// MutationDropTable describes dropping an active table schema and data.
	MutationDropTable MutationKind = "drop_table"
	// MutationCreateView describes creating a logical non-materialized view.
	MutationCreateView MutationKind = "create_view"
	// MutationDropView describes dropping a logical non-materialized view.
	MutationDropView MutationKind = "drop_view"
)

// MutationRow describes one parser-neutral row of values for INSERT planning.
type MutationRow struct {
	Values []Expr
}

// MutationAssignment describes one target-field assignment for UPDATE planning.
type MutationAssignment struct {
	Field FieldRef
	Value Expr
}

// MutationShape records write metadata before any execution strategy is chosen.
//
// The shape is intentionally executor-neutral: it captures target table identity,
// resolved target columns, row values, assignments, and predicates so parser and
// planner work can advance without wiring runtime mutation behavior into
// qsbridge yet.
type MutationShape struct {
	Kind                   MutationKind
	Target                 TableInstance
	ViewSQL                string
	Replace                bool
	IfExists               bool
	IfNotExists            bool
	Temporary              bool
	Cascade                bool
	ViewDependencies       []TableInstance
	Columns                []FieldRef
	Rows                   []MutationRow
	Assignments            []MutationAssignment
	Predicates             []Predicate
	DependentRelationships []RelationshipDefinition
}
