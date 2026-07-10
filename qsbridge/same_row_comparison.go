package qsbridge

import (
	"context"
	"strconv"
)

// SameRowComparisonKind describes the native primitive used for a row-local comparison.
type SameRowComparisonKind string

const (
	// SameRowComparisonUnknown means the comparison has no native primitive yet.
	SameRowComparisonUnknown SameRowComparisonKind = ""
	// SameRowComparisonBSI compares two BSI-backed fields for the same rownum domain.
	SameRowComparisonBSI SameRowComparisonKind = "bsi"
)

// SameRowComparisonStageKind names one executor primitive in a same-row comparison lifecycle.
type SameRowComparisonStageKind string

const (
	// SameRowComparisonStageSeedCandidates starts from predicate-produced candidate rownums.
	SameRowComparisonStageSeedCandidates SameRowComparisonStageKind = "seed_candidates"
	// SameRowComparisonStageCompareBSIFields compares two BSI-backed fields in one rownum domain.
	SameRowComparisonStageCompareBSIFields SameRowComparisonStageKind = "compare_bsi_fields"
	// SameRowComparisonStageReturnRownums returns matching rownums without materializing compared values.
	SameRowComparisonStageReturnRownums SameRowComparisonStageKind = "return_rownums"
)

// SameRowComparisonRequest describes a field-vs-field comparison over one rownum domain.
//
// The request intentionally returns rownums instead of materialized values. It
// is the native kernel target for predicates such as l_receiptdate > l_commitdate.
type SameRowComparisonRequest struct {
	ID              string
	ProbePrefix     string
	Domain          RownumDomainSet
	Left            FieldRef
	Right           FieldRef
	Operator        BinaryOp
	Kind            SameRowComparisonKind
	Cacheable       bool
	FromEpochMillis int64
	ToEpochMillis   int64
}

// CandidateCount reports how many rownums enter the comparison.
func (r SameRowComparisonRequest) CandidateCount() int {
	return r.Domain.CandidateCount()
}

// SameRowComparisonResult is the protocol-neutral output from a same-row comparison kernel.
type SameRowComparisonResult struct {
	ID          string
	Domain      RownumDomainSet
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// CandidateCount reports how many rownums survived the comparison.
func (r SameRowComparisonResult) CandidateCount() int {
	return r.Domain.CandidateCount()
}

// SameRowComparisonKernel executes native same-row comparison primitives.
type SameRowComparisonKernel interface {
	CompareSameRowFields(context.Context, SameRowComparisonRequest) (SameRowComparisonResult, error)
}

// SameRowComparisonPlan describes a recognized same-row comparison predicate.
type SameRowComparisonPlan struct {
	ID              string
	ProbeName       string
	Left            FieldRef
	Right           FieldRef
	Operator        BinaryOp
	Kind            SameRowComparisonKind
	Domain          RownumDomain
	PredicateScope  PredicateScope
	PredicateIndex  int
	FromEpochMillis int64
	ToEpochMillis   int64
}

// SameRowComparisonExecutionStep describes one ordered same-row comparison handoff.
type SameRowComparisonExecutionStep struct {
	ID          string
	Kind        SameRowComparisonStageKind
	ProbePrefix string
	Request     SameRowComparisonRequest
}

// SameRowComparisonExecutionPlan sketches the runnable same-row comparison lifecycle.
type SameRowComparisonExecutionPlan struct {
	Comparison SameRowComparisonPlan
	Stages     []SameRowComparisonExecutionStep
}

// Request builds a kernel request for candidate rownums.
func (p SameRowComparisonPlan) Request(candidates []QuantaRownum) SameRowComparisonRequest {
	return SameRowComparisonRequest{
		ID:          p.ID,
		ProbePrefix: p.ProbeName + "_",
		Domain: RownumDomainSet{
			Domain:  p.Domain,
			Rownums: append([]QuantaRownum(nil), candidates...),
		},
		Left:            p.Left,
		Right:           p.Right,
		Operator:        p.Operator,
		Kind:            p.Kind,
		Cacheable:       true,
		FromEpochMillis: p.FromEpochMillis,
		ToEpochMillis:   p.ToEpochMillis,
	}
}

// ExecutionPlan expands this comparison into ordered native executor handoffs.
func (p SameRowComparisonPlan) ExecutionPlan(candidates []QuantaRownum) SameRowComparisonExecutionPlan {
	request := p.Request(candidates)
	return SameRowComparisonExecutionPlan{
		Comparison: p,
		Stages: []SameRowComparisonExecutionStep{
			{
				ID:          p.ID + ".seed",
				Kind:        SameRowComparisonStageSeedCandidates,
				ProbePrefix: p.ProbeName + "_seed_",
			},
			{
				ID:          p.ID + ".compare",
				Kind:        SameRowComparisonStageCompareBSIFields,
				ProbePrefix: p.ProbeName + "_compare_",
				Request:     request,
			},
			{
				ID:          p.ID + ".return",
				Kind:        SameRowComparisonStageReturnRownums,
				ProbePrefix: p.ProbeName + "_return_",
			},
		},
	}
}

// SameRowComparisonPlans finds native same-row comparison candidates in query predicates.
func SameRowComparisonPlans(query QueryIR) []SameRowComparisonPlan {
	plans := make([]SameRowComparisonPlan, 0)
	plans = append(plans, sameRowComparisonPlansForPredicates(query.Predicates, PredicateScopeWhere, len(plans))...)
	plans = append(plans, sameRowComparisonPlansForPredicates(query.Having, PredicateScopeHaving, len(plans))...)
	return plans
}

func sameRowComparisonPlansForPredicates(predicates []Predicate, fallbackScope PredicateScope, offset int) []SameRowComparisonPlan {
	plans := make([]SameRowComparisonPlan, 0)
	for index, predicate := range predicates {
		left, right, op, ok := SameRowBSIComparisonPredicate(predicate)
		if !ok {
			continue
		}
		scope := predicate.Scope
		if scope == PredicateScopeUnknown {
			scope = fallbackScope
		}
		id := sameRowComparisonID(offset+len(plans)+1, left, right)
		plans = append(plans, SameRowComparisonPlan{
			ID:             id,
			ProbeName:      projectorProbeName(id),
			Left:           left,
			Right:          right,
			Operator:       op,
			Kind:           SameRowComparisonBSI,
			Domain:         relationshipDomainForField(left),
			PredicateScope: scope,
			PredicateIndex: index,
		})
	}
	return plans
}

// SameRowBSIComparisonPredicate reports whether predicate can become a same-row BSI comparison.
func SameRowBSIComparisonPredicate(predicate Predicate) (FieldRef, FieldRef, BinaryOp, bool) {
	binary, ok := asBinaryExpr(predicate.Expr)
	if !ok || !sameRowComparisonOperator(binary.Op) {
		return FieldRef{}, FieldRef{}, "", false
	}
	leftExpr, ok := asFieldExpr(binary.Left)
	if !ok || !sameRowComparisonBSIField(leftExpr.Ref) {
		return FieldRef{}, FieldRef{}, "", false
	}
	rightExpr, ok := asFieldExpr(binary.Right)
	if !ok || !sameRowComparisonBSIField(rightExpr.Ref) {
		return FieldRef{}, FieldRef{}, "", false
	}
	left := leftExpr.Ref
	right := rightExpr.Ref
	if !sameRownumDomain(left, right) {
		return FieldRef{}, FieldRef{}, "", false
	}
	return left, right, binary.Op, true
}

func sameRowComparisonOperator(op BinaryOp) bool {
	switch op {
	case BinaryOpEqual,
		BinaryOpNotEqual,
		BinaryOpLess,
		BinaryOpLessEqual,
		BinaryOpGreater,
		BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func sameRowComparisonBSIField(field FieldRef) bool {
	if field.Index == IndexBSI || field.Index == IndexDateTime {
		return true
	}
	switch field.Type {
	case DataTypeInt, DataTypeFloat, DataTypeTime:
		return true
	default:
		return false
	}
}

func sameRownumDomain(left FieldRef, right FieldRef) bool {
	leftDomain := relationshipDomainForField(left)
	rightDomain := relationshipDomainForField(right)
	return leftDomain.Name() != "" && leftDomain.Name() == rightDomain.Name()
}

func sameRowComparisonID(sequence int, left FieldRef, right FieldRef) string {
	leftName := left.QualifiedName()
	if leftName == "" {
		leftName = "left"
	}
	rightName := right.QualifiedName()
	if rightName == "" {
		rightName = "right"
	}
	return "same_row_comparison." + strconv.Itoa(sequence) + "." + leftName + "." + rightName
}
