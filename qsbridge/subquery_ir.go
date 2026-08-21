package qsbridge

// SubqueryIntentKind classifies subquery work before the planner chooses an
// execution strategy.
type SubqueryIntentKind string

const (
	// SubqueryIntentScalar evaluates a subquery that must produce one scalar value.
	SubqueryIntentScalar SubqueryIntentKind = "scalar_subquery"
	// SubqueryIntentCorrelatedAggregate evaluates an aggregate subquery correlated to outer rows.
	SubqueryIntentCorrelatedAggregate SubqueryIntentKind = "correlated_aggregate_subquery"
	// SubqueryIntentCorrelatedMembership evaluates EXISTS/NOT EXISTS membership correlated to outer rows.
	SubqueryIntentCorrelatedMembership SubqueryIntentKind = "correlated_membership_subquery"
)

// SubqueryHelperIntent describes helper work implied by a subquery intent.
//
// Helper intent is executor-neutral. SQL text may exist as fallback/debug
// context while the planner and executor grow native bitmap implementations.
type SubqueryHelperIntent struct {
	Name               string
	Kind               string
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
}

// ScalarSubqueryIntent records the planner-visible shape of a scalar subquery.
type ScalarSubqueryIntent struct {
	SubquerySQL string
	OutputName  string
	Scope       PredicateScope
}

// CorrelatedAggregateSubqueryIntent records the planner-visible shape of a
// correlated aggregate subquery before it becomes native execution nodes.
type CorrelatedAggregateSubqueryIntent struct {
	AggregateFunction    string
	Factor               float64
	SourcePredicate      string
	OuterValue           FieldRef
	InnerValue           FieldRef
	InnerKey             FieldRef
	OuterKey             FieldRef
	OuterValueRef        string
	InnerValueRef        string
	InnerKeyRef          string
	OuterKeyRef          string
	RequiredFilterFields []FieldRef
	RequiredFilters      []string
	Scope                PredicateScope
}

// CorrelatedMembershipSubqueryIntent records an EXISTS/NOT EXISTS subquery that
// should become a semi/anti membership operation over a correlated domain.
type CorrelatedMembershipSubqueryIntent struct {
	Operation              RelationshipJoinOperationIntent
	OuterDomain            RownumDomain
	InnerDomain            RownumDomain
	OuterKeyRef            string
	InnerKeyRef            string
	CrossDomainPredicates  []string
	RequiredFilters        []string
	OutputName             string
	BitmapNativeTarget     string
	Scope                  PredicateScope
	RepeatedPhysicalSource bool
}

// SubqueryPlanIntent is the durable planner vocabulary for subquery work that
// may still be executed through temporary preflight helpers today.
type SubqueryPlanIntent struct {
	Kind                 SubqueryIntentKind
	Capability           PlanCapability
	HelperIntents        []SubqueryHelperIntent
	Access               []AccessRequirement
	Scalar               *ScalarSubqueryIntent
	CorrelatedAggregate  *CorrelatedAggregateSubqueryIntent
	CorrelatedMembership *CorrelatedMembershipSubqueryIntent
}

// RequiredAccess returns authorization metadata implied by this subquery intent.
func (i SubqueryPlanIntent) RequiredAccess() []AccessRequirement {
	collector := newAccessCollector()
	for _, requirement := range i.Access {
		collector.addRequirement(requirement)
	}
	if i.CorrelatedAggregate != nil {
		collector.addSubqueryField(i.CorrelatedAggregate.OuterValue)
		collector.addSubqueryField(i.CorrelatedAggregate.InnerValue)
		collector.addSubqueryField(i.CorrelatedAggregate.InnerKey)
		collector.addSubqueryField(i.CorrelatedAggregate.OuterKey)
		for _, field := range i.CorrelatedAggregate.RequiredFilterFields {
			collector.addSubqueryField(field)
		}
	}
	if i.CorrelatedMembership != nil {
		collector.ensureSubqueryTable(i.CorrelatedMembership.OuterDomain.Table)
		collector.ensureSubqueryTable(i.CorrelatedMembership.InnerDomain.Table)
	}
	return collector.requirements
}

// Valid reports whether the intent has the shape required by its kind.
func (i SubqueryPlanIntent) Valid() bool {
	switch i.Kind {
	case SubqueryIntentScalar:
		return i.Scalar != nil && i.Scalar.OutputName != ""
	case SubqueryIntentCorrelatedAggregate:
		return i.CorrelatedAggregate != nil &&
			i.CorrelatedAggregate.AggregateFunction != "" &&
			correlatedAggregateFieldName(i.CorrelatedAggregate.InnerKeyRef, i.CorrelatedAggregate.InnerKey) != "" &&
			correlatedAggregateFieldName(i.CorrelatedAggregate.OuterKeyRef, i.CorrelatedAggregate.OuterKey) != ""
	case SubqueryIntentCorrelatedMembership:
		return i.CorrelatedMembership != nil &&
			(i.CorrelatedMembership.Operation == RelationshipJoinOperationSemi || i.CorrelatedMembership.Operation == RelationshipJoinOperationAnti) &&
			i.CorrelatedMembership.OuterDomain.Name() != "" &&
			i.CorrelatedMembership.InnerDomain.Name() != "" &&
			i.CorrelatedMembership.OuterKeyRef != "" &&
			i.CorrelatedMembership.InnerKeyRef != ""
	default:
		return false
	}
}

// HelperKinds returns helper kind names in planner-observed order.
func (i SubqueryPlanIntent) HelperKinds() []string {
	kinds := make([]string, 0, len(i.HelperIntents))
	for _, helper := range i.HelperIntents {
		kinds = append(kinds, helper.Kind)
	}
	return kinds
}

// SubqueryPlanIntentReport is a compact inspection view of subquery planning intent.
type SubqueryPlanIntentReport struct {
	Kind                 SubqueryIntentKind
	Capability           PlanCapability
	HelperKinds          []string
	Scalar               *ScalarSubqueryIntentReport
	CorrelatedAggregate  *CorrelatedAggregateSubqueryIntentReport
	CorrelatedMembership *CorrelatedMembershipSubqueryIntentReport
}

// ScalarSubqueryIntentReport summarizes scalar subquery planning intent.
type ScalarSubqueryIntentReport struct {
	OutputName  string
	SubquerySQL string
	Scope       PredicateScope
}

// CorrelatedAggregateSubqueryIntentReport summarizes correlated aggregate subquery planning intent.
type CorrelatedAggregateSubqueryIntentReport struct {
	AggregateFunction string
	Factor            float64
	SourcePredicate   string
	OuterValueRef     string
	InnerValueRef     string
	InnerKeyRef       string
	OuterKeyRef       string
	RequiredFilters   []string
	Scope             PredicateScope
}

// CorrelatedMembershipSubqueryIntentReport summarizes correlated membership planning intent.
type CorrelatedMembershipSubqueryIntentReport struct {
	Operation              RelationshipJoinOperationIntent
	OuterDomain            string
	InnerDomain            string
	OuterKeyRef            string
	InnerKeyRef            string
	CrossDomainPredicates  []string
	RequiredFilters        []string
	OutputName             string
	BitmapNativeTarget     string
	Scope                  PredicateScope
	RepeatedPhysicalSource bool
}

// Report returns a compact inspection view of the subquery planning intent.
func (i SubqueryPlanIntent) Report() SubqueryPlanIntentReport {
	report := SubqueryPlanIntentReport{
		Kind:        i.Kind,
		Capability:  i.Capability,
		HelperKinds: i.HelperKinds(),
	}
	if i.Scalar != nil {
		report.Scalar = &ScalarSubqueryIntentReport{
			OutputName:  i.Scalar.OutputName,
			SubquerySQL: i.Scalar.SubquerySQL,
			Scope:       i.Scalar.Scope,
		}
	}
	if i.CorrelatedAggregate != nil {
		report.CorrelatedAggregate = &CorrelatedAggregateSubqueryIntentReport{
			AggregateFunction: i.CorrelatedAggregate.AggregateFunction,
			Factor:            i.CorrelatedAggregate.Factor,
			SourcePredicate:   i.CorrelatedAggregate.SourcePredicate,
			OuterValueRef:     correlatedAggregateFieldName(i.CorrelatedAggregate.OuterValueRef, i.CorrelatedAggregate.OuterValue),
			InnerValueRef:     correlatedAggregateFieldName(i.CorrelatedAggregate.InnerValueRef, i.CorrelatedAggregate.InnerValue),
			InnerKeyRef:       correlatedAggregateFieldName(i.CorrelatedAggregate.InnerKeyRef, i.CorrelatedAggregate.InnerKey),
			OuterKeyRef:       correlatedAggregateFieldName(i.CorrelatedAggregate.OuterKeyRef, i.CorrelatedAggregate.OuterKey),
			RequiredFilters:   correlatedAggregateRequiredFilterNames(*i.CorrelatedAggregate),
			Scope:             i.CorrelatedAggregate.Scope,
		}
	}
	if i.CorrelatedMembership != nil {
		report.CorrelatedMembership = &CorrelatedMembershipSubqueryIntentReport{
			Operation:              i.CorrelatedMembership.Operation,
			OuterDomain:            i.CorrelatedMembership.OuterDomain.Name(),
			InnerDomain:            i.CorrelatedMembership.InnerDomain.Name(),
			OuterKeyRef:            i.CorrelatedMembership.OuterKeyRef,
			InnerKeyRef:            i.CorrelatedMembership.InnerKeyRef,
			CrossDomainPredicates:  append([]string(nil), i.CorrelatedMembership.CrossDomainPredicates...),
			RequiredFilters:        append([]string(nil), i.CorrelatedMembership.RequiredFilters...),
			OutputName:             i.CorrelatedMembership.OutputName,
			BitmapNativeTarget:     i.CorrelatedMembership.BitmapNativeTarget,
			Scope:                  i.CorrelatedMembership.Scope,
			RepeatedPhysicalSource: i.CorrelatedMembership.RepeatedPhysicalSource,
		}
	}
	return report
}

func correlatedAggregateFieldName(fallback string, field FieldRef) string {
	if fallback != "" {
		return fallback
	}
	if field.Name == "" {
		return ""
	}
	return field.QualifiedName()
}

func correlatedAggregateRequiredFilterNames(intent CorrelatedAggregateSubqueryIntent) []string {
	if len(intent.RequiredFilters) > 0 {
		return append([]string(nil), intent.RequiredFilters...)
	}
	return qualifiedFieldNames(intent.RequiredFilterFields)
}
