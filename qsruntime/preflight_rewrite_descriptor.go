package qsruntime

import (
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// PreflightRewriteTextRange records the byte range of SQL text recognized by a
// temporary preflight rewrite.
type PreflightRewriteTextRange struct {
	Start int
	End   int
}

// PreflightRewriteDescriptorAttribute records one typed attribute extracted
// from a temporary preflight rewrite shape.
type PreflightRewriteDescriptorAttribute struct {
	Name  string
	Value string
}

// PreflightRewriteDescriptorSummary is a parser-adjacent description of a
// temporary SQL rewrite shape. It gives tests and future planner work a typed
// surface to target before the real IR nodes exist.
type PreflightRewriteDescriptorSummary struct {
	Rule                qsbridge.RewriteRuleID
	SourceSQLShape      string
	ReplacementSQLShape string
	Range               PreflightRewriteTextRange
	Attributes          []PreflightRewriteDescriptorAttribute
	Fields              []qsbridge.FieldRef
	HelperPlans         []PreflightRewriteHelperPlanDescriptor
	NativeSteps         []qsbridge.NativeSubqueryStep
	SubqueryIntent      *qsbridge.SubqueryPlanIntent
}

// PreflightRewriteHelperPlanKind identifies one temporary helper execution
// shape used by a preflight rewrite.
type PreflightRewriteHelperPlanKind string

const (
	// PreflightHelperPlanScalarSubquery evaluates a scalar subquery once.
	PreflightHelperPlanScalarSubquery PreflightRewriteHelperPlanKind = "scalar_subquery"
	// PreflightHelperPlanParentKeyLookup finds parent keys required by a correlated helper.
	PreflightHelperPlanParentKeyLookup PreflightRewriteHelperPlanKind = "parent_key_lookup"
	// PreflightHelperPlanAggregateThresholdLookup computes per-key aggregate thresholds.
	PreflightHelperPlanAggregateThresholdLookup PreflightRewriteHelperPlanKind = "aggregate_threshold_lookup"
)

// PreflightRewriteHelperPlanDescriptor describes a temporary helper query that
// should eventually become a bitmap-native helper plan.
type PreflightRewriteHelperPlanDescriptor struct {
	Name               string
	Kind               PreflightRewriteHelperPlanKind
	Purpose            string
	SQL                string
	Inputs             []string
	Outputs            []string
	InputFields        []qsbridge.FieldRef
	OutputFields       []qsbridge.FieldRef
	Materialization    string
	BitmapNativeTarget string
	Lifecycle          qsbridge.SubqueryStepLifecycle
	NativeStep         *qsbridge.NativeSubqueryStep
}

// PreflightRewriteDescriptorCompletenessReport records whether descriptor metadata is complete enough for inspection and future planner replacement work.
type PreflightRewriteDescriptorCompletenessReport struct {
	Complete bool
	Rule     qsbridge.RewriteRuleID
	Missing  []string
	Helpers  []PreflightRewriteHelperPlanCompletenessReport
}

// PreflightRewriteHelperPlanCompletenessReport records completeness for one helper descriptor.
type PreflightRewriteHelperPlanCompletenessReport struct {
	Complete bool
	Name     string
	Kind     PreflightRewriteHelperPlanKind
	Missing  []string
}

// CompletenessReport checks that temporary descriptor metadata has the stable fields needed by inspection and planner migration.
func (d PreflightRewriteDescriptorSummary) CompletenessReport() PreflightRewriteDescriptorCompletenessReport {
	report := PreflightRewriteDescriptorCompletenessReport{
		Complete: true,
		Rule:     d.Rule,
	}
	if d.Rule == "" {
		report.Missing = append(report.Missing, "rule")
	}
	if strings.TrimSpace(d.SourceSQLShape) == "" {
		report.Missing = append(report.Missing, "source_sql_shape")
	}
	if strings.TrimSpace(d.ReplacementSQLShape) == "" {
		report.Missing = append(report.Missing, "replacement_sql_shape")
	}
	if d.Range.End <= d.Range.Start {
		report.Missing = append(report.Missing, "text_range")
	}
	if d.SubqueryIntent == nil || !d.SubqueryIntent.Valid() {
		report.Missing = append(report.Missing, "subquery_intent")
	}
	if len(d.HelperPlans) == 0 {
		report.Missing = append(report.Missing, "helper_plans")
	}
	for _, helper := range d.HelperPlans {
		helperReport := helper.CompletenessReport()
		if !helperReport.Complete {
			report.Complete = false
		}
		report.Helpers = append(report.Helpers, helperReport)
	}
	if len(report.Missing) > 0 {
		report.Complete = false
	}
	return report
}

// Complete reports whether descriptor metadata has all required inspection fields.
func (d PreflightRewriteDescriptorSummary) Complete() bool {
	return d.CompletenessReport().Complete
}

// CompletenessReport checks that one helper descriptor has stable identity, output, and planning metadata.
func (p PreflightRewriteHelperPlanDescriptor) CompletenessReport() PreflightRewriteHelperPlanCompletenessReport {
	report := PreflightRewriteHelperPlanCompletenessReport{
		Complete: true,
		Name:     p.Name,
		Kind:     p.Kind,
	}
	if strings.TrimSpace(p.Name) == "" {
		report.Missing = append(report.Missing, "name")
	}
	if p.Kind == "" {
		report.Missing = append(report.Missing, "kind")
	}
	if strings.TrimSpace(p.Purpose) == "" {
		report.Missing = append(report.Missing, "purpose")
	}
	if len(p.Outputs) == 0 {
		report.Missing = append(report.Missing, "outputs")
	}
	if strings.TrimSpace(p.Materialization) == "" {
		report.Missing = append(report.Missing, "materialization")
	}
	if strings.TrimSpace(p.BitmapNativeTarget) == "" {
		report.Missing = append(report.Missing, "bitmap_native_target")
	}
	if len(report.Missing) > 0 {
		report.Complete = false
	}
	return report
}

func preflightRewriteDescriptor(rule qsbridge.RewriteRuleID, sql string) (*PreflightRewriteDescriptorSummary, bool) {
	switch rule {
	case qsbridge.RewriteCorrelatedAggregatePreflight:
		if descriptor, ok := findCorrelatedAverageQuantityPredicate(sql); ok {
			summary := descriptor.descriptorSummary()
			return &summary, true
		}
	}
	return nil, false
}

func correlatedParentKeyHelperPlan(partAlias string, sql string) PreflightRewriteHelperPlanDescriptor {
	step := correlatedParentKeyNativeStep(partAlias)
	return PreflightRewriteHelperPlanDescriptor{
		Name:    "correlated_parent_keys",
		Kind:    PreflightHelperPlanParentKeyLookup,
		Purpose: "resolve the filtered parent part keys that seed the correlated aggregate threshold lookup",
		SQL:     sql,
		Inputs:  []string{partAlias + ".p_brand", partAlias + ".p_container"},
		Outputs: []string{partAlias + ".p_partkey"},
		InputFields: []qsbridge.FieldRef{
			rewriteDescriptorField(partAlias+".p_brand", "part"),
			rewriteDescriptorField(partAlias+".p_container", "part"),
		},
		OutputFields:       []qsbridge.FieldRef{rewriteDescriptorField(partAlias+".p_partkey", "part")},
		Materialization:    "parent key set",
		BitmapNativeTarget: "bitmap-filtered parent key rownum set",
		Lifecycle:          qsbridge.SubqueryStepNativeReady,
		NativeStep:         &step,
	}
}

func correlatedParentKeyNativeStep(partAlias string) qsbridge.NativeSubqueryStep {
	return qsbridge.NativeSubqueryStep{
		Name:               "correlated_parent_keys",
		Kind:               qsbridge.NativeSubqueryStepParentKeyLookup,
		Lifecycle:          qsbridge.SubqueryStepNativeReady,
		SubqueryKind:       qsbridge.SubqueryIntentCorrelatedAggregate,
		Inputs:             []string{partAlias + ".p_brand", partAlias + ".p_container"},
		Outputs:            []string{partAlias + ".p_partkey"},
		Materialization:    "parent key set",
		BitmapNativeTarget: "bitmap-filtered parent key rownum set",
		ExecutionMode:      "sql_backed_until_bitmap_native_executor_exists",
	}
}

func correlatedThresholdHelperPlan(sql string, inputs []string, outputs []string) PreflightRewriteHelperPlanDescriptor {
	step := correlatedThresholdNativeStep(inputs, outputs)
	return PreflightRewriteHelperPlanDescriptor{
		Name:               "correlated_average_thresholds",
		Kind:               PreflightHelperPlanAggregateThresholdLookup,
		Purpose:            "compute per-part aggregate thresholds for the correlated quantity predicate",
		SQL:                sql,
		Inputs:             append([]string(nil), inputs...),
		Outputs:            append([]string(nil), outputs...),
		InputFields:        rewriteDescriptorFields(inputs, "lineitem"),
		OutputFields:       rewriteDescriptorFields(firstQualifiedReferences(outputs), "part"),
		Materialization:    "per-key aggregate threshold map",
		BitmapNativeTarget: "aggregate-threshold helper kernel feeding bitmap predicate branches",
		Lifecycle:          qsbridge.SubqueryStepNativeReady,
		NativeStep:         &step,
	}
}

func correlatedThresholdNativeStep(inputs []string, outputs []string) qsbridge.NativeSubqueryStep {
	return qsbridge.NativeSubqueryStep{
		Name:               "correlated_average_thresholds",
		Kind:               qsbridge.NativeSubqueryStepAggregateThresholdLookup,
		Lifecycle:          qsbridge.SubqueryStepNativeReady,
		SubqueryKind:       qsbridge.SubqueryIntentCorrelatedAggregate,
		Inputs:             append([]string(nil), inputs...),
		Outputs:            append([]string(nil), outputs...),
		Materialization:    "per-key aggregate threshold map",
		BitmapNativeTarget: "aggregate-threshold helper kernel feeding bitmap predicate branches",
		ExecutionMode:      "sql_backed_until_bitmap_native_executor_exists",
	}
}

func (d correlatedAverageQuantityDescriptor) descriptorSummary() PreflightRewriteDescriptorSummary {
	attributes := []PreflightRewriteDescriptorAttribute{
		{Name: "aggregate_function", Value: d.AggregateFunction},
		{Name: "factor", Value: strconv.FormatFloat(d.Factor, 'g', -1, 64)},
		{Name: "outer_lineitem_alias", Value: d.OuterLineitem},
		{Name: "inner_lineitem_alias", Value: d.InnerLineitem},
		{Name: "outer_part_alias", Value: d.OuterPart},
		{Name: "inner_correlated_key", Value: d.InnerKey.qualifiedName()},
		{Name: "outer_correlated_key", Value: d.OuterKey.qualifiedName()},
	}
	for _, filter := range d.RequiredFilters {
		attributes = append(attributes, PreflightRewriteDescriptorAttribute{Name: "required_filter", Value: filter.qualifiedName()})
	}

	fields := []qsbridge.FieldRef{
		d.OuterQuantity.fieldRef(),
		d.InnerQuantity.fieldRef(),
		d.InnerKey.fieldRef(),
		d.OuterKey.fieldRef(),
	}
	fields = append(fields, correlatedFieldRefs(d.RequiredFilters)...)

	parentPlan := correlatedParentKeyHelperPlan(d.OuterPart, "")
	thresholdPlan := correlatedThresholdHelperPlan(
		"",
		[]string{d.InnerKey.qualifiedName(), d.InnerQuantity.qualifiedName()},
		[]string{d.OuterKey.qualifiedName(), "threshold"},
	)

	return PreflightRewriteDescriptorSummary{
		Rule:                qsbridge.RewriteCorrelatedAggregatePreflight,
		SourceSQLShape:      "predicate(correlated_aggregate_subquery)",
		ReplacementSQLShape: "predicate(disjunction(per_key_thresholds))",
		Range: PreflightRewriteTextRange{
			Start: d.Start,
			End:   d.End,
		},
		Attributes:     attributes,
		Fields:         fields,
		HelperPlans:    []PreflightRewriteHelperPlanDescriptor{parentPlan, thresholdPlan},
		NativeSteps:    []qsbridge.NativeSubqueryStep{*parentPlan.NativeStep, *thresholdPlan.NativeStep},
		SubqueryIntent: ptrSubqueryPlanIntent(d.subqueryPlanIntent()),
	}
}

func (d correlatedAverageQuantityDescriptor) subqueryPlanIntent() qsbridge.SubqueryPlanIntent {
	helperPlans := []PreflightRewriteHelperPlanDescriptor{
		correlatedParentKeyHelperPlan(d.OuterPart, ""),
		correlatedThresholdHelperPlan("", []string{d.InnerKey.qualifiedName(), d.InnerQuantity.qualifiedName()}, []string{d.OuterKey.qualifiedName(), "threshold"}),
	}
	helperIntents := make([]qsbridge.SubqueryHelperIntent, 0, len(helperPlans))
	for _, helper := range helperPlans {
		helperIntents = append(helperIntents, helperPlanIntent(helper))
	}
	return qsbridge.SubqueryPlanIntent{
		Kind:       qsbridge.SubqueryIntentCorrelatedAggregate,
		Capability: qsbridge.CapabilityScalarSubquery,
		CorrelatedAggregate: &qsbridge.CorrelatedAggregateSubqueryIntent{
			AggregateFunction: d.AggregateFunction,
			Factor:            d.Factor,
			OuterValueRef:     d.OuterQuantity.qualifiedName(),
			InnerValueRef:     d.InnerQuantity.qualifiedName(),
			InnerKeyRef:       d.InnerKey.qualifiedName(),
			OuterKeyRef:       d.OuterKey.qualifiedName(),
			RequiredFilters:   correlatedQualifiedNames(d.RequiredFilters),
			Scope:             qsbridge.PredicateScopeWhere,
		},
		HelperIntents: helperIntents,
	}
}

func rewriteDescriptorField(reference string, table string) qsbridge.FieldRef {
	alias, field := splitRewriteDescriptorReference(reference)
	return qsbridge.FieldRef{
		Table: qsbridge.TableInstance{
			Table: table,
			Alias: alias,
		},
		Name:         field,
		PhysicalName: field,
	}
}

func rewriteDescriptorFields(references []string, table string) []qsbridge.FieldRef {
	fields := make([]qsbridge.FieldRef, 0, len(references))
	for _, reference := range references {
		if !strings.Contains(reference, ".") {
			continue
		}
		fields = append(fields, rewriteDescriptorField(reference, table))
	}
	return fields
}

func firstQualifiedReferences(references []string) []string {
	for _, reference := range references {
		if strings.Contains(reference, ".") {
			return []string{reference}
		}
	}
	return nil
}

func splitRewriteDescriptorReference(reference string) (string, string) {
	reference = strings.TrimSpace(reference)
	parts := strings.SplitN(reference, ".", 2)
	if len(parts) != 2 {
		return "", reference
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func descriptorAttributeValue(summary PreflightRewriteDescriptorSummary, name string) (string, bool) {
	for _, attribute := range summary.Attributes {
		if attribute.Name == name {
			return attribute.Value, true
		}
	}
	return "", false
}

func descriptorQualifiedFieldNames(fields []qsbridge.FieldRef) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.QualifiedName())
	}
	return names
}

func descriptorHasQualifiedField(fields []qsbridge.FieldRef, name string) bool {
	for _, field := range fields {
		if field.QualifiedName() == name {
			return true
		}
	}
	return false
}

func helperPlanIntent(plan PreflightRewriteHelperPlanDescriptor) qsbridge.SubqueryHelperIntent {
	return qsbridge.SubqueryHelperIntent{
		Name:               plan.Name,
		Kind:               string(plan.Kind),
		Inputs:             append([]string(nil), plan.Inputs...),
		Outputs:            append([]string(nil), plan.Outputs...),
		Materialization:    plan.Materialization,
		BitmapNativeTarget: plan.BitmapNativeTarget,
	}
}

func ptrSubqueryPlanIntent(intent qsbridge.SubqueryPlanIntent) *qsbridge.SubqueryPlanIntent {
	return &intent
}

// PreflightRewriteDescriptorReport is a compact inspection view of a temporary
// preflight rewrite descriptor.
type PreflightRewriteDescriptorReport struct {
	Rule                     qsbridge.RewriteRuleID
	SourceSQLShape           string
	ReplacementSQLShape      string
	Start                    int
	End                      int
	Attributes               []string
	Fields                   []string
	NativeReplacementReady   bool
	NativeReplacementMissing []string
	HelperPlans              []PreflightRewriteHelperPlanReport
	NativeSteps              []qsbridge.NativeSubqueryStepReport
	SubqueryIntent           *qsbridge.SubqueryPlanIntentReport
}

// PreflightRewriteHelperPlanReport is a compact inspection view of one
// temporary helper plan descriptor.
type PreflightRewriteHelperPlanReport struct {
	Name                     string
	Kind                     PreflightRewriteHelperPlanKind
	Inputs                   []string
	Outputs                  []string
	InputFields              []string
	OutputFields             []string
	Materialization          string
	BitmapNativeTarget       string
	NativeReplacementReady   bool
	NativeReplacementMissing []string
	Lifecycle                qsbridge.SubqueryStepLifecycle
	NativeStep               *qsbridge.NativeSubqueryStepReport
}

// Report returns a compact inspection view suitable for explain and diagnostic
// surfaces that should not dump the full descriptor structure.
func (d PreflightRewriteDescriptorSummary) Report() PreflightRewriteDescriptorReport {
	attributes := make([]string, 0, len(d.Attributes))
	for _, attribute := range d.Attributes {
		attributes = append(attributes, attribute.Name)
	}
	helperPlans := make([]PreflightRewriteHelperPlanReport, 0, len(d.HelperPlans))
	for _, plan := range d.HelperPlans {
		helperPlans = append(helperPlans, plan.Report())
	}
	nativeSteps := make([]qsbridge.NativeSubqueryStepReport, 0, len(d.NativeSteps))
	for _, step := range d.NativeSteps {
		nativeSteps = append(nativeSteps, step.Report())
	}
	var subqueryIntent *qsbridge.SubqueryPlanIntentReport
	if d.SubqueryIntent != nil {
		report := d.SubqueryIntent.Report()
		subqueryIntent = &report
	}
	readiness := d.CompletenessReport()
	return PreflightRewriteDescriptorReport{
		Rule:                     d.Rule,
		SourceSQLShape:           d.SourceSQLShape,
		ReplacementSQLShape:      d.ReplacementSQLShape,
		Start:                    d.Range.Start,
		End:                      d.Range.End,
		Attributes:               attributes,
		Fields:                   descriptorQualifiedFieldNames(d.Fields),
		NativeReplacementReady:   readiness.Complete,
		NativeReplacementMissing: append([]string(nil), readiness.Missing...),
		HelperPlans:              helperPlans,
		NativeSteps:              nativeSteps,
		SubqueryIntent:           subqueryIntent,
	}
}

// Report returns a compact inspection view of one temporary helper plan.
func (p PreflightRewriteHelperPlanDescriptor) Report() PreflightRewriteHelperPlanReport {
	readiness := p.CompletenessReport()
	lifecycle := p.Lifecycle
	if lifecycle == "" {
		lifecycle = qsbridge.SubqueryStepCompatibility
	}
	var nativeStep *qsbridge.NativeSubqueryStepReport
	if p.NativeStep != nil {
		report := p.NativeStep.Report()
		nativeStep = &report
	}
	return PreflightRewriteHelperPlanReport{
		Name:                     p.Name,
		Kind:                     p.Kind,
		Inputs:                   append([]string(nil), p.Inputs...),
		Outputs:                  append([]string(nil), p.Outputs...),
		InputFields:              descriptorQualifiedFieldNames(p.InputFields),
		OutputFields:             descriptorQualifiedFieldNames(p.OutputFields),
		Materialization:          p.Materialization,
		BitmapNativeTarget:       p.BitmapNativeTarget,
		NativeReplacementReady:   readiness.Complete,
		NativeReplacementMissing: append([]string(nil), readiness.Missing...),
		Lifecycle:                lifecycle,
		NativeStep:               nativeStep,
	}
}

// DescriptorReport returns a compact inspection view when a preflight decision
// recognized a temporary rewrite descriptor.
func (i PreflightRewriteInspection) DescriptorReport() (PreflightRewriteDescriptorReport, bool) {
	if i.Descriptor == nil {
		return PreflightRewriteDescriptorReport{}, false
	}
	return i.Descriptor.Report(), true
}

// DescriptorReports returns compact reports for every preflight rewrite
// decision that recognized a temporary rewrite descriptor.
func (s PreflightRewriteSummary) DescriptorReports() []PreflightRewriteDescriptorReport {
	reports := make([]PreflightRewriteDescriptorReport, 0)
	for _, rewrite := range s.Rewrites {
		report, ok := rewrite.DescriptorReport()
		if !ok {
			continue
		}
		reports = append(reports, report)
	}
	return reports
}

// HelperExecutionReports returns compact request reports for every helper
// execution produced by applied preflight rewrites.
func (s PreflightRewriteSummary) HelperExecutionReports() []PreflightHelperExecutionRequestReport {
	reports := make([]PreflightHelperExecutionRequestReport, 0)
	for _, rewrite := range s.Rewrites {
		reports = append(reports, rewrite.HelperReports...)
	}
	return reports
}
