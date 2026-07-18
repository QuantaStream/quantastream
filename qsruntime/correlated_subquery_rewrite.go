package qsruntime

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type correlatedAverageQuantityDescriptor struct {
	Start             int
	End               int
	OuterLineitem     string
	InnerLineitem     string
	OuterPart         string
	Factor            float64
	AggregateFunction string
	OuterQuantity     correlatedSubqueryField
	InnerQuantity     correlatedSubqueryField
	InnerKey          correlatedSubqueryField
	OuterKey          correlatedSubqueryField
	RequiredFilters   []correlatedSubqueryField
}

type correlatedSubqueryField struct {
	Table string
	Alias string
	Name  string
	Type  qsbridge.DataType
}

func correlatedField(table string, alias string, name string, dataType qsbridge.DataType) correlatedSubqueryField {
	return correlatedSubqueryField{
		Table: table,
		Alias: alias,
		Name:  name,
		Type:  dataType,
	}
}

func (f correlatedSubqueryField) qualifiedName() string {
	if f.Alias != "" {
		return f.Alias + "." + f.Name
	}
	if f.Table != "" {
		return f.Table + "." + f.Name
	}
	return f.Name
}

func (f correlatedSubqueryField) fieldRef() qsbridge.FieldRef {
	return qsbridge.FieldRef{
		Table: qsbridge.TableInstance{
			Table: f.Table,
			Alias: f.Alias,
		},
		Name:         f.Name,
		PhysicalName: f.Name,
		Type:         f.Type,
	}
}

func correlatedQualifiedNames(fields []correlatedSubqueryField) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.qualifiedName())
	}
	return names
}

func correlatedFieldRefs(fields []correlatedSubqueryField) []qsbridge.FieldRef {
	refs := make([]qsbridge.FieldRef, 0, len(fields))
	for _, field := range fields {
		refs = append(refs, field.fieldRef())
	}
	return refs
}

type correlatedAverageQuantitySQLMatch struct {
	Descriptor           correlatedAverageQuantityDescriptor
	PartBrand            string
	PartContainer        string
	RequiredFiltersFound bool
}

func (m correlatedAverageQuantitySQLMatch) requiredPartFilters() (string, string, bool) {
	return m.PartBrand, m.PartContainer, m.RequiredFiltersFound
}

type q17PartKeySeed struct {
	PartKey      int64
	ParentRownum qsbridge.QuantaRownum
}

type q17PartThreshold struct {
	PartKey   int64
	Threshold float64
}

func correlatedAverageRewriteTrace() qsbridge.OptimizationTrace {
	trace := qsbridge.NewOptimizationTrace()
	trace.Add(qsbridge.RewriteAppliedRecord(
		qsbridge.RewriteCorrelatedAggregatePreflight,
		"correlated aggregate subquery intent is not planner-native yet; expanded average quantity subquery into per-key threshold predicates before native planning",
		"predicate(correlated_aggregate_subquery)",
		"predicate(disjunction(per_key_thresholds))",
	))
	return trace
}

func (r SQLRuntime) rewriteCorrelatedAverageQuantitySubquery(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (string, qsbridge.DiagnosticSet, qsbridge.OptimizationTrace, []PreflightHelperExecutionRequestReport, error, bool) {
	match, ok := r.correlatedAverageQuantityRewriteMatch(sql)
	if !ok {
		return "", nil, qsbridge.OptimizationTrace{}, nil, nil, false
	}
	descriptor := match.Descriptor
	brand, container, ok := match.requiredPartFilters()
	if !ok {
		return "", qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCorrelatedAggregateSubquery, qsbridge.PhasePlan, "correlated average rewrite requires part brand and container filters")}, qsbridge.OptimizationTrace{}, nil, nil, true
	}
	partSeeds, diagnostics, partKeyReports, err := r.correlatedAveragePartKeySeeds(ctx, descriptor.OuterPart, brand, container, options, values...)
	helperReports := append([]PreflightHelperExecutionRequestReport(nil), partKeyReports...)
	if err != nil || diagnostics.BlocksNative() {
		return "", diagnostics, qsbridge.OptimizationTrace{}, helperReports, err, true
	}
	thresholds, diagnostics, thresholdReports, err := r.correlatedAverageThresholdsForSeeds(ctx, partSeeds, descriptor.Factor, options, values...)
	helperReports = append(helperReports, thresholdReports...)
	if err != nil || diagnostics.BlocksNative() {
		return "", diagnostics, qsbridge.OptimizationTrace{}, helperReports, err, true
	}
	predicate, ok := correlatedAverageThresholdPredicateSQL(correlatedAverageThresholdPredicateExpr(descriptor, thresholds))
	if !ok {
		return "", qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCorrelatedAggregateSubquery, qsbridge.PhasePlan, "correlated average rewrite could not render threshold predicate expression")}, qsbridge.OptimizationTrace{}, helperReports, nil, true
	}
	rewritten := rewriteCorrelatedAveragePredicateSQL(sql, descriptor, predicate)
	return rewritten, nil, correlatedAverageRewriteTrace(), helperReports, nil, true
}

func (r SQLRuntime) correlatedAverageQuantityRewriteMatch(sql string) (correlatedAverageQuantitySQLMatch, bool) {
	return r.correlatedAverageQuantityTypedMatch(sql)
}

func (r SQLRuntime) correlatedAverageQuantityRewriteDescriptor(sql string) (*PreflightRewriteDescriptorSummary, bool) {
	match, ok := r.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		return nil, false
	}
	summary := match.Descriptor.descriptorSummary()
	return &summary, true
}

func (r SQLRuntime) correlatedAverageQuantityTypedMatch(sql string) (correlatedAverageQuantitySQLMatch, bool) {
	plan := r.Plan(sql)
	for _, intent := range plan.Query.Subqueries {
		if intent.Kind != qsbridge.SubqueryIntentCorrelatedAggregate || intent.CorrelatedAggregate == nil {
			continue
		}
		descriptor, ok := correlatedAverageQuantityDescriptorFromIntent(sql, *intent.CorrelatedAggregate)
		if !ok {
			continue
		}
		brand, container, ok := correlatedAverageQuantityFilterValues(plan.Query, intent.CorrelatedAggregate.RequiredFilterFields)
		if !ok {
			continue
		}
		return correlatedAverageQuantitySQLMatch{
			Descriptor:           descriptor,
			PartBrand:            brand,
			PartContainer:        container,
			RequiredFiltersFound: true,
		}, true
	}
	return correlatedAverageQuantitySQLMatch{}, false
}

func correlatedAverageQuantityDescriptorFromIntent(sql string, intent qsbridge.CorrelatedAggregateSubqueryIntent) (correlatedAverageQuantityDescriptor, bool) {
	start, end, ok := correlatedAveragePredicateRange(sql, intent.SourcePredicate)
	if !ok {
		return correlatedAverageQuantityDescriptor{}, false
	}
	return correlatedAverageQuantityDescriptor{
		Start:             start,
		End:               end,
		OuterLineitem:     intent.OuterValue.Table.RefName(),
		InnerLineitem:     intent.InnerValue.Table.RefName(),
		OuterPart:         intent.OuterKey.Table.RefName(),
		Factor:            intent.Factor,
		AggregateFunction: intent.AggregateFunction,
		OuterQuantity:     correlatedFieldFromRef(intent.OuterValue),
		InnerQuantity:     correlatedFieldFromRef(intent.InnerValue),
		InnerKey:          correlatedFieldFromRef(intent.InnerKey),
		OuterKey:          correlatedFieldFromRef(intent.OuterKey),
		RequiredFilters:   correlatedFieldsFromRefs(intent.RequiredFilterFields),
	}, true
}

func correlatedAveragePredicateRange(sql string, sourcePredicate string) (int, int, bool) {
	sourcePredicate = strings.TrimSpace(sourcePredicate)
	if sourcePredicate == "" {
		return 0, 0, false
	}
	start := strings.Index(sql, sourcePredicate)
	if start < 0 {
		return 0, 0, false
	}
	return start, start + len(sourcePredicate), true
}

func rewriteCorrelatedAveragePredicateSQL(sql string, descriptor correlatedAverageQuantityDescriptor, predicate string) string {
	replacement := strings.TrimSpace(predicate)
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql[descriptor.Start:descriptor.End])), "and ") {
		replacement = " and " + replacement
	}
	return sql[:descriptor.Start] + replacement + sql[descriptor.End:]
}

func correlatedAverageQuantityFilterValues(query qsbridge.QueryIR, filters []qsbridge.FieldRef) (string, string, bool) {
	values := make(map[string]string)
	for _, predicate := range query.Predicates {
		field, value, ok := correlatedAverageEqualityStringFilter(predicate.Expr)
		if !ok {
			continue
		}
		for _, filter := range filters {
			if !sameCorrelatedFilterField(field, filter) {
				continue
			}
			values[strings.ToLower(filter.Name)] = value
		}
	}
	brand := values["p_brand"]
	container := values["p_container"]
	return brand, container, brand != "" && container != ""
}

func correlatedAverageEqualityStringFilter(expr qsbridge.Expr) (qsbridge.FieldRef, string, bool) {
	binary, ok := correlatedAverageBinaryExpr(expr)
	if !ok || binary.Op != qsbridge.BinaryOpEqual {
		return qsbridge.FieldRef{}, "", false
	}
	if field, ok := correlatedAverageFieldExpr(binary.Left); ok {
		if literal, ok := correlatedAverageStringLiteral(binary.Right); ok {
			return field, literal, true
		}
	}
	if field, ok := correlatedAverageFieldExpr(binary.Right); ok {
		if literal, ok := correlatedAverageStringLiteral(binary.Left); ok {
			return field, literal, true
		}
	}
	return qsbridge.FieldRef{}, "", false
}

func correlatedAverageBinaryExpr(expr qsbridge.Expr) (qsbridge.BinaryExpr, bool) {
	switch typed := expr.(type) {
	case qsbridge.BinaryExpr:
		return typed, true
	case *qsbridge.BinaryExpr:
		if typed != nil {
			return *typed, true
		}
	}
	return qsbridge.BinaryExpr{}, false
}

func correlatedAverageFieldExpr(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return typed.Ref, true
	case *qsbridge.FieldExpr:
		if typed != nil {
			return typed.Ref, true
		}
	}
	return qsbridge.FieldRef{}, false
}

func correlatedAverageStringLiteral(expr qsbridge.Expr) (string, bool) {
	switch typed := expr.(type) {
	case qsbridge.LiteralExpr:
		if typed.Kind == qsbridge.ValueString {
			value, ok := typed.Value.(string)
			return value, ok
		}
	case *qsbridge.LiteralExpr:
		if typed != nil && typed.Kind == qsbridge.ValueString {
			value, ok := typed.Value.(string)
			return value, ok
		}
	}
	return "", false
}

func sameCorrelatedFilterField(left qsbridge.FieldRef, right qsbridge.FieldRef) bool {
	return strings.EqualFold(left.Table.RefName(), right.Table.RefName()) && strings.EqualFold(left.Name, right.Name)
}

func correlatedFieldFromRef(ref qsbridge.FieldRef) correlatedSubqueryField {
	return correlatedField(ref.Table.Table, ref.Table.Alias, ref.Name, ref.Type)
}

func correlatedFieldsFromRefs(refs []qsbridge.FieldRef) []correlatedSubqueryField {
	fields := make([]correlatedSubqueryField, 0, len(refs))
	for _, ref := range refs {
		fields = append(fields, correlatedFieldFromRef(ref))
	}
	return fields
}

func correlatedAverageThresholdPredicateExpr(descriptor correlatedAverageQuantityDescriptor, thresholds []q17PartThreshold) qsbridge.Expr {
	if len(thresholds) == 0 {
		return qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Literal(qsbridge.ValueInt, int64(0)))
	}
	var expression qsbridge.Expr
	for _, threshold := range thresholds {
		branch := qsbridge.Binary(
			qsbridge.BinaryOpAnd,
			qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(descriptor.OuterKey.fieldRef()), qsbridge.Literal(qsbridge.ValueInt, threshold.PartKey)),
			qsbridge.Binary(qsbridge.BinaryOpLess, qsbridge.Field(descriptor.OuterQuantity.fieldRef()), qsbridge.Literal(qsbridge.ValueFloat, threshold.Threshold)),
		)
		if expression == nil {
			expression = branch
			continue
		}
		expression = qsbridge.Binary(qsbridge.BinaryOpOr, expression, branch)
	}
	return expression
}

func correlatedAverageThresholdPredicateSQL(expr qsbridge.Expr) (string, bool) {
	branches := correlatedAverageFlattenBinary(expr, qsbridge.BinaryOpOr)
	if len(branches) > 1 {
		rendered := make([]string, 0, len(branches))
		for _, branch := range branches {
			sql, ok := correlatedAverageThresholdPredicateBranchSQL(branch)
			if !ok {
				return "", false
			}
			rendered = append(rendered, sql)
		}
		return "(" + strings.Join(rendered, " or ") + ")", true
	}
	return correlatedAverageThresholdPredicateBranchSQL(expr)
}

func correlatedAverageThresholdPredicateBranchSQL(expr qsbridge.Expr) (string, bool) {
	switch typed := expr.(type) {
	case qsbridge.BinaryExpr:
		if typed.Op == qsbridge.BinaryOpAnd {
			left, leftOK := correlatedAverageThresholdPredicateBranchSQL(typed.Left)
			right, rightOK := correlatedAverageThresholdPredicateBranchSQL(typed.Right)
			if !leftOK || !rightOK {
				return "", false
			}
			return "(" + left + " and " + right + ")", true
		}
		left, leftOK := correlatedAverageThresholdOperandSQL(typed.Left)
		right, rightOK := correlatedAverageThresholdOperandSQL(typed.Right)
		if !leftOK || !rightOK {
			return "", false
		}
		return left + " " + string(typed.Op) + " " + right, true
	default:
		return correlatedAverageThresholdOperandSQL(expr)
	}
}

func correlatedAverageThresholdOperandSQL(expr qsbridge.Expr) (string, bool) {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return correlatedFieldFromRef(typed.Ref).qualifiedName(), true
	case qsbridge.LiteralExpr:
		return correlatedAverageThresholdLiteralSQL(typed)
	default:
		return "", false
	}
}

func correlatedAverageThresholdLiteralSQL(literal qsbridge.LiteralExpr) (string, bool) {
	switch literal.Kind {
	case qsbridge.ValueInt:
		switch value := literal.Value.(type) {
		case int:
			return strconv.Itoa(value), true
		case int64:
			return strconv.FormatInt(value, 10), true
		default:
			return "", false
		}
	case qsbridge.ValueFloat:
		value, ok := resultCellFloat64(qsbridge.ResultCell{Kind: literal.Kind, Value: literal.Value})
		if !ok {
			return "", false
		}
		return strconv.FormatFloat(value, 'g', -1, 64), true
	default:
		return "", false
	}
}

func correlatedAverageFlattenBinary(expr qsbridge.Expr, op qsbridge.BinaryOp) []qsbridge.Expr {
	binary, ok := expr.(qsbridge.BinaryExpr)
	if !ok || binary.Op != op {
		return []qsbridge.Expr{expr}
	}
	left := correlatedAverageFlattenBinary(binary.Left, op)
	right := correlatedAverageFlattenBinary(binary.Right, op)
	return append(left, right...)
}

func (r SQLRuntime) correlatedAveragePartKeys(ctx context.Context, partAlias string, brand string, container string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) ([]int64, qsbridge.DiagnosticSet, []PreflightHelperExecutionRequestReport, error) {
	seeds, diagnostics, reports, err := r.correlatedAveragePartKeySeeds(ctx, partAlias, brand, container, options, values...)
	keys := make([]int64, 0, len(seeds))
	for _, seed := range seeds {
		keys = append(keys, seed.PartKey)
	}
	return keys, diagnostics, reports, err
}

func (r SQLRuntime) correlatedAveragePartKeySeeds(ctx context.Context, partAlias string, brand string, container string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) ([]q17PartKeySeed, qsbridge.DiagnosticSet, []PreflightHelperExecutionRequestReport, error) {
	query := fmt.Sprintf("select %s.p_partkey from part as %s where %s.p_brand = '%s' and %s.p_container = '%s' order by %s.p_partkey", partAlias, partAlias, partAlias, escapeSQLString(brand), partAlias, escapeSQLString(container), partAlias)
	request := correlatedParentKeyHelperRequest(partAlias, brand, container, query, options, values...)
	helper, err := r.executeParentKeyNativeSubqueryStep(ctx, request)
	helperReports := []PreflightHelperExecutionRequestReport{helper.Report()}
	diagnostics := append(qsbridge.DiagnosticSet(nil), helper.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, helperReports, err
	}
	chunk, chunkDiagnostics := helper.Result.Runtime.RowSet.ToResultChunk(0, true)
	diagnostics = append(diagnostics, chunkDiagnostics...)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, helperReports, nil
	}
	if len(helper.Result.Runtime.RowSet.Rownums) != len(chunk.Rows) {
		return nil, helperExecutionDiagnostic(PreflightHelperPlanParentKeyLookup, "part-key lookup returned rownums that do not align with rows"), helperReports, nil
	}
	seeds := make([]q17PartKeySeed, 0, len(chunk.Rows))
	for i, row := range chunk.Rows {
		if len(row) != 1 {
			return nil, helperExecutionDiagnostic(PreflightHelperPlanParentKeyLookup, "part-key lookup returned an unexpected row shape"), helperReports, nil
		}
		key, ok := resultCellInt64(row[0])
		if !ok {
			return nil, helperExecutionDiagnostic(PreflightHelperPlanParentKeyLookup, "part-key lookup returned a non-integer key"), helperReports, nil
		}
		seeds = append(seeds, q17PartKeySeed{PartKey: key, ParentRownum: helper.Result.Runtime.RowSet.Rownums[i]})
	}
	return seeds, nil, helperReports, nil
}

func (r SQLRuntime) correlatedAverageThresholds(ctx context.Context, partKeys []int64, factor float64, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) ([]q17PartThreshold, qsbridge.DiagnosticSet, []PreflightHelperExecutionRequestReport, error) {
	seeds := make([]q17PartKeySeed, 0, len(partKeys))
	for _, key := range partKeys {
		seeds = append(seeds, q17PartKeySeed{PartKey: key, ParentRownum: qsbridge.QuantaRownum(key)})
	}
	return r.correlatedAverageThresholdsForSeeds(ctx, seeds, factor, options, values...)
}

func (r SQLRuntime) correlatedAverageThresholdsForSeeds(ctx context.Context, seeds []q17PartKeySeed, factor float64, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) ([]q17PartThreshold, qsbridge.DiagnosticSet, []PreflightHelperExecutionRequestReport, error) {
	if len(seeds) == 0 {
		return nil, nil, nil, nil
	}
	partKeys := make([]int64, 0, len(seeds))
	parentRownums := make([]qsbridge.QuantaRownum, 0, len(seeds))
	keyText := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		partKeys = append(partKeys, seed.PartKey)
		parentRownums = append(parentRownums, seed.ParentRownum)
		keyText = append(keyText, strconv.FormatInt(seed.PartKey, 10))
	}
	query := fmt.Sprintf("select l_partkey, avg(l_quantity) as avg_quantity from lineitem where l_partkey in (%s) group by l_partkey order by l_partkey", strings.Join(keyText, ", "))
	request := correlatedThresholdHelperRequest(partKeys, parentRownums, factor, query, options, values...)
	helper, err := r.executeAggregateThresholdNativeSubqueryStep(ctx, request)
	helperReports := []PreflightHelperExecutionRequestReport{helper.Report()}
	diagnostics := append(qsbridge.DiagnosticSet(nil), helper.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, helperReports, err
	}
	chunk, chunkDiagnostics := helper.Result.Runtime.RowSet.ToResultChunk(0, true)
	diagnostics = append(diagnostics, chunkDiagnostics...)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, helperReports, nil
	}
	thresholds := make([]q17PartThreshold, 0, len(chunk.Rows))
	for _, row := range chunk.Rows {
		if len(row) != 2 {
			return nil, helperExecutionDiagnostic(PreflightHelperPlanAggregateThresholdLookup, "average lookup returned an unexpected row shape"), helperReports, nil
		}
		key, keyOK := resultCellInt64(row[0])
		avg, avgOK := resultCellFloat64(row[1])
		if !keyOK || !avgOK {
			return nil, helperExecutionDiagnostic(PreflightHelperPlanAggregateThresholdLookup, "average lookup returned non-numeric values"), helperReports, nil
		}
		thresholds = append(thresholds, q17PartThreshold{PartKey: key, Threshold: avg * factor})
	}
	sort.Slice(thresholds, func(i, j int) bool { return thresholds[i].PartKey < thresholds[j].PartKey })
	return thresholds, nil, helperReports, nil
}

func correlatedParentKeyHelperRequest(partAlias string, brand string, container string, query string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) PreflightHelperExecutionRequest {
	return PreflightHelperExecutionRequest{
		Plan: correlatedParentKeyHelperPlan(partAlias, query),
		SQL:  query,
		Payload: PreflightHelperPayload{ParentKeyLookup: &PreflightParentKeyLookupPayload{
			Table:    "part",
			Alias:    partAlias,
			KeyField: "p_partkey",
			Filters: []PreflightHelperEqualityFilter{
				{Field: "p_brand", Value: brand},
				{Field: "p_container", Value: container},
			},
			Output: partAlias + ".p_partkey",
		}},
		Options: options,
		Values:  append([]qsbridge.ParameterValue(nil), values...),
	}
}

func correlatedThresholdHelperRequest(partKeys []int64, parentRownums []qsbridge.QuantaRownum, factor float64, query string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) PreflightHelperExecutionRequest {
	return PreflightHelperExecutionRequest{
		Plan: correlatedThresholdHelperPlan(
			query,
			[]string{"lineitem.l_partkey", "lineitem.l_quantity"},
			[]string{"lineitem.l_partkey", "threshold"},
		),
		SQL: query,
		Payload: PreflightHelperPayload{AggregateThresholdLookup: &PreflightAggregateThresholdLookupPayload{
			Table:             "lineitem",
			KeyField:          "l_partkey",
			AggregateFunction: "avg",
			ValueField:        "l_quantity",
			PartKeys:          append([]int64(nil), partKeys...),
			ParentRownums:     append([]qsbridge.QuantaRownum(nil), parentRownums...),
			Factor:            factor,
			KeyOutput:         "lineitem.l_partkey",
			ValueOutput:       "threshold",
		}},
		Options: options,
		Values:  append([]qsbridge.ParameterValue(nil), values...),
	}
}

func resultCellInt64(cell qsbridge.ResultCell) (int64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		if value <= uint64(^uint64(0)>>1) {
			return int64(value), true
		}
	case float64:
		return int64(value), value == float64(int64(value))
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func resultCellFloat64(cell qsbridge.ResultCell) (float64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		return parsed, err == nil
	}
	return 0, false
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
