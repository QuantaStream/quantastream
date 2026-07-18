package qsruntime

import (
	"context"
	"fmt"
	"regexp"
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

type correlatedAverageQuantitySQLRecognizer struct{}

func (correlatedAverageQuantitySQLRecognizer) recognize(sql string) (correlatedAverageQuantitySQLMatch, bool) {
	descriptor, ok := matchCorrelatedAverageQuantityPredicateSQL(sql)
	if !ok {
		return correlatedAverageQuantitySQLMatch{}, false
	}
	brand, container, filtersFound := matchCorrelatedPartFiltersSQL(sql, descriptor.OuterPart)
	return correlatedAverageQuantitySQLMatch{
		Descriptor:           descriptor,
		PartBrand:            brand,
		PartContainer:        container,
		RequiredFiltersFound: filtersFound,
	}, true
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
	match, ok := (correlatedAverageQuantitySQLRecognizer{}).recognize(sql)
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
	if len(thresholds) == 0 {
		trace := correlatedAverageRewriteTrace()
		return sql[:descriptor.Start] + " and 1 = 0" + sql[descriptor.End:], nil, trace, helperReports, nil, true
	}
	branches := make([]string, 0, len(thresholds))
	for _, threshold := range thresholds {
		branches = append(branches, fmt.Sprintf("(%s = %d and %s < %s)", descriptor.OuterKey.qualifiedName(), threshold.PartKey, descriptor.OuterQuantity.qualifiedName(), strconv.FormatFloat(threshold.Threshold, 'g', -1, 64)))
	}
	rewritten := sql[:descriptor.Start] + " and (" + strings.Join(branches, " or ") + ")" + sql[descriptor.End:]
	return rewritten, nil, correlatedAverageRewriteTrace(), helperReports, nil, true
}

func findCorrelatedAverageQuantityPredicate(sql string) (correlatedAverageQuantityDescriptor, bool) {
	match, ok := (correlatedAverageQuantitySQLRecognizer{}).recognize(sql)
	if !ok {
		return correlatedAverageQuantityDescriptor{}, false
	}
	return match.Descriptor, true
}

func matchCorrelatedAverageQuantityPredicateSQL(sql string) (correlatedAverageQuantityDescriptor, bool) {
	re := regexp.MustCompile(`(?is)\s+and\s+([a-z_][a-z0-9_]*)\.l_quantity\s*<\s*\(\s*select\s+([0-9]+(?:\.[0-9]+)?)\s*\*\s*avg\(\s*([a-z_][a-z0-9_]*)\.l_quantity\s*\)\s+from\s+lineitem\s+as\s+([a-z_][a-z0-9_]*)\s+where\s+([a-z_][a-z0-9_]*)\.l_partkey\s*=\s*([a-z_][a-z0-9_]*)\.p_partkey\s*\)`)
	match := re.FindStringSubmatchIndex(sql)
	if match == nil {
		return correlatedAverageQuantityDescriptor{}, false
	}
	groups := regexpSubmatches(sql, match)
	if len(groups) != 7 || !strings.EqualFold(groups[3], groups[4]) || !strings.EqualFold(groups[3], groups[5]) {
		return correlatedAverageQuantityDescriptor{}, false
	}
	factor, err := strconv.ParseFloat(groups[2], 64)
	if err != nil {
		return correlatedAverageQuantityDescriptor{}, false
	}
	return correlatedAverageQuantityDescriptor{
		Start:             match[0],
		End:               match[1],
		OuterLineitem:     groups[1],
		Factor:            factor,
		AggregateFunction: "avg",
		OuterQuantity:     correlatedField("lineitem", groups[1], "l_quantity", qsbridge.DataTypeInt),
		InnerQuantity:     correlatedField("lineitem", groups[3], "l_quantity", qsbridge.DataTypeInt),
		InnerLineitem:     groups[3],
		InnerKey:          correlatedField("lineitem", groups[3], "l_partkey", qsbridge.DataTypeInt),
		OuterPart:         groups[6],
		OuterKey:          correlatedField("part", groups[6], "p_partkey", qsbridge.DataTypeInt),
		RequiredFilters: []correlatedSubqueryField{
			correlatedField("part", groups[6], "p_brand", qsbridge.DataTypeString),
			correlatedField("part", groups[6], "p_container", qsbridge.DataTypeString),
		},
	}, true
}

func correlatedPartFilters(sql string, partAlias string) (string, string, bool) {
	return matchCorrelatedPartFiltersSQL(sql, partAlias)
}

func matchCorrelatedPartFiltersSQL(sql string, partAlias string) (string, string, bool) {
	alias := regexp.QuoteMeta(partAlias)
	brand := firstRegexpGroup(sql, `(?is)\b`+alias+`\.p_brand\s*=\s*'([^']*)'`)
	container := firstRegexpGroup(sql, `(?is)\b`+alias+`\.p_container\s*=\s*'([^']*)'`)
	return brand, container, brand != "" && container != ""
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

func regexpSubmatches(text string, indexes []int) []string {
	groups := make([]string, 0, len(indexes)/2)
	for i := 0; i < len(indexes); i += 2 {
		if indexes[i] < 0 || indexes[i+1] < 0 {
			groups = append(groups, "")
			continue
		}
		groups = append(groups, text[indexes[i]:indexes[i+1]])
	}
	return groups
}

func firstRegexpGroup(text string, pattern string) string {
	match := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return match[1]
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
