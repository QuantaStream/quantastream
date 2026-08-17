package qsbridge

import (
	"fmt"
	"sort"
	"strings"
)

// InMemoryNativeRow is one test/storage-neutral row keyed by logical or physical field name.
type InMemoryNativeRow map[string]ResultCell

// InMemoryNativeTable is a named rowset used by InMemoryNativeExecutor.
type InMemoryNativeTable struct {
	Name string
	Rows []InMemoryNativeRow
}

type inMemoryNativeCandidate struct {
	Rownum QuantaRownum
	Row    InMemoryNativeRow
}

// InMemoryNativeExecutor produces rows for a narrow one-table SELECT slice.
//
// It is intentionally a fixture-grade native executor, not a storage engine.
// The goal is to prove qsbridge result envelopes, dispatch, and adapters can
// carry projected rows before bitmap/BSI-backed execution is available.
type InMemoryNativeExecutor struct {
	Tables map[string]InMemoryNativeTable
}

// NewInMemoryNativeExecutor indexes in-memory tables by case-insensitive table name.
func NewInMemoryNativeExecutor(tables ...InMemoryNativeTable) InMemoryNativeExecutor {
	indexed := make(map[string]InMemoryNativeTable, len(tables))
	for _, table := range tables {
		if table.Name == "" {
			continue
		}
		copied := InMemoryNativeTable{
			Name: table.Name,
			Rows: cloneInMemoryNativeRows(table.Rows),
		}
		indexed[strings.ToLower(table.Name)] = copied
	}
	return InMemoryNativeExecutor{Tables: indexed}
}

// ExecuteNative returns projected rows for supported one-table SELECT requests.
func (e InMemoryNativeExecutor) ExecuteNative(request ExecutionRequest) ExecutionResult {
	if !request.SupportedForExecution() {
		return PlanOnlyNativeExecutor{}.ExecuteNative(request)
	}
	query := request.Bound.Prepared.Query
	if query.Kind != QueryKindSelect {
		return PlanOnlyNativeExecutor{}.ExecuteNative(request)
	}
	if len(query.Sources) == 0 {
		return e.executeProjectionOnlyNative(request, query)
	}
	if diagnostic, blocked := inMemoryUnsupportedSelectDiagnostic(query); blocked {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	table, ok := e.tableForSource(query.Sources[0])
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(inMemoryNativeDiagnostic("table %q is not loaded", query.Sources[0].Table))
	}
	result := request.EmptyResult()
	filteredCandidates := make([]inMemoryNativeCandidate, 0, len(table.Rows))
	for rowIndex, sourceRow := range table.Rows {
		matches, diagnostic, ok := inMemoryRowMatches(query.Predicates, sourceRow, request.Bound.Parameters)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		if !matches {
			continue
		}
		filteredCandidates = append(filteredCandidates, inMemoryNativeCandidate{
			Rownum: inMemoryRownumForRow(rowIndex, sourceRow),
			Row:    sourceRow,
		})
	}
	matchedCandidates := len(filteredCandidates)
	var diagnostic Diagnostic
	if inMemoryGroupedAggregateQuery(query) {
		result, diagnostic, ok = inMemoryAppendGroupedAggregateChunks(result, query, filteredCandidates, request.Options.BatchSize)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		return inMemoryAttachProfileCounters(result, matchedCandidates)
	}
	if inMemoryGlobalAggregateQuery(query) {
		result, diagnostic, ok = inMemoryAppendGlobalAggregateChunk(result, query, filteredCandidates)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		return inMemoryAttachProfileCounters(result, matchedCandidates)
	}
	diagnostic, ok = inMemorySortCandidates(query.OrderBy, filteredCandidates)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	filteredCandidates = inMemoryLimitCandidates(filteredCandidates, query.Result.Offset, query.Result.Limit, query.Result.HasResultLimit())
	filteredCandidates = inMemoryLimitCandidates(filteredCandidates, 0, request.Options.MaxRows, request.Options.MaxRows > 0)
	if inMemoryNeedsEvaluatedProjection(query) {
		rows, diagnostic, ok := inMemoryEvaluateProjectionRows(query.Projection, filteredCandidates)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		result = inMemoryAppendResultRowsChunks(result, rows, request.Options.BatchSize)
		return inMemoryAttachProfileCounters(result, matchedCandidates)
	}
	candidateSet := QuantaCandidateSet{
		Index:   table.Name,
		Rownums: inMemoryCandidateRownums(filteredCandidates),
	}
	materialization := candidateSet.MaterializationRequest(inMemoryMaterializationFields(query))
	projected, diagnostic, ok := inMemoryMaterializeCandidates(materialization, filteredCandidates)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	result = inMemoryAppendProjectedRowSetChunks(result, projected, request.Options.BatchSize)
	return inMemoryAttachProfileCounters(result, matchedCandidates)
}

func (e InMemoryNativeExecutor) executeProjectionOnlyNative(request ExecutionRequest, query QueryIR) ExecutionResult {
	if diagnostic, blocked := inMemoryUnsupportedProjectionOnlySelectDiagnostic(query); blocked {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	matched, diagnostic, ok := inMemoryProjectionOnlyMatches(query, request.Bound.Parameters)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	candidates := []inMemoryNativeCandidate(nil)
	if matched {
		candidates = append(candidates, inMemoryNativeCandidate{
			Rownum: 1,
			Row:    InMemoryNativeRow{},
		})
	}
	rows, diagnostic, ok := inMemoryEvaluateProjectionRows(query.Projection, candidates)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	result := request.EmptyResult()
	result = inMemoryAppendResultRowsChunks(result, rows, request.Options.BatchSize)
	return inMemoryAttachProfileCounters(result, len(candidates))
}

// ExecuteNativeBatch keeps batch execution metadata-only until row batches are modeled.
func (e InMemoryNativeExecutor) ExecuteNativeBatch(request BatchExecutionRequest) BatchExecutionResult {
	return PlanOnlyNativeExecutor{}.ExecuteNativeBatch(request)
}

func inMemoryUnsupportedProjectionOnlySelectDiagnostic(query QueryIR) (Diagnostic, bool) {
	if len(query.Joins) > 0 || len(query.Memberships) > 0 || len(query.Subqueries) > 0 {
		return inMemoryNativeDiagnostic("projection-only SELECT cannot use joins, memberships, or subqueries"), true
	}
	if len(query.GroupBy) > 0 || len(query.Aggregates) > 0 || len(query.Having) > 0 {
		return inMemoryNativeDiagnostic("projection-only SELECT cannot use grouped or aggregate clauses"), true
	}
	if len(query.OrderBy) > 0 {
		return inMemoryNativeDiagnostic("projection-only SELECT ORDER BY is not supported"), true
	}
	for _, projection := range query.Projection {
		if !inMemorySupportedProjectionOnlyExpr(projection.Expr) {
			return inMemoryNativeDiagnostic("projection-only SELECT supports only scalar literal and arithmetic projections"), true
		}
	}
	for _, predicate := range query.Predicates {
		if !inMemorySupportedProjectionOnlyBooleanExpr(predicate.Expr) {
			return inMemoryNativeDiagnostic("projection-only SELECT WHERE supports only scalar boolean predicates"), true
		}
	}
	if query.WhereExpr != nil && !inMemorySupportedProjectionOnlyBooleanExpr(query.WhereExpr) {
		return inMemoryNativeDiagnostic("projection-only SELECT WHERE supports only scalar boolean predicates"), true
	}
	return Diagnostic{}, false
}

func inMemoryUnsupportedSelectDiagnostic(query QueryIR) (Diagnostic, bool) {
	if len(query.Sources) != 1 {
		return inMemoryNativeDiagnostic("expected exactly one source, got %d", len(query.Sources)), true
	}
	if len(query.Joins) > 0 {
		return inMemoryNativeDiagnostic("joins are not supported"), true
	}
	if len(query.Memberships) > 0 {
		return inMemoryNativeDiagnostic("membership predicates are not supported"), true
	}
	for _, predicate := range query.Predicates {
		_, _, inOK := inMemoryInPredicateParts(predicate)
		_, _, _, _, betweenOK := inMemoryBetweenPredicateParts(predicate)
		_, _, _, comparisonOK := inMemoryComparisonPredicateParts(predicate)
		if !inOK && !betweenOK && !comparisonOK {
			return inMemoryNativeDiagnostic("only comparison and IN predicates comparing one field to literals are supported"), true
		}
	}
	if len(query.GroupBy) > 0 && !inMemoryGroupedAggregateQuery(query) {
		return inMemoryNativeDiagnostic("only single-field GROUP BY with count(*), sum(field), avg(field), min(field), and max(field) is supported"), true
	}
	if len(query.Aggregates) > 0 && len(query.GroupBy) == 0 && !inMemoryGlobalAggregateQuery(query) {
		return inMemoryNativeDiagnostic("only global count(*), sum(field), avg(field), min(field), and max(field) aggregate projections are supported"), true
	}
	if len(query.OrderBy) > 1 {
		return inMemoryNativeDiagnostic("only one ORDER BY field is supported"), true
	}
	if len(query.GroupBy) > 0 {
		if diagnostic, ok := inMemoryGroupedSortSupported(query); !ok {
			return diagnostic, true
		}
	} else {
		for _, sort := range query.OrderBy {
			if _, ok := inMemoryProjectionField(sort.Expr); !ok {
				return inMemoryNativeDiagnostic("only direct field ORDER BY is supported"), true
			}
		}
	}
	for _, projection := range query.Projection {
		if !inMemorySupportedProjectionExpr(projection.Expr) {
			return inMemoryNativeDiagnostic("only direct field and aggregate reference projections are supported"), true
		}
	}
	return Diagnostic{}, false
}

func (e InMemoryNativeExecutor) tableForSource(source TableInstance) (InMemoryNativeTable, bool) {
	if len(e.Tables) == 0 {
		return InMemoryNativeTable{}, false
	}
	for _, name := range []string{source.Table, source.RefName(), string(source.ID), source.DisplayName()} {
		if table, ok := e.Tables[strings.ToLower(name)]; ok {
			return cloneInMemoryNativeTable(table), true
		}
	}
	return InMemoryNativeTable{}, false
}

func inMemoryRownumForRow(rowIndex int, row InMemoryNativeRow) QuantaRownum {
	if cell, ok := row["rownum"]; ok {
		if number, ok := inMemoryNumericValue(cell.Value); ok {
			return QuantaRownum(number)
		}
	}
	return QuantaRownum(rowIndex + 1)
}

func inMemoryCandidateRownums(candidates []inMemoryNativeCandidate) []QuantaRownum {
	rownums := make([]QuantaRownum, len(candidates))
	for i, candidate := range candidates {
		rownums[i] = candidate.Rownum
	}
	return rownums
}

func inMemoryMaterializationFields(query QueryIR) []QuantaProjectionField {
	seen := make(map[string]struct{})
	fields := make([]QuantaProjectionField, 0, len(query.RequiredFields()))
	for _, projection := range query.Projection {
		for _, field := range projection.RequiredFields() {
			key := quantaIntermediateFieldKey(field)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			fields = append(fields, inMemoryProjectionRequestField(field, true))
		}
	}
	for _, field := range query.RequiredFields() {
		key := quantaIntermediateFieldKey(field)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		fields = append(fields, inMemoryProjectionRequestField(field, false))
	}
	return fields
}

func inMemoryProjectionRequestField(field FieldRef, visible bool) QuantaProjectionField {
	roles := field.Roles
	if visible {
		roles |= FieldRoleVisible
	}
	return QuantaProjectionField{
		Index:        field.Table.Table,
		Field:        quantaIntermediateFieldName(field),
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        roles,
		Visible:      visible,
	}
}

func inMemoryMaterializeCandidates(request QuantaMaterializationRequest, candidates []inMemoryNativeCandidate) (QuantaProjectedRowSet, Diagnostic, bool) {
	vectors := make([]QuantaProjectionVector, 0, len(request.ProjectionFields))
	for _, field := range request.ProjectionFields {
		values := make([]ResultCell, 0, len(candidates))
		for _, candidate := range candidates {
			cell, ok := inMemoryFieldCell(candidate.Row, FieldRef{
				Name:         field.Field,
				PhysicalName: field.PhysicalName,
				Type:         field.Type,
			})
			if !ok {
				return QuantaProjectedRowSet{}, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", field.Field), false
			}
			values = append(values, cell)
		}
		vectors = append(vectors, QuantaProjectionVector{Field: field, Values: values})
	}
	return QuantaProjectedRowSet{
		Index:             request.Index,
		LogicalShard:      request.LogicalShard,
		Replica:           request.Replica,
		Rownums:           append([]QuantaRownum(nil), request.Rownums...),
		ProjectionVectors: vectors,
	}, Diagnostic{}, true
}

func inMemoryAppendProjectedRowSetChunks(result ExecutionResult, rowSet QuantaProjectedRowSet, batchSize int) ExecutionResult {
	if batchSize <= 0 || batchSize >= len(rowSet.Rownums) {
		return result.WithProjectedRowSet(rowSet, 0, true)
	}
	sequence := 0
	for start := 0; start < len(rowSet.Rownums); start += batchSize {
		end := start + batchSize
		if end > len(rowSet.Rownums) {
			end = len(rowSet.Rownums)
		}
		result = result.WithProjectedRowSet(inMemoryProjectedRowSetSlice(rowSet, start, end), sequence, end == len(rowSet.Rownums))
		if result.Diagnostics.BlocksNative() {
			return result
		}
		sequence++
	}
	return result
}

func inMemoryProjectedRowSetSlice(rowSet QuantaProjectedRowSet, start int, end int) QuantaProjectedRowSet {
	sliced := QuantaProjectedRowSet{
		Index:        rowSet.Index,
		LogicalShard: rowSet.LogicalShard,
		Replica:      rowSet.Replica,
		Rownums:      append([]QuantaRownum(nil), rowSet.Rownums[start:end]...),
	}
	sliced.ProjectionVectors = make([]QuantaProjectionVector, 0, len(rowSet.ProjectionVectors))
	for _, vector := range rowSet.ProjectionVectors {
		sliced.ProjectionVectors = append(sliced.ProjectionVectors, QuantaProjectionVector{
			Field:  vector.Field,
			Values: append([]ResultCell(nil), vector.Values[start:end]...),
		})
	}
	return sliced
}

func inMemoryAttachProfileCounters(result ExecutionResult, matchedCandidates int) ExecutionResult {
	if !result.Profile.IncludeProfile {
		return result
	}
	profile := result.Profile
	profile.Counters = append(profile.Counters,
		ExecutionCounter{Name: "matched_candidates", Value: uint64(matchedCandidates), Unit: "rows"},
		ExecutionCounter{Name: "delivered_rows", Value: result.RowsReturned, Unit: "rows"},
		ExecutionCounter{Name: "result_chunks", Value: uint64(len(result.Chunks)), Unit: "chunks"},
	)
	return result.WithProfile(profile)
}

func inMemorySortCandidates(sortSpecs []SortSpec, candidates []inMemoryNativeCandidate) (Diagnostic, bool) {
	if len(sortSpecs) == 0 || len(candidates) < 2 {
		return Diagnostic{}, true
	}
	if len(sortSpecs) > 1 {
		return inMemoryNativeDiagnostic("only one ORDER BY field is supported"), false
	}
	field, ok := inMemoryProjectionField(sortSpecs[0].Expr)
	if !ok {
		return inMemoryNativeDiagnostic("only direct field ORDER BY is supported"), false
	}
	descending := sortSpecs[0].Direction == SortDescending
	sort.SliceStable(candidates, func(i, j int) bool {
		left, leftOK := inMemoryFieldCell(candidates[i].Row, field)
		right, rightOK := inMemoryFieldCell(candidates[j].Row, field)
		if !leftOK || !rightOK {
			return false
		}
		less := inMemoryCellLess(left, right)
		if descending {
			return !less && !inMemoryCellEqual(left, right)
		}
		return less
	})
	return Diagnostic{}, true
}

func inMemoryLimitCandidates(candidates []inMemoryNativeCandidate, offset int, limit int, hasLimit bool) []inMemoryNativeCandidate {
	if !hasLimit && offset <= 0 {
		return candidates
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(candidates) {
		return nil
	}
	candidates = candidates[offset:]
	if hasLimit && limit <= 0 {
		return nil
	}
	if hasLimit && limit < len(candidates) {
		return candidates[:limit]
	}
	return candidates
}

func inMemoryRowMatches(predicates []Predicate, sourceRow InMemoryNativeRow, parameters ParameterBindingSet) (bool, Diagnostic, bool) {
	for _, predicate := range predicates {
		if handled, matched, diagnostic, ok := inMemoryInPredicateMatches(predicate, sourceRow, parameters); handled {
			if !ok {
				return false, diagnostic, false
			}
			if !matched {
				return false, Diagnostic{}, true
			}
			continue
		}
		if handled, matched, diagnostic, ok := inMemoryBetweenPredicateMatches(predicate, sourceRow, parameters); handled {
			if !ok {
				return false, diagnostic, false
			}
			if !matched {
				return false, Diagnostic{}, true
			}
			continue
		}
		op, field, valueExpr, ok := inMemoryComparisonPredicateParts(predicate)
		if !ok {
			return false, inMemoryNativeDiagnostic("only comparison predicates comparing one field to one literal or parameter are supported"), false
		}
		cell, ok := inMemoryFieldCell(sourceRow, field)
		if !ok {
			return false, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", field.Name), false
		}
		literal, diagnostic, ok := inMemoryComparisonValue(valueExpr, parameters)
		if !ok {
			return false, diagnostic, false
		}
		if !inMemoryCellComparesLiteral(op, cell, literal) {
			return false, Diagnostic{}, true
		}
	}
	return true, Diagnostic{}, true
}

func inMemoryInPredicateMatches(predicate Predicate, sourceRow InMemoryNativeRow, parameters ParameterBindingSet) (bool, bool, Diagnostic, bool) {
	field, list, ok := inMemoryInPredicateParts(predicate)
	if !ok {
		return false, false, Diagnostic{}, true
	}
	cell, ok := inMemoryFieldCell(sourceRow, field)
	if !ok {
		return true, false, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", field.Name), false
	}
	for _, item := range list.Items {
		literal, diagnostic, ok := inMemoryComparisonValue(item, parameters)
		if !ok {
			return true, false, diagnostic, false
		}
		if inMemoryCellComparesLiteral(BinaryOpEqual, cell, literal) {
			return true, !inMemoryInPredicateNegated(predicate), Diagnostic{}, true
		}
	}
	return true, inMemoryInPredicateNegated(predicate), Diagnostic{}, true
}

func inMemoryBetweenPredicateMatches(predicate Predicate, sourceRow InMemoryNativeRow, parameters ParameterBindingSet) (bool, bool, Diagnostic, bool) {
	field, lowerExpr, upperExpr, negate, ok := inMemoryBetweenPredicateParts(predicate)
	if !ok {
		return false, false, Diagnostic{}, true
	}
	cell, ok := inMemoryFieldCell(sourceRow, field)
	if !ok {
		return true, false, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", field.Name), false
	}
	lower, diagnostic, ok := inMemoryComparisonValue(lowerExpr, parameters)
	if !ok {
		return true, false, diagnostic, false
	}
	upper, diagnostic, ok := inMemoryComparisonValue(upperExpr, parameters)
	if !ok {
		return true, false, diagnostic, false
	}
	matched := inMemoryCellComparesLiteral(BinaryOpGreaterEqual, cell, lower) &&
		inMemoryCellComparesLiteral(BinaryOpLessEqual, cell, upper)
	if negate {
		return true, !matched, Diagnostic{}, true
	}
	return true, matched, Diagnostic{}, true
}

func inMemoryBetweenPredicateParts(predicate Predicate) (FieldRef, Expr, Expr, bool, bool) {
	binary, ok := inMemoryBinaryExpr(predicate.Expr)
	if !ok || (binary.Op != BinaryOpBetween && binary.Op != BinaryOpNotBetween) {
		return FieldRef{}, nil, nil, false, false
	}
	field, ok := inMemoryProjectionField(binary.Left)
	if !ok {
		return FieldRef{}, nil, nil, false, false
	}
	list, ok := inMemoryListExpr(binary.Right)
	if !ok || len(list.Items) != 2 {
		return FieldRef{}, nil, nil, false, false
	}
	if !inMemoryComparisonValueExpr(list.Items[0]) || !inMemoryComparisonValueExpr(list.Items[1]) {
		return FieldRef{}, nil, nil, false, false
	}
	return field, list.Items[0], list.Items[1], binary.Op == BinaryOpNotBetween, true
}

func inMemoryInPredicateParts(predicate Predicate) (FieldRef, ListExpr, bool) {
	binary, ok := inMemoryBinaryExpr(predicate.Expr)
	if !ok || (binary.Op != BinaryOpIn && binary.Op != BinaryOpNotIn) {
		return FieldRef{}, ListExpr{}, false
	}
	field, ok := inMemoryProjectionField(binary.Left)
	if !ok {
		return FieldRef{}, ListExpr{}, false
	}
	list, ok := inMemoryListExpr(binary.Right)
	return field, list, ok
}

func inMemoryInPredicateNegated(predicate Predicate) bool {
	binary, ok := inMemoryBinaryExpr(predicate.Expr)
	return ok && binary.Op == BinaryOpNotIn
}

func inMemoryComparisonPredicateParts(predicate Predicate) (BinaryOp, FieldRef, Expr, bool) {
	binary, ok := inMemoryBinaryExpr(predicate.Expr)
	if !ok || !inMemorySupportedComparisonOp(binary.Op) {
		return "", FieldRef{}, nil, false
	}
	if field, ok := inMemoryProjectionField(binary.Left); ok {
		if inMemoryComparisonValueExpr(binary.Right) {
			return binary.Op, field, binary.Right, true
		}
	}
	if field, ok := inMemoryProjectionField(binary.Right); ok {
		if inMemoryComparisonValueExpr(binary.Left) {
			op, ok := inMemoryReverseComparisonOp(binary.Op)
			return op, field, binary.Left, ok
		}
	}
	return "", FieldRef{}, nil, false
}

func inMemorySupportedComparisonOp(op BinaryOp) bool {
	switch op {
	case BinaryOpEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func inMemoryReverseComparisonOp(op BinaryOp) (BinaryOp, bool) {
	switch op {
	case BinaryOpEqual:
		return BinaryOpEqual, true
	case BinaryOpLess:
		return BinaryOpGreater, true
	case BinaryOpLessEqual:
		return BinaryOpGreaterEqual, true
	case BinaryOpGreater:
		return BinaryOpLess, true
	case BinaryOpGreaterEqual:
		return BinaryOpLessEqual, true
	default:
		return "", false
	}
}

func inMemoryBinaryExpr(expr Expr) (BinaryExpr, bool) {
	switch typed := expr.(type) {
	case BinaryExpr:
		return typed, true
	case *BinaryExpr:
		if typed == nil {
			return BinaryExpr{}, false
		}
		return *typed, true
	default:
		return BinaryExpr{}, false
	}
}

func inMemoryProjectionField(expr Expr) (FieldRef, bool) {
	switch typed := expr.(type) {
	case FieldExpr:
		return typed.Ref, true
	case *FieldExpr:
		if typed == nil {
			return FieldRef{}, false
		}
		return typed.Ref, true
	default:
		return FieldRef{}, false
	}
}

func inMemoryAggregateRef(expr Expr) (AggregateRefExpr, bool) {
	switch typed := expr.(type) {
	case AggregateRefExpr:
		return typed, true
	case *AggregateRefExpr:
		if typed == nil {
			return AggregateRefExpr{}, false
		}
		return *typed, true
	default:
		return AggregateRefExpr{}, false
	}
}

func inMemoryListExpr(expr Expr) (ListExpr, bool) {
	switch typed := expr.(type) {
	case ListExpr:
		return typed, true
	case *ListExpr:
		if typed == nil {
			return ListExpr{}, false
		}
		return *typed, true
	default:
		return ListExpr{}, false
	}
}

func inMemoryLiteralExpr(expr Expr) (LiteralExpr, bool) {
	switch typed := expr.(type) {
	case LiteralExpr:
		return typed, true
	case *LiteralExpr:
		if typed == nil {
			return LiteralExpr{}, false
		}
		return *typed, true
	default:
		return LiteralExpr{}, false
	}
}

func inMemoryParameterExpr(expr Expr) (ParameterExpr, bool) {
	switch typed := expr.(type) {
	case ParameterExpr:
		return typed, true
	case *ParameterExpr:
		if typed == nil {
			return ParameterExpr{}, false
		}
		return *typed, true
	default:
		return ParameterExpr{}, false
	}
}

func inMemoryComparisonValueExpr(expr Expr) bool {
	if _, ok := inMemoryLiteralExpr(expr); ok {
		return true
	}
	_, ok := inMemoryParameterExpr(expr)
	return ok
}

func inMemoryComparisonValue(expr Expr, parameters ParameterBindingSet) (LiteralExpr, Diagnostic, bool) {
	if literal, ok := inMemoryLiteralExpr(expr); ok {
		return literal, Diagnostic{}, true
	}
	parameter, ok := inMemoryParameterExpr(expr)
	if !ok {
		return LiteralExpr{}, inMemoryNativeDiagnostic("comparison value must be a literal or parameter"), false
	}
	for _, binding := range parameters.Bindings {
		if parameterRefKey(binding.Ref) != parameterRefKey(parameter.Ref) {
			continue
		}
		return Literal(binding.Value.Kind, binding.Value.Value), Diagnostic{}, true
	}
	return LiteralExpr{}, inMemoryNativeDiagnostic("prepared-statement parameter %d is not bound", parameter.Ref.Index), false
}

func inMemoryFieldCell(row InMemoryNativeRow, field FieldRef) (ResultCell, bool) {
	for _, name := range []string{field.Name, field.PhysicalName, field.QualifiedName()} {
		if name == "" {
			continue
		}
		if cell, ok := row[name]; ok {
			return inMemoryTypedCell(cell, field.Type), true
		}
	}
	return ResultCell{}, false
}

func inMemoryTypedCell(cell ResultCell, dataType DataType) ResultCell {
	if cell.Kind != "" {
		return cell
	}
	cell.Kind = inMemoryValueKind(dataType)
	return cell
}

func inMemoryCellComparesLiteral(op BinaryOp, cell ResultCell, literal LiteralExpr) bool {
	if cell.Value == nil || literal.Value == nil {
		return op == BinaryOpEqual && cell.Value == nil && literal.Value == nil
	}
	if inMemoryIsNumericKind(cell.Kind) || inMemoryIsNumericKind(literal.Kind) {
		left, leftOK := inMemoryNumericValue(cell.Value)
		right, rightOK := inMemoryNumericValue(literal.Value)
		return leftOK && rightOK && inMemoryCompareFloat(op, left, right)
	}
	return inMemoryCompareString(op, fmt.Sprint(cell.Value), fmt.Sprint(literal.Value))
}

func inMemoryCompareFloat(op BinaryOp, left float64, right float64) bool {
	switch op {
	case BinaryOpEqual:
		return left == right
	case BinaryOpLess:
		return left < right
	case BinaryOpLessEqual:
		return left <= right
	case BinaryOpGreater:
		return left > right
	case BinaryOpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func inMemoryCompareString(op BinaryOp, left string, right string) bool {
	switch op {
	case BinaryOpEqual:
		return left == right
	case BinaryOpLess:
		return left < right
	case BinaryOpLessEqual:
		return left <= right
	case BinaryOpGreater:
		return left > right
	case BinaryOpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func inMemoryCellLess(left ResultCell, right ResultCell) bool {
	if inMemoryIsNumericKind(left.Kind) || inMemoryIsNumericKind(right.Kind) {
		leftNumber, leftOK := inMemoryNumericValue(left.Value)
		rightNumber, rightOK := inMemoryNumericValue(right.Value)
		if leftOK && rightOK {
			return leftNumber < rightNumber
		}
	}
	return fmt.Sprint(left.Value) < fmt.Sprint(right.Value)
}

func inMemoryCellEqual(left ResultCell, right ResultCell) bool {
	if inMemoryIsNumericKind(left.Kind) || inMemoryIsNumericKind(right.Kind) {
		leftNumber, leftOK := inMemoryNumericValue(left.Value)
		rightNumber, rightOK := inMemoryNumericValue(right.Value)
		if leftOK && rightOK {
			return leftNumber == rightNumber
		}
	}
	return fmt.Sprint(left.Value) == fmt.Sprint(right.Value)
}

func inMemoryIsNumericKind(kind ValueKind) bool {
	return kind == ValueInt || kind == ValueFloat
}

func inMemoryNumericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func inMemoryValueKind(dataType DataType) ValueKind {
	switch dataType {
	case DataTypeBool:
		return ValueBool
	case DataTypeInt:
		return ValueInt
	case DataTypeFloat:
		return ValueFloat
	case DataTypeString:
		return ValueString
	case DataTypeTime:
		return ValueTime
	default:
		return ValueUnknown
	}
}

func inMemoryNativeDiagnostic(format string, args ...any) Diagnostic {
	return ErrorDiagnostic(DiagnosticNativeBlocker, PhaseExecute, "in-memory native executor supports only one-table direct SELECT projections with optional comparison predicates, ORDER BY, and LIMIT: "+fmt.Sprintf(format, args...))
}

func cloneInMemoryNativeTable(table InMemoryNativeTable) InMemoryNativeTable {
	return InMemoryNativeTable{
		Name: table.Name,
		Rows: cloneInMemoryNativeRows(table.Rows),
	}
}

func cloneInMemoryNativeRows(rows []InMemoryNativeRow) []InMemoryNativeRow {
	if len(rows) == 0 {
		return nil
	}
	copied := make([]InMemoryNativeRow, len(rows))
	for i, row := range rows {
		copied[i] = cloneInMemoryNativeRow(row)
	}
	return copied
}

func cloneInMemoryNativeRow(row InMemoryNativeRow) InMemoryNativeRow {
	if len(row) == 0 {
		return nil
	}
	copied := make(InMemoryNativeRow, len(row))
	for name, cell := range row {
		copied[name] = cell
	}
	return copied
}
