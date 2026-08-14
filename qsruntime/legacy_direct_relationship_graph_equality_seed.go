package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type legacyDirectRelationshipGraphEqualityRoleSeed struct {
	sourceRole  string
	sourceTable string
	sourceField qsbridge.FieldRef
	targetRole  string
	targetTable string
	targetField qsbridge.FieldRef
}

type legacyDirectRelationshipGraphEqualityRoleSeedMode string

const (
	legacyDirectRelationshipGraphEqualityRoleSeedModeAuto legacyDirectRelationshipGraphEqualityRoleSeedMode = "auto"
	legacyDirectRelationshipGraphEqualityRoleSeedModeOff  legacyDirectRelationshipGraphEqualityRoleSeedMode = "off"
	legacyDirectRelationshipGraphEqualityRoleSeedModeOn   legacyDirectRelationshipGraphEqualityRoleSeedMode = "on"

	legacyDirectRelationshipGraphEqualityRoleSeedAutoMaxSourceRows = directBitmapMembershipMaxDynamicBatchEQValues * 32
)

type legacyDirectRelationshipGraphEqualityRoleSeedOptions struct {
	maxSourceRows int
}

func legacyDirectRelationshipGraphEqualityRoleSeedModeFromEnv() legacyDirectRelationshipGraphEqualityRoleSeedMode {
	raw := strings.TrimSpace(os.Getenv("QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED"))
	if raw == "" {
		return legacyDirectRelationshipGraphEqualityRoleSeedModeAuto
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return legacyDirectRelationshipGraphEqualityRoleSeedModeOn
	case "auto":
		return legacyDirectRelationshipGraphEqualityRoleSeedModeAuto
	default:
		return legacyDirectRelationshipGraphEqualityRoleSeedModeOff
	}
}

func legacyDirectRelationshipGraphEqualityRoleSeedEnabled() bool {
	return legacyDirectRelationshipGraphEqualityRoleSeedModeFromEnv() == legacyDirectRelationshipGraphEqualityRoleSeedModeOn
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyGraphEqualityRoleSeeds(ctx context.Context, request ExecutionRequest, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool, iteration int) ([]ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipApplyGraphEqualityRoleSeedsWithPrefix(ctx, request, rowsByRole, fullDomainRowsByRole, fmt.Sprintf("graph_iter_%d_", iteration))
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyGraphEqualityRoleSeedsWithPrefix(ctx context.Context, request ExecutionRequest, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool, probePrefix string) ([]ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithPrefix(ctx, request, legacyDirectRelationshipGraphEqualityFields(request), rowsByRole, fullDomainRowsByRole, probePrefix)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithPrefix(ctx context.Context, request ExecutionRequest, equalities []legacyDirectRelationshipGraphEqualityFieldPair, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool, probePrefix string) ([]ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithOptions(ctx, request, equalities, rowsByRole, fullDomainRowsByRole, probePrefix, legacyDirectRelationshipGraphEqualityRoleSeedOptions{})
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithOptions(ctx context.Context, request ExecutionRequest, equalities []legacyDirectRelationshipGraphEqualityFieldPair, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool, probePrefix string, options legacyDirectRelationshipGraphEqualityRoleSeedOptions) ([]ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	if len(equalities) == 0 {
		return nil, false, nil, nil
	}
	probes := []ExecutionProbe{}
	changed := false
	totalCandidates := 0
	appliedCandidates := 0
	seedOrdinal := 0
	maxRounds := len(rowsByRole) + 1
	for round := 1; round <= maxRounds; round++ {
		candidates := legacyDirectRelationshipGraphEqualityRoleSeedCandidatesFromFieldsWithOptions(equalities, rowsByRole, fullDomainRowsByRole, options)
		totalCandidates += len(candidates)
		probes = append(probes, legacyDirectRelationshipProbe(fmt.Sprintf("%sequality_role_seed_round_%d_candidates", probePrefix, round), strconv.Itoa(len(candidates))))
		if len(candidates) == 0 {
			break
		}
		roundChanged := false
		for _, candidate := range candidates {
			seedOrdinal++
			prefix := fmt.Sprintf("%sequality_role_seed_%d_", probePrefix, seedOrdinal)
			sourceRows := rowsByRole[candidate.sourceRole]
			targetRows := rowsByRole[candidate.targetRole]
			probes = append(probes,
				legacyDirectRelationshipProbe(prefix+"source_role", candidate.sourceRole),
				legacyDirectRelationshipProbe(prefix+"source_field", directBitmapFieldPhysicalName(candidate.sourceField)),
				legacyDirectRelationshipProbe(prefix+"source_rows", strconv.Itoa(len(sourceRows))),
				legacyDirectRelationshipProbe(prefix+"target_role", candidate.targetRole),
				legacyDirectRelationshipProbe(prefix+"target_field", directBitmapFieldPhysicalName(candidate.targetField)),
				legacyDirectRelationshipProbe(prefix+"target_rows_before", strconv.Itoa(len(targetRows))),
			)
			if len(sourceRows) == 0 {
				rowsByRole[candidate.targetRole] = nil
				fullDomainRowsByRole[candidate.targetRole] = false
				candidateChanged := len(targetRows) != 0
				changed = changed || candidateChanged
				roundChanged = roundChanged || candidateChanged
				appliedCandidates++
				probes = append(probes,
					legacyDirectRelationshipProbe(prefix+"applied", "true"),
					legacyDirectRelationshipProbe(prefix+"reason", "empty_source_rows"),
					legacyDirectRelationshipProbe(prefix+"values", "0"),
					legacyDirectRelationshipProbe(prefix+"candidate_rows", "0"),
					legacyDirectRelationshipProbe(prefix+"target_rows_after", "0"),
					legacyDirectRelationshipProbe(prefix+"materialization_elapsed", "0s"),
					legacyDirectRelationshipProbe(prefix+"query_elapsed", "0s"),
					legacyDirectRelationshipProbe(prefix+"intersect_elapsed", "0s"),
					legacyDirectRelationshipProbe(prefix+"elapsed", "0s"),
				)
				continue
			}
			start := time.Now()
			values, materializationElapsed, diagnostics, err := e.legacyDirectRelationshipGraphEqualitySeedValues(ctx, request, candidate.sourceTable, candidate.sourceRole, candidate.sourceField, sourceRows)
			if err != nil || diagnostics.BlocksNative() {
				return probes, changed, diagnostics, err
			}
			probes = append(probes,
				legacyDirectRelationshipProbe(prefix+"values", strconv.Itoa(len(values))),
				legacyDirectRelationshipProbe(prefix+"materialization_elapsed", materializationElapsed.String()),
			)
			if len(values) > directBitmapMembershipMaxDynamicBatchEQValues {
				probes = append(probes,
					legacyDirectRelationshipProbe(prefix+"applied", "false"),
					legacyDirectRelationshipProbe(prefix+"reason", "key_count_exceeds_cap"),
					legacyDirectRelationshipProbe(prefix+"cap", strconv.Itoa(directBitmapMembershipMaxDynamicBatchEQValues)),
					legacyDirectRelationshipProbe(prefix+"candidate_rows", strconv.Itoa(len(targetRows))),
					legacyDirectRelationshipProbe(prefix+"target_rows_after", strconv.Itoa(len(targetRows))),
					legacyDirectRelationshipProbe(prefix+"query_elapsed", "0s"),
					legacyDirectRelationshipProbe(prefix+"intersect_elapsed", "0s"),
					legacyDirectRelationshipProbe(prefix+"elapsed", time.Since(start).String()),
				)
				continue
			}
			queryStart := time.Now()
			candidateRows, diagnostics, err := e.legacyDirectRelationshipGraphEqualitySeedTargetRows(ctx, request, candidate.targetTable, candidate.targetRole, candidate.targetField, values)
			queryElapsed := time.Since(queryStart)
			if err != nil || diagnostics.BlocksNative() {
				return probes, changed, diagnostics, err
			}
			intersectStart := time.Now()
			nextRows := legacyDirectRelationshipIntersectRownums(targetRows, candidateRows)
			intersectElapsed := time.Since(intersectStart)
			candidateChanged := len(nextRows) < len(targetRows)
			if candidateChanged {
				rowsByRole[candidate.targetRole] = nextRows
				fullDomainRowsByRole[candidate.targetRole] = false
				changed = true
				roundChanged = true
			}
			appliedCandidates++
			probes = append(probes,
				legacyDirectRelationshipProbe(prefix+"applied", "true"),
				legacyDirectRelationshipProbe(prefix+"reason", "reduced_role_equality"),
				legacyDirectRelationshipProbe(prefix+"candidate_rows", strconv.Itoa(len(candidateRows))),
				legacyDirectRelationshipProbe(prefix+"target_rows_after", strconv.Itoa(len(rowsByRole[candidate.targetRole]))),
				legacyDirectRelationshipProbe(prefix+"query_elapsed", queryElapsed.String()),
				legacyDirectRelationshipProbe(prefix+"intersect_elapsed", intersectElapsed.String()),
				legacyDirectRelationshipProbe(prefix+"elapsed", time.Since(start).String()),
			)
		}
		if !roundChanged {
			break
		}
	}
	probes = append(probes,
		legacyDirectRelationshipProbe(probePrefix+"equality_role_seed_candidates", strconv.Itoa(totalCandidates)),
		legacyDirectRelationshipProbe(probePrefix+"equality_role_seed_applied", strconv.Itoa(appliedCandidates)),
		legacyDirectRelationshipProbe(probePrefix+"equality_role_seed_changed", strconv.FormatBool(changed)),
	)
	return probes, changed, nil, nil
}

func legacyDirectRelationshipGraphEqualityRoleSeedCandidates(request ExecutionRequest, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool) []legacyDirectRelationshipGraphEqualityRoleSeed {
	return legacyDirectRelationshipGraphEqualityRoleSeedCandidatesFromFields(legacyDirectRelationshipGraphEqualityFields(request), rowsByRole, fullDomainRowsByRole)
}

func legacyDirectRelationshipGraphEqualityRoleSeedCandidatesFromFields(equalities []legacyDirectRelationshipGraphEqualityFieldPair, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool) []legacyDirectRelationshipGraphEqualityRoleSeed {
	return legacyDirectRelationshipGraphEqualityRoleSeedCandidatesFromFieldsWithOptions(equalities, rowsByRole, fullDomainRowsByRole, legacyDirectRelationshipGraphEqualityRoleSeedOptions{})
}

func legacyDirectRelationshipGraphEqualityRoleSeedCandidatesFromFieldsWithOptions(equalities []legacyDirectRelationshipGraphEqualityFieldPair, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainRowsByRole map[string]bool, options legacyDirectRelationshipGraphEqualityRoleSeedOptions) []legacyDirectRelationshipGraphEqualityRoleSeed {
	candidates := make([]legacyDirectRelationshipGraphEqualityRoleSeed, 0, len(equalities))
	seen := make(map[string]struct{}, len(equalities)*2)
	for _, equality := range equalities {
		leftRole, leftOK := legacyDirectRelationshipGraphEqualityFieldRole(equality.left)
		rightRole, rightOK := legacyDirectRelationshipGraphEqualityFieldRole(equality.right)
		if !leftOK || !rightOK || leftRole == rightRole {
			continue
		}
		if _, ok := rowsByRole[leftRole]; !ok {
			continue
		}
		if _, ok := rowsByRole[rightRole]; !ok {
			continue
		}
		if candidate, ok := legacyDirectRelationshipGraphEqualityRoleSeedCandidate(leftRole, equality.left, rightRole, equality.right, fullDomainRowsByRole); ok {
			if !legacyDirectRelationshipGraphEqualityRoleSeedCandidateAllowed(candidate, rowsByRole, options) {
				continue
			}
			key := candidate.sourceRole + "." + directBitmapFieldPhysicalName(candidate.sourceField) + "->" + candidate.targetRole + "." + directBitmapFieldPhysicalName(candidate.targetField)
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				candidates = append(candidates, candidate)
			}
		}
		if candidate, ok := legacyDirectRelationshipGraphEqualityRoleSeedCandidate(rightRole, equality.right, leftRole, equality.left, fullDomainRowsByRole); ok {
			if !legacyDirectRelationshipGraphEqualityRoleSeedCandidateAllowed(candidate, rowsByRole, options) {
				continue
			}
			key := candidate.sourceRole + "." + directBitmapFieldPhysicalName(candidate.sourceField) + "->" + candidate.targetRole + "." + directBitmapFieldPhysicalName(candidate.targetField)
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				candidates = append(candidates, candidate)
			}
		}
	}
	return candidates
}

func legacyDirectRelationshipGraphEqualityRoleSeedCandidateAllowed(candidate legacyDirectRelationshipGraphEqualityRoleSeed, rowsByRole map[string][]qsbridge.QuantaRownum, options legacyDirectRelationshipGraphEqualityRoleSeedOptions) bool {
	if options.maxSourceRows <= 0 {
		return true
	}
	return len(rowsByRole[candidate.sourceRole]) <= options.maxSourceRows
}

func legacyDirectRelationshipGraphEqualityRoleSeedCandidate(sourceRole string, sourceField qsbridge.FieldRef, targetRole string, targetField qsbridge.FieldRef, fullDomainRowsByRole map[string]bool) (legacyDirectRelationshipGraphEqualityRoleSeed, bool) {
	if fullDomainRowsByRole[sourceRole] || !fullDomainRowsByRole[targetRole] {
		return legacyDirectRelationshipGraphEqualityRoleSeed{}, false
	}
	if !legacyDirectRelationshipGraphEqualitySeedFieldSupported(sourceField) || !legacyDirectRelationshipGraphEqualitySeedFieldSupported(targetField) {
		return legacyDirectRelationshipGraphEqualityRoleSeed{}, false
	}
	return legacyDirectRelationshipGraphEqualityRoleSeed{
		sourceRole:  sourceRole,
		sourceTable: sourceField.Table.Table,
		sourceField: sourceField,
		targetRole:  targetRole,
		targetTable: targetField.Table.Table,
		targetField: targetField,
	}, true
}

func legacyDirectRelationshipGraphEqualitySeedFieldSupported(field qsbridge.FieldRef) bool {
	return field.Table.Table != "" && directBitmapFieldPhysicalName(field) != "" && field.Type == qsbridge.DataTypeInt
}

type legacyDirectRelationshipGraphEqualityFieldPair struct {
	left  qsbridge.FieldRef
	right qsbridge.FieldRef
}

func legacyDirectRelationshipGraphEqualityFields(request ExecutionRequest) []legacyDirectRelationshipGraphEqualityFieldPair {
	var pairs []legacyDirectRelationshipGraphEqualityFieldPair
	if legacyDirectRelationshipGraphEqualityRequestSupportsSemijoinSeed(request) {
		for _, predicate := range request.Predicates {
			if !legacyDirectRelationshipGraphEqualityPredicateSupportsSemijoinSeed(predicate) {
				continue
			}
			pairs = append(pairs, legacyDirectRelationshipGraphEqualityFieldsFromExpr(predicate.Expr)...)
		}
	}
	return pairs
}

func legacyDirectRelationshipGraphEqualityPredicateSupportsSemijoinSeed(predicate qsbridge.Predicate) bool {
	if predicate.Scope != qsbridge.PredicateScopeWhere {
		return false
	}
	return predicate.Placement == qsbridge.PredicateResidualScan || predicate.Placement == qsbridge.PredicateResidualJoin
}

func legacyDirectRelationshipGraphEqualityRequestSupportsSemijoinSeed(request ExecutionRequest) bool {
	for _, join := range request.Joins {
		if !legacyDirectRelationshipGraphEqualityJoinSupportsSemijoinSeed(join) {
			return false
		}
	}
	return true
}

func legacyDirectRelationshipGraphEqualityJoinSupportsSemijoinSeed(join qsbridge.JoinEdge) bool {
	switch join.Kind {
	case qsbridge.JoinKindLeftOuter, qsbridge.JoinKindRightOuter, qsbridge.JoinKindFullOuter:
		return false
	default:
		return true
	}
}

func legacyDirectRelationshipGraphEqualityFieldsFromExpr(expr qsbridge.Expr) []legacyDirectRelationshipGraphEqualityFieldPair {
	switch typed := expr.(type) {
	case qsbridge.BinaryExpr:
		return legacyDirectRelationshipGraphEqualityFieldsFromBinary(typed)
	case *qsbridge.BinaryExpr:
		if typed == nil {
			return nil
		}
		return legacyDirectRelationshipGraphEqualityFieldsFromBinary(*typed)
	default:
		return nil
	}
}

func legacyDirectRelationshipGraphEqualityFieldsFromBinary(binary qsbridge.BinaryExpr) []legacyDirectRelationshipGraphEqualityFieldPair {
	if binary.Op == qsbridge.BinaryOpAnd {
		pairs := legacyDirectRelationshipGraphEqualityFieldsFromExpr(binary.Left)
		pairs = append(pairs, legacyDirectRelationshipGraphEqualityFieldsFromExpr(binary.Right)...)
		return pairs
	}
	if binary.Op != qsbridge.BinaryOpEqual {
		return nil
	}
	left, leftOK := legacyDirectRelationshipResidualFieldExpr(binary.Left)
	right, rightOK := legacyDirectRelationshipResidualFieldExpr(binary.Right)
	if !leftOK || !rightOK {
		return nil
	}
	return []legacyDirectRelationshipGraphEqualityFieldPair{{left: left, right: right}}
}

func legacyDirectRelationshipGraphEqualityFieldRole(field qsbridge.FieldRef) (string, bool) {
	role := strings.ToLower(materializationFieldRole(field.Table.Table, field))
	if role == "" {
		return "", false
	}
	return role, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphEqualitySeedValues(ctx context.Context, request ExecutionRequest, table string, role string, field qsbridge.FieldRef, rows []qsbridge.QuantaRownum) ([]*big.Int, time.Duration, qsbridge.DiagnosticSet, error) {
	projectionField := directBitmapMembershipProjectionField(field)
	materialization := e.legacyDirectRelationshipTimeMaterializationForRole(request, table, role)
	materialization.Index = table
	materialization.Rownums = append([]qsbridge.QuantaRownum(nil), rows...)
	materialization.ProjectionFields = []qsbridge.QuantaProjectionField{projectionField}
	start := time.Now()
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, e.projectionMaterializationKernel(), materialization)
	elapsed := time.Since(start)
	if err != nil || diagnostics.BlocksNative() {
		return nil, elapsed, diagnostics, err
	}
	values, ok := directBitmapProjectedValues(rowSet, field)
	if !ok {
		return nil, elapsed, legacyDirectRelationshipDiagnostic(fmt.Sprintf("graph equality seed missing projected field %s.%s", table, directBitmapFieldPhysicalName(field))), nil
	}
	bigInts, ok := legacyDirectRelationshipGraphEqualitySeedBatchValues(values)
	if !ok {
		return nil, elapsed, legacyDirectRelationshipDiagnostic(fmt.Sprintf("graph equality seed cannot convert %s.%s values to BSI keys", table, directBitmapFieldPhysicalName(field))), nil
	}
	return bigInts, elapsed, nil, nil
}

func legacyDirectRelationshipGraphEqualitySeedBatchValues(cells []qsbridge.ResultCell) ([]*big.Int, bool) {
	seen := make(map[string]struct{}, len(cells))
	values := make([]*big.Int, 0, len(cells))
	for _, cell := range cells {
		value, ok := directBitmapMembershipCellBigInt(cell)
		if !ok {
			return nil, false
		}
		if value == nil {
			continue
		}
		key := value.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, new(big.Int).Set(value))
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Cmp(values[j]) < 0
	})
	return values, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphEqualitySeedTargetRows(ctx context.Context, request ExecutionRequest, table string, role string, field qsbridge.FieldRef, values []*big.Int) ([]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	if len(values) == 0 {
		return nil, nil, nil
	}
	provider := e.legacyDirectRelationshipSessionProvider()
	if provider == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "graph equality seed has no session provider"),
		}, nil
	}
	fragment := qsbridge.QuantaQueryFragment{
		Index:     table,
		Role:      qsbridge.TableInstanceID(role),
		Field:     directBitmapFieldPhysicalName(field),
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
		Values:    cloneBigIntSlice(values),
	}
	tableRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{fragment}})
	tableRequest.Sources = append([]qsbridge.TableInstance(nil), request.Sources...)
	tableRequest.Materialization = e.legacyDirectRelationshipTimeMaterializationForRole(request, table, role)
	session, diagnostics, err := provider.BorrowDirectSession(ctx, tableRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "graph equality seed received nil session"),
		}, nil
	}
	result, queryDiagnostics, queryErr := session.QueryBitmap(ctx, tableRequest)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, queryErr
	}
	return append([]qsbridge.QuantaRownum(nil), result.Rownums...), diagnostics, nil
}
