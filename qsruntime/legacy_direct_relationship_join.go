package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// LegacyDirectRelationshipVectorJoinExecutor executes relationship-vector joins.
type LegacyDirectRelationshipVectorJoinExecutor struct {
	KernelAdapter       RelationshipVectorKernel
	Source              *source.QuantaSource
	Sessions            DirectSessionProvider
	TableCache          *core.TableCacheStruct
	Materializer        ProjectionMaterializer
	Materialization     ProjectionMaterializationKernel
	ProjectionBSIReader NativeProjectionBSIReader
	// SameRowComparison filters same-row BSI predicates without materializing compared values.
	SameRowComparison SameRowComparisonKernel
	// ProjectionCache reuses relationship-vector FK BSI projections during one execution request.
	ProjectionCache *LegacyDirectRelationshipVectorProjectionCache
	// RelationshipProjectionReader projects relationship-vector FK BSIs without a source-backed session.
	RelationshipProjectionReader legacyDirectRelationshipVectorProjectionReader
	// RelationshipSourceKeyReader projects source-domain keys for relationship-vector expansion.
	RelationshipSourceKeyReader LegacyDirectRelationshipVectorSourceKeyReader
	// ReverseArtifacts optionally serves parent-to-child expansion through reverse artifacts.
	ReverseArtifacts *RelationshipVectorReverseArtifactManager
	// ReverseArtifactCandidateReader exposes physical-tier maintained reverse artifacts.
	ReverseArtifactCandidateReader LegacyDirectRelationshipVectorReverseArtifactCandidateReader
	// RelationshipAggregateReader exposes physical-tier relationship-vector aggregate primitives.
	RelationshipAggregateReader LegacyDirectRelationshipVectorAggregateReader
	// ApplyRecommendedEdgeOrder enables dependency-ordered graph reduction.
	ApplyRecommendedEdgeOrder bool
}

type legacyDirectRelationshipEdge struct {
	childRole                string
	childTable               string
	childField               string
	parentRole               string
	parentTable              string
	parentField              string
	capabilities             qsbridge.RelationshipCapabilities
	sqlKind                  qsbridge.JoinKind
	leftOuterPreservesParent bool
	projectionScope          qsbridge.RelationshipVectorProjectionScope
}

type legacyDirectRelationshipPair struct {
	child  qsbridge.QuantaRownum
	parent qsbridge.QuantaRownum
}

type legacyDirectRelationshipReduceTiming struct {
	domainMappingCacheHit               bool
	domainMappingCacheMode              string
	projectionElapsed                   time.Duration
	projectionCacheHit                  bool
	projectionRows                      int
	fkProjectionRows                    int
	fkChildOverlapRows                  int
	fkProjectionInitialCoverage         qsbridge.RelationshipVectorProjectionCoverage
	fkProjectionCoverage                qsbridge.RelationshipVectorProjectionCoverage
	fkProjectionScope                   string
	fkProjectionRetryRows               int
	fkProjectionRetryOverlap            int
	fkProjectionRetryCoverage           qsbridge.RelationshipVectorProjectionCoverage
	parentKeyElapsed                    time.Duration
	parentKeyMaterialization            bool
	parentKeyRows                       int
	reverseArtifactUsed                 bool
	reverseArtifactSkipReason           string
	reverseArtifactMode                 string
	reverseArtifactCacheHit             bool
	reverseArtifactSourceValues         int
	reverseArtifactCandidateRows        int
	reverseArtifactNarrowedRows         int
	reverseArtifactElapsed              time.Duration
	reverseArtifactLookupElapsed        time.Duration
	reverseArtifactFanoutElapsed        time.Duration
	reverseArtifactClientRPCElapsed     time.Duration
	reverseArtifactClientRPCMaxElapsed  time.Duration
	reverseArtifactResponseMergeElapsed time.Duration
	reverseArtifactRowMergeElapsed      time.Duration
	reverseArtifactParentMergeElapsed   time.Duration
	reverseArtifactSortElapsed          time.Duration
	reverseArtifactLocalMode            string
	reverseArtifactTargetCandidateMode  string
	reverseArtifactSourceElapsed        time.Duration
	reverseArtifactReadElapsed          time.Duration
	reverseArtifactRowConversionElapsed time.Duration
	reverseArtifactMapConversionElapsed time.Duration
	reverseArtifactNarrowElapsed        time.Duration
	reverseArtifactParentElapsed        time.Duration
	reverseArtifactProjectElapsed       time.Duration
	reverseArtifactProjectMode          string
	reverseArtifactCacheSetElapsed      time.Duration
	matchedRows                         int
	childRetainCovered                  bool
	childRetainMode                     string
	batchEqualElapsed                   time.Duration
	singleKeyFoundSetElapsed            time.Duration
	singleKeyEqualElapsed               time.Duration
	valueVectorElapsed                  time.Duration
	intersectElapsed                    time.Duration
	rownumElapsed                       time.Duration
	pairElapsed                         time.Duration
}

type legacyDirectRelationshipReverseArtifactLocalTiming struct {
	mode                 string
	targetCandidateMode  string
	sourceValues         int
	candidateRows        int
	elapsed              time.Duration
	lookupElapsed        time.Duration
	fanoutElapsed        time.Duration
	clientRPCElapsed     time.Duration
	maxClientRPCElapsed  time.Duration
	responseMergeElapsed time.Duration
	rowMergeElapsed      time.Duration
	parentMergeElapsed   time.Duration
	sortElapsed          time.Duration
	sourceElapsed        time.Duration
	readElapsed          time.Duration
	rowConversionElapsed time.Duration
	mapConversionElapsed time.Duration
	narrowElapsed        time.Duration
	parentElapsed        time.Duration
}

type legacyDirectRelationshipReduceOptions struct {
	omitFullDomainTargetCandidates bool
}

type legacyDirectRelationshipGraphReductionSummary struct {
	edges                         int
	totalReduceElapsed            time.Duration
	totalProjectionElapsed        time.Duration
	totalParentKeyElapsed         time.Duration
	totalReverseArtifactElapsed   time.Duration
	totalReverseArtifactRPC       time.Duration
	totalReverseArtifactRPCMax    time.Duration
	totalValueVectorElapsed       time.Duration
	totalBatchEqualElapsed        time.Duration
	totalIntersectElapsed         time.Duration
	totalPairElapsed              time.Duration
	totalChildRetainElapsed       time.Duration
	totalProjectionRows           int
	totalParentRows               int
	totalChildRows                int
	totalJoinedRows               int
	totalReverseArtifactSource    int
	totalReverseArtifactCandidate int
	totalReverseArtifactNarrowed  int
	totalMatchedRows              int
	maxReduceElapsed              time.Duration
	maxReduceLabel                string
	maxProjectionElapsed          time.Duration
	maxProjectionLabel            string
	maxReverseArtifactElapsed     time.Duration
	maxReverseArtifactLabel       string
	maxChildRetainElapsed         time.Duration
	maxChildRetainLabel           string
	edgeSummaries                 []string
}

type legacyDirectRelationshipProjectedFKReduceTiming struct {
	batchEqualUsed           bool
	singleKeyEqualUsed       bool
	valueVectorUsed          bool
	batchEqualElapsed        time.Duration
	singleKeyFoundSetElapsed time.Duration
	singleKeyEqualElapsed    time.Duration
	valueVectorElapsed       time.Duration
	intersectElapsed         time.Duration
	rownumElapsed            time.Duration
	pairElapsed              time.Duration
}

type legacyDirectRelationshipRoleFallback struct {
	table string
	role  string
	field string
}

type legacyDirectRelationshipGraphScratchpad struct {
	initialRowsByRole        map[string][]qsbridge.QuantaRownum
	initialRowsByTable       map[string][]qsbridge.QuantaRownum
	fullDomainInitialRoleSet map[string]bool
	alignedParentRowsByEdge  map[string]legacyDirectRelationshipAlignedParentRows
}

type legacyDirectRelationshipAlignedParentRows struct {
	childRows     []qsbridge.QuantaRownum
	parentRows    []qsbridge.QuantaRownum
	parentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum
}

func newLegacyDirectRelationshipGraphScratchpad(rowsByRole map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, fullDomainRowsByRole ...map[string]bool) legacyDirectRelationshipGraphScratchpad {
	scratchpad := legacyDirectRelationshipGraphScratchpad{
		initialRowsByRole:        make(map[string][]qsbridge.QuantaRownum, len(rowsByRole)),
		initialRowsByTable:       make(map[string][]qsbridge.QuantaRownum, len(rowsByRole)),
		fullDomainInitialRoleSet: make(map[string]bool, len(rowsByRole)),
		alignedParentRowsByEdge:  make(map[string]legacyDirectRelationshipAlignedParentRows, len(edges)),
	}
	for role, rows := range rowsByRole {
		scratchpad.initialRowsByRole[role] = append([]qsbridge.QuantaRownum(nil), rows...)
	}
	if len(fullDomainRowsByRole) > 0 {
		for role, fullDomain := range fullDomainRowsByRole[0] {
			if fullDomain {
				scratchpad.fullDomainInitialRoleSet[role] = true
			}
		}
	}
	for _, edge := range edges {
		scratchpad.storeInitialTableRows(edge.parentTable, rowsByRole[edge.parentKey()])
		scratchpad.storeInitialTableRows(edge.childTable, rowsByRole[edge.childKey()])
	}
	return scratchpad
}

func (s legacyDirectRelationshipGraphScratchpad) storeInitialTableRows(table string, rows []qsbridge.QuantaRownum) {
	key := strings.ToLower(table)
	if key == "" || len(rows) == 0 {
		return
	}
	if existing, ok := s.initialRowsByTable[key]; ok && len(existing) >= len(rows) {
		return
	}
	s.initialRowsByTable[key] = append([]qsbridge.QuantaRownum(nil), rows...)
}

func (s legacyDirectRelationshipGraphScratchpad) initialRowsForRole(role string) ([]qsbridge.QuantaRownum, bool) {
	rows, ok := s.initialRowsByRole[role]
	if !ok {
		return nil, false
	}
	return append([]qsbridge.QuantaRownum(nil), rows...), true
}

func (s legacyDirectRelationshipGraphScratchpad) fullDomainInitialRowsForRole(role string, rows []qsbridge.QuantaRownum) bool {
	if !s.fullDomainInitialRoleSet[role] {
		return false
	}
	initialRows, ok := s.initialRowsByRole[role]
	if !ok {
		return false
	}
	return legacyDirectRelationshipRownumsEqual(initialRows, rows)
}

func legacyDirectRelationshipRownumsEqual(left []qsbridge.QuantaRownum, right []qsbridge.QuantaRownum) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s legacyDirectRelationshipGraphScratchpad) initialRowsForTable(table string) ([]qsbridge.QuantaRownum, bool) {
	rows, ok := s.initialRowsByTable[strings.ToLower(table)]
	if !ok {
		return nil, false
	}
	return append([]qsbridge.QuantaRownum(nil), rows...), true
}

func (s legacyDirectRelationshipGraphScratchpad) storeAlignedParentRows(edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) {
	if s.alignedParentRowsByEdge == nil || len(childRows) == 0 || len(pairs) == 0 {
		return
	}
	parentRows := make([]qsbridge.QuantaRownum, len(childRows))
	if len(pairs) == len(childRows) {
		exact := true
		for i, pair := range pairs {
			if pair.child != childRows[i] {
				exact = false
				break
			}
			parentRows[i] = pair.parent
		}
		if exact {
			s.alignedParentRowsByEdge[legacyDirectRelationshipEdgeAlignmentKey(edge)] = legacyDirectRelationshipAlignedParentRows{
				childRows:  append([]qsbridge.QuantaRownum(nil), childRows...),
				parentRows: parentRows,
			}
			return
		}
	}
	parentByChild := legacyDirectRelationshipParentMapFromPairs(pairs)
	if len(parentByChild) == 0 {
		return
	}
	for i, child := range childRows {
		parent, ok := parentByChild[child]
		if !ok {
			return
		}
		parentRows[i] = parent
	}
	s.alignedParentRowsByEdge[legacyDirectRelationshipEdgeAlignmentKey(edge)] = legacyDirectRelationshipAlignedParentRows{
		childRows:  append([]qsbridge.QuantaRownum(nil), childRows...),
		parentRows: parentRows,
	}
}

func (s legacyDirectRelationshipGraphScratchpad) alignedParentRows(edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, bool) {
	if s.alignedParentRowsByEdge == nil || len(childRows) == 0 {
		return nil, false
	}
	aligned, ok := s.alignedParentRowsByEdge[legacyDirectRelationshipEdgeAlignmentKey(edge)]
	if !ok {
		return nil, false
	}
	if legacyDirectRelationshipRownumsEqual(aligned.childRows, childRows) {
		return append([]qsbridge.QuantaRownum(nil), aligned.parentRows...), true
	}
	if len(aligned.childRows) != len(aligned.parentRows) {
		return nil, false
	}
	parentByChild := aligned.parentByChild
	if parentByChild == nil {
		parentByChild = make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(aligned.childRows))
		for i, child := range aligned.childRows {
			parentByChild[child] = aligned.parentRows[i]
		}
		aligned.parentByChild = parentByChild
		s.alignedParentRowsByEdge[legacyDirectRelationshipEdgeAlignmentKey(edge)] = aligned
	}
	parentRows := make([]qsbridge.QuantaRownum, len(childRows))
	for i, child := range childRows {
		parent, ok := parentByChild[child]
		if !ok {
			return nil, false
		}
		parentRows[i] = parent
	}
	return parentRows, true
}

func legacyDirectRelationshipEdgeAlignmentKey(edge legacyDirectRelationshipEdge) string {
	return strings.Join([]string{
		edge.childKey(),
		edge.parentKey(),
		edge.childTable,
		edge.childField,
		edge.parentTable,
		edge.parentField,
	}, "\x00")
}

const (
	legacyDirectRelationshipFullTimeRangeBeginMillis int64 = -2208988800000 // 1900-01-01T00:00:00Z
	legacyDirectRelationshipFullTimeRangeEndMillis   int64 = 4102444800000  // 2100-01-01T00:00:00Z
)

func (e legacyDirectRelationshipEdge) childKey() string {
	return legacyDirectRelationshipRoleKey(e.childRole, e.childTable)
}

func (e legacyDirectRelationshipEdge) parentKey() string {
	return legacyDirectRelationshipRoleKey(e.parentRole, e.parentTable)
}

func legacyDirectRelationshipTableRoleKey(table qsbridge.TableInstance) string {
	switch {
	case table.Alias != "":
		return strings.ToLower(table.Alias)
	case table.ID != "":
		return strings.ToLower(string(table.ID))
	default:
		return strings.ToLower(table.Table)
	}
}

func legacyDirectRelationshipRoleKey(role string, table string) string {
	if role != "" {
		return strings.ToLower(role)
	}
	return strings.ToLower(table)
}

func legacyDirectRelationshipProjectionFieldRoleKey(field qsbridge.QuantaProjectionField, table string) string {
	if field.Role != "" {
		return strings.ToLower(string(field.Role))
	}
	return strings.ToLower(table)
}

func legacyDirectRelationshipUniqueSourceRoleKey(request ExecutionRequest, table string) string {
	var role string
	for _, source := range request.Sources {
		if !strings.EqualFold(source.Table, table) {
			continue
		}
		next := legacyDirectRelationshipTableRoleKey(source)
		if role != "" && role != next {
			return ""
		}
		role = next
	}
	return role
}

func legacyDirectRelationshipAlignedRoleDebug(aligned map[string][]qsbridge.QuantaRownum) string {
	keys := make([]string, 0, len(aligned))
	for key, rows := range aligned {
		keys = append(keys, fmt.Sprintf("%s:%d", key, len(rows)))
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func legacyDirectRelationshipRoleSetDebug(roles map[string]struct{}) string {
	keys := make([]string, 0, len(roles))
	for role := range roles {
		keys = append(keys, role)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func legacyDirectRelationshipProjectionFieldsDebug(fields []qsbridge.QuantaProjectionField) string {
	keys := make([]string, 0, len(fields))
	for _, field := range fields {
		name := field.PhysicalName
		if name == "" {
			name = field.Field
		}
		if name == "" {
			continue
		}
		role := string(field.Role)
		if role == "" {
			role = field.Index
		}
		if role == "" {
			role = "?"
		}
		keys = append(keys, strings.ToLower(role+"."+name))
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func legacyDirectRelationshipMaterializationFieldProbePrefix(prefix string, ordinal int, field qsbridge.QuantaProjectionField) string {
	table := field.Index
	if table == "" {
		table = "?"
	}
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	if name == "" {
		name = "?"
	}
	role := string(field.Role)
	if role == "" {
		role = table
	}
	return fmt.Sprintf("%sfield_%d_%s_%s_%s_", prefix, ordinal, strings.ToLower(role), strings.ToLower(table), strings.ToLower(name))
}

func legacyDirectRelationshipRequiredAlignmentRoles(request ExecutionRequest, sink string, fields []qsbridge.QuantaProjectionField) map[string]struct{} {
	roles := make(map[string]struct{}, len(fields)+1)
	if sink != "" {
		if role := legacyDirectRelationshipUniqueSourceRoleKey(request, sink); role != "" {
			roles[role] = struct{}{}
		} else {
			roles[strings.ToLower(sink)] = struct{}{}
		}
	}
	for _, field := range fields {
		table := field.Index
		if table == "" {
			table = sink
		}
		role := strings.ToLower(string(field.Role))
		if role == "" && table != "" {
			if sourceRole := legacyDirectRelationshipUniqueSourceRoleKey(request, table); sourceRole != "" {
				role = sourceRole
			} else {
				role = strings.ToLower(table)
			}
		}
		if role != "" {
			roles[role] = struct{}{}
		}
	}
	return roles
}

func legacyDirectRelationshipAlignedHasRoles(aligned map[string][]qsbridge.QuantaRownum, requiredRoles map[string]struct{}) bool {
	for role := range requiredRoles {
		if _, ok := aligned[role]; !ok {
			return false
		}
	}
	return true
}

func legacyDirectRelationshipExpandRequiredAlignmentRoles(requiredRoles map[string]struct{}, sinkRole string, edges []legacyDirectRelationshipEdge) map[string]struct{} {
	expanded := make(map[string]struct{}, len(requiredRoles)+len(edges)+1)
	for role := range requiredRoles {
		if role != "" {
			expanded[role] = struct{}{}
		}
	}
	if sinkRole != "" {
		expanded[sinkRole] = struct{}{}
	}
	for {
		changed := false
		for _, edge := range edges {
			if _, ok := expanded[edge.parentKey()]; !ok {
				continue
			}
			childKey := edge.childKey()
			if childKey == "" {
				continue
			}
			if _, ok := expanded[childKey]; ok {
				continue
			}
			expanded[childKey] = struct{}{}
			changed = true
		}
		if !changed {
			break
		}
	}
	return expanded
}

func legacyDirectRelationshipSyntheticEndpointProjection(field qsbridge.QuantaProjectionField, roleKey string, tableRows []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, alignedRows map[string][]qsbridge.QuantaRownum, probePrefix string) (qsbridge.QuantaProjectionVector, []ExecutionProbe, bool) {
	if field.Type != qsbridge.DataTypeInt {
		return qsbridge.QuantaProjectionVector{}, nil, false
	}
	fieldName := field.PhysicalName
	if fieldName == "" {
		fieldName = field.Field
	}
	table := field.Index
	if table == "" || fieldName == "" {
		return qsbridge.QuantaProjectionVector{}, nil, false
	}
	for _, edge := range edges {
		var sourceRole string
		switch {
		case strings.EqualFold(roleKey, edge.childKey()) &&
			strings.EqualFold(table, edge.childTable) &&
			strings.EqualFold(fieldName, edge.childField):
			sourceRole = edge.parentKey()
		case strings.EqualFold(roleKey, edge.parentKey()) &&
			strings.EqualFold(table, edge.parentTable) &&
			strings.EqualFold(fieldName, edge.parentField):
			sourceRole = edge.parentKey()
		default:
			continue
		}
		sourceRows, ok := alignedRows[sourceRole]
		if !ok || len(sourceRows) != len(tableRows) {
			return qsbridge.QuantaProjectionVector{}, nil, false
		}
		attachStart := time.Now()
		vector := qsbridge.QuantaProjectionVector{Field: field}
		for _, rownum := range sourceRows {
			vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
		}
		attachElapsed := time.Since(attachStart)
		return vector, []ExecutionProbe{
			legacyDirectRelationshipProbe(probePrefix+"role", roleKey),
			legacyDirectRelationshipProbe(probePrefix+"table", table),
			legacyDirectRelationshipProbe(probePrefix+"field", field.Field),
			legacyDirectRelationshipProbe(probePrefix+"rows", strconv.Itoa(len(tableRows))),
			legacyDirectRelationshipProbe(probePrefix+"source", "synthetic_relationship_endpoint"),
			legacyDirectRelationshipProbe(probePrefix+"source_role", sourceRole),
			legacyDirectRelationshipProbe(probePrefix+"fetch_elapsed", time.Duration(0).String()),
			legacyDirectRelationshipProbe(probePrefix+"attach_elapsed", attachElapsed.String()),
		}, true
	}
	return qsbridge.QuantaProjectionVector{}, nil, false
}

// ExecuteRelationshipVectorJoin executes a supported direct relationship-vector shape or reports the explicit boundary.
func (e LegacyDirectRelationshipVectorJoinExecutor) ExecuteRelationshipVectorJoin(ctx context.Context, request ExecutionRequest, vector RelationshipVectorJoinRequest) (result ExecutionResult, err error) {
	start := time.Now()
	defer func() {
		recordExecutionProbes(ctx, result.Probes)
		recorder := ExecutionInstrumentationFromContext(ctx)
		if recorder != nil {
			recorder.ObserveDuration("relationship_join", "phase_execute_relationship_vector_elapsed", time.Since(start), fmt.Sprintf("edges=%d", vector.EdgeCount()))
		}
	}()
	if (e.Source != nil || e.RelationshipProjectionReader != nil) && e.TableCache != nil {
		if e.relationshipVectorProjectionCache(ctx) == nil {
			e.ProjectionCache = NewLegacyDirectRelationshipVectorProjectionCache()
		}
		return e.executeLegacyDirectRelationshipVectorJoin(ctx, request, vector)
	}
	kernelResult, err := ExecuteRelationshipVectorKernel(ctx, e.Kernel(), vector)
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   kernelResult.RootIndex,
			Rownums: kernelResult.CandidateSet,
		},
		Diagnostics: kernelResult.Diagnostics,
	}, err
}

func (e LegacyDirectRelationshipVectorJoinExecutor) relationshipVectorProjectionCache(ctx context.Context) *LegacyDirectRelationshipVectorProjectionCache {
	if cache := RelationshipVectorProjectionCacheFromContext(ctx); cache != nil {
		return cache
	}
	return e.ProjectionCache
}

// Kernel returns the low-level kernel used by the relationship-vector adapter.
func (e LegacyDirectRelationshipVectorJoinExecutor) Kernel() RelationshipVectorKernel {
	if e.KernelAdapter != nil {
		return e.KernelAdapter
	}
	return UnsupportedRelationshipVectorKernel{}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) projectionMaterializationKernel() ProjectionMaterializationKernel {
	if e.Materialization != nil {
		return e.Materialization
	}
	if e.Materializer != nil {
		return ProjectionMaterializerKernelAdapter{Materializer: e.Materializer}
	}
	return nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) sameRowComparisonKernel() SameRowComparisonKernel {
	if e.SameRowComparison != nil {
		return e.SameRowComparison
	}
	if e.Source != nil {
		return LegacyDirectSameRowBSIComparisonKernel{
			Source:     e.Source,
			TableCache: e.TableCache,
			Comparator: LegacyDirectSharedSameRowBSIComparator{
				Source:     e.Source,
				TableCache: e.TableCache,
			},
		}
	}
	return UnsupportedSameRowComparisonKernel{}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) executeLegacyDirectRelationshipVectorJoin(ctx context.Context, request ExecutionRequest, vector RelationshipVectorJoinRequest) (ExecutionResult, error) {
	if vector.EdgeCount() > 1 {
		return e.executeLegacyDirectRelationshipVectorJoinChain(ctx, request, vector)
	}
	edge, diagnostics := e.legacyDirectSingleRelationshipEdge(vector)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	if diagnostics = legacyDirectRelationshipShapeDiagnostics(request, vector); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	if result, handled, err := e.legacyDirectRelationshipCountFromVectorExistence(ctx, request, edge); handled || err != nil {
		return result, err
	}
	parentStart := time.Now()
	parentRows, childRows, joined, pairs, usedChildCandidateSet, diagnostics, err := e.legacyDirectRelationshipRowsFromChildCandidateSet(ctx, request, edge)
	parentElapsed := time.Since(parentStart)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	childElapsed := time.Duration(0)
	prefilterElapsed := time.Duration(0)
	var prefilterProbes []ExecutionProbe
	reduceElapsed := time.Duration(0)
	reduceTiming := legacyDirectRelationshipReduceTiming{}
	if !usedChildCandidateSet {
		parentRows, diagnostics, err = e.legacyDirectRelationshipRownumsForTable(ctx, request, edge.parentTable, edge.parentRole, edge.parentField)
		parentElapsed = time.Since(parentStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		childStart := time.Now()
		childRows, diagnostics, err = e.legacyDirectRelationshipRownumsForTable(ctx, request, edge.childTable, edge.childRole, edge.childField)
		childElapsed = time.Since(childStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		prefilterStart := time.Now()
		rowsByRole := map[string][]qsbridge.QuantaRownum{
			edge.parentKey(): parentRows,
			edge.childKey():  childRows,
		}
		var appliedPrefilterPredicates []int
		prefilterProbes, appliedPrefilterPredicates, diagnostics, err = e.legacyDirectRelationshipApplyResidualRolePrefilters(ctx, request, rowsByRole, []legacyDirectRelationshipEdge{edge})
		prefilterElapsed = time.Since(prefilterStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		request = legacyDirectRelationshipRequestWithoutAppliedResidualPrefilters(request, appliedPrefilterPredicates)
		parentRows = rowsByRole[edge.parentKey()]
		childRows = rowsByRole[edge.childKey()]
		reduceStart := time.Now()
		joined, pairs, reduceTiming, diagnostics, err = e.legacyDirectRelationshipReduceWithTiming(ctx, request, edge, parentRows, childRows)
		reduceElapsed = time.Since(reduceStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
	}
	membershipStart := time.Now()
	joined, pairs, diagnostics, err = e.legacyDirectRelationshipApplyMemberships(ctx, request, edge, joined, pairs)
	membershipElapsed := time.Since(membershipStart)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	result := ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   edge.childTable,
			Rownums: joined,
		},
		Count: uint64(len(joined)),
		Probes: []ExecutionProbe{
			legacyDirectRelationshipProbe("parent_rows", strconv.Itoa(len(parentRows))),
			legacyDirectRelationshipProbe("child_rows", strconv.Itoa(len(childRows))),
			legacyDirectRelationshipProbe("joined_rows", strconv.Itoa(len(joined))),
			legacyDirectRelationshipProbe("joined_pairs", strconv.Itoa(len(pairs))),
			legacyDirectRelationshipProbe("membership_count", strconv.Itoa(len(request.Memberships))),
			legacyDirectRelationshipProbe("candidate_set_index", request.CandidateSet.Index),
			legacyDirectRelationshipProbe("candidate_set_rows", strconv.Itoa(len(request.CandidateSet.Rownums))),
			legacyDirectRelationshipProbe("used_child_candidate_set", strconv.FormatBool(usedChildCandidateSet)),
			legacyDirectRelationshipProbe("single_parent_rows_seed", legacyDirectRelationshipInitialReadSeed(usedChildCandidateSet, len(parentRows))),
			legacyDirectRelationshipProbe("single_child_rows_seed", legacyDirectRelationshipChildInitialReadSeed(usedChildCandidateSet, len(childRows))),
			legacyDirectRelationshipProbe("single_projection_rows", strconv.Itoa(reduceTiming.projectionRows)),
			legacyDirectRelationshipProbe("single_domain_mapping_cache_hit", strconv.FormatBool(reduceTiming.domainMappingCacheHit)),
			legacyDirectRelationshipProbe("single_domain_mapping_cache_mode", reduceTiming.domainMappingCacheMode),
			legacyDirectRelationshipProbe("single_fk_projection_rows", strconv.Itoa(reduceTiming.fkProjectionRows)),
			legacyDirectRelationshipProbe("single_fk_child_overlap_rows", strconv.Itoa(reduceTiming.fkChildOverlapRows)),
			legacyDirectRelationshipProbe("single_fk_projection_initial_coverage_status", string(reduceTiming.fkProjectionInitialCoverage.Status)),
			legacyDirectRelationshipProbe("single_fk_projection_coverage_status", string(reduceTiming.fkProjectionCoverage.Status)),
			legacyDirectRelationshipProbe("single_fk_projection_recovery_policy", string(reduceTiming.fkProjectionCoverage.RecoveryPolicy)),
			legacyDirectRelationshipProbe("single_fk_projection_scope", reduceTiming.fkProjectionScope),
			legacyDirectRelationshipProbe("single_fk_projection_retry_rows", strconv.Itoa(reduceTiming.fkProjectionRetryRows)),
			legacyDirectRelationshipProbe("single_fk_projection_retry_overlap_rows", strconv.Itoa(reduceTiming.fkProjectionRetryOverlap)),
			legacyDirectRelationshipProbe("single_fk_projection_retry_coverage_status", string(reduceTiming.fkProjectionRetryCoverage.Status)),
			legacyDirectRelationshipProbe("single_projection_cache_hit", strconv.FormatBool(reduceTiming.projectionCacheHit)),
			legacyDirectRelationshipProbe("single_projection_elapsed", reduceTiming.projectionElapsed.String()),
			legacyDirectRelationshipProbe("single_parent_key_rows", strconv.Itoa(reduceTiming.parentKeyRows)),
			legacyDirectRelationshipProbe("single_parent_key_elapsed", reduceTiming.parentKeyElapsed.String()),
			legacyDirectRelationshipProbe("single_parent_key_materialization", strconv.FormatBool(reduceTiming.parentKeyMaterialization)),
			legacyDirectRelationshipProbe("single_matched_rows", strconv.Itoa(reduceTiming.matchedRows)),
			legacyDirectRelationshipProbe("single_batch_equal_elapsed", reduceTiming.batchEqualElapsed.String()),
			legacyDirectRelationshipProbe("single_single_key_foundset_elapsed", reduceTiming.singleKeyFoundSetElapsed.String()),
			legacyDirectRelationshipProbe("single_single_key_equal_elapsed", reduceTiming.singleKeyEqualElapsed.String()),
			legacyDirectRelationshipProbe("single_value_vector_elapsed", reduceTiming.valueVectorElapsed.String()),
			legacyDirectRelationshipProbe("single_intersect_elapsed", reduceTiming.intersectElapsed.String()),
			legacyDirectRelationshipProbe("single_rownum_elapsed", reduceTiming.rownumElapsed.String()),
			legacyDirectRelationshipProbe("single_pair_elapsed", reduceTiming.pairElapsed.String()),
			legacyDirectRelationshipProbe("phase_parent_rows_elapsed", parentElapsed.String()),
			legacyDirectRelationshipProbe("phase_child_rows_elapsed", childElapsed.String()),
			legacyDirectRelationshipProbe("phase_single_residual_prefilter_elapsed", prefilterElapsed.String()),
			legacyDirectRelationshipProbe("phase_reduce_elapsed", reduceElapsed.String()),
			legacyDirectRelationshipProbe("phase_membership_elapsed", membershipElapsed.String()),
		},
	}
	result.Probes = append(result.Probes, prefilterProbes...)
	if len(request.SQLAggregates) == 0 {
		return e.legacyDirectRelationshipProjectionResult(ctx, request, edge, joined, pairs, result, parentRows)
	}
	if edge.sqlKind == qsbridge.JoinKindLeftOuter && edge.leftOuterPreservesParent {
		return e.legacyDirectRelationshipLeftOuterAggregateResult(ctx, request, edge, parentRows, pairs, result)
	}
	return e.legacyDirectRelationshipAggregateResult(ctx, request, edge, joined, pairs, result)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipCountFromVectorExistence(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge) (ExecutionResult, bool, error) {
	if !legacyDirectRelationshipCanCountFromVectorExistence(request, edge) {
		return ExecutionResult{}, false, nil
	}
	projectionStart := time.Now()
	fkBSI, cacheHit, diagnostics, err := e.legacyDirectRelationshipProjectedFullFKBSI(ctx, request, edge)
	projectionElapsed := time.Since(projectionStart)
	result := ExecutionResult{Diagnostics: diagnostics}
	if err != nil || diagnostics.BlocksNative() {
		return result, true, err
	}
	if fkBSI == nil || fkBSI.GetExistenceBitmap() == nil {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic(
			fmt.Sprintf("relationship-vector count fast path did not return existence bitmap for %s.%s", edge.childTable, edge.childField),
		)...)
		return result, true, nil
	}
	existence := fkBSI.GetExistenceBitmap()
	result.Count = existence.GetCardinality()
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("count_fast_path", "relationship_vector_existence"),
		legacyDirectRelationshipProbe("count_fast_path_child_table", edge.childTable),
		legacyDirectRelationshipProbe("count_fast_path_child_field", edge.childField),
		legacyDirectRelationshipProbe("count_fast_path_rows", strconv.FormatUint(result.Count, 10)),
		legacyDirectRelationshipProbe("count_fast_path_projection_cache_hit", strconv.FormatBool(cacheHit)),
		legacyDirectRelationshipProbe("phase_count_fast_path_projection_elapsed", projectionElapsed.String()),
	)
	return directBitmapCountAggregateResult(request, result), true, nil
}

func legacyDirectRelationshipCanCountFromVectorExistence(request ExecutionRequest, edge legacyDirectRelationshipEdge) bool {
	if edge.sqlKind != qsbridge.JoinKindInner {
		return false
	}
	if len(request.GroupBy) > 0 || len(request.SQLAggregates) == 0 || !directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) {
		return false
	}
	if directBitmapHasResidualScanPredicates(request) || len(request.Memberships) > 0 {
		return false
	}
	if request.HasCandidateSet || request.CandidateSet.Index != "" || len(request.CandidateSet.Rownums) > 0 {
		return false
	}
	if len(request.Query.Fragments) > 0 || len(request.Query.Seeds) > 0 || !request.Query.Filter.Empty() {
		return false
	}
	return edge.childTable != "" && edge.childField != ""
}

func (e LegacyDirectRelationshipVectorJoinExecutor) executeLegacyDirectRelationshipVectorJoinChain(ctx context.Context, request ExecutionRequest, vector RelationshipVectorJoinRequest) (ExecutionResult, error) {
	if diagnostics := legacyDirectRelationshipChainShapeDiagnostics(request, vector); diagnostics.BlocksNative() {
		return e.executeLegacyDirectRelationshipVectorJoinGraph(ctx, request, vector)
	}
	edges, diagnostics := e.legacyDirectRelationshipOrderedChainEdges(vector)
	if diagnostics.BlocksNative() {
		return e.executeLegacyDirectRelationshipVectorJoinGraph(ctx, request, vector)
	}
	result := ExecutionResult{}
	parentRows, diagnostics, err := e.legacyDirectRelationshipRownumsForTable(ctx, request, edges[0].parentTable, edges[0].parentRole, edges[0].parentField)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("chain_edges", strconv.Itoa(len(edges))),
		legacyDirectRelationshipProbe("chain_root_parent_rows", strconv.Itoa(len(parentRows))),
	)
	currentParentRows := parentRows
	var finalRows []qsbridge.QuantaRownum
	for i, edge := range edges {
		childStart := time.Now()
		childRows, diagnostics, err := e.legacyDirectRelationshipRownumsForTable(ctx, request, edge.childTable, edge.childRole, edge.childField)
		childElapsed := time.Since(childStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		reduceStart := time.Now()
		joined, _, diagnostics, err := e.legacyDirectRelationshipReduce(ctx, request, edge, currentParentRows, childRows)
		reduceElapsed := time.Since(reduceStart)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		prefix := fmt.Sprintf("chain_edge_%d_", i+1)
		result.Probes = append(result.Probes,
			legacyDirectRelationshipProbe(prefix+"parent_table", edge.parentTable),
			legacyDirectRelationshipProbe(prefix+"child_table", edge.childTable),
			legacyDirectRelationshipProbe(prefix+"parent_rows", strconv.Itoa(len(currentParentRows))),
			legacyDirectRelationshipProbe(prefix+"child_rows", strconv.Itoa(len(childRows))),
			legacyDirectRelationshipProbe(prefix+"joined_rows", strconv.Itoa(len(joined))),
			legacyDirectRelationshipProbe(prefix+"phase_child_rows_elapsed", childElapsed.String()),
			legacyDirectRelationshipProbe(prefix+"phase_reduce_elapsed", reduceElapsed.String()),
		)
		currentParentRows = joined
		finalRows = joined
	}
	result.RowSet = qsbridge.QuantaProjectedRowSet{
		Index:   edges[len(edges)-1].childTable,
		Rownums: finalRows,
	}
	result.Count = uint64(len(finalRows))
	return directBitmapCountAggregateResult(request, result), nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) executeLegacyDirectRelationshipVectorJoinGraph(ctx context.Context, request ExecutionRequest, vector RelationshipVectorJoinRequest) (ExecutionResult, error) {
	if diagnostics := legacyDirectRelationshipGraphShapeDiagnostics(request, vector); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	edges := make([]legacyDirectRelationshipEdge, 0, len(vector.Edges))
	for _, planned := range vector.Edges {
		edge, diagnostics := e.legacyDirectRelationshipEdge(planned)
		if diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, nil
		}
		edges = append(edges, edge)
	}
	if shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape(edges); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	} else if ok {
		return e.legacyDirectRelationshipSiblingRootGraphAggregateResult(ctx, request, edges, shape)
	}
	edges, pruneProbes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)
	initialRowsStart := time.Now()
	rowsByTable, initialRowProbes, fullDomainInitialRowsByRole, diagnostics, err := e.legacyDirectRelationshipInitialGraphRows(ctx, request, edges)
	initialRowsElapsed := time.Since(initialRowsStart)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	rowsByTableBeforePrefilter := legacyDirectRelationshipCloneRowsByRole(rowsByTable)
	prefilterStart := time.Now()
	prefilterProbes, appliedPrefilterPredicates, diagnostics, err := e.legacyDirectRelationshipApplyResidualRolePrefilters(ctx, request, rowsByTable, edges)
	prefilterElapsed := time.Since(prefilterStart)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	legacyDirectRelationshipRetainUnchangedFullDomainRoles(fullDomainInitialRowsByRole, rowsByTableBeforePrefilter, rowsByTable)
	request = legacyDirectRelationshipRequestWithoutAppliedResidualPrefilters(request, appliedPrefilterPredicates)
	equalitySeedEnabled := legacyDirectRelationshipGraphEqualityRoleSeedEnabled()
	equalitySeedFields := legacyDirectRelationshipGraphEqualityFields(request)
	if !equalitySeedEnabled {
		equalitySeedFields = nil
	}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(rowsByTable, edges, fullDomainInitialRowsByRole)
	result := ExecutionResult{Probes: []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_edges", strconv.Itoa(len(edges))),
		legacyDirectRelationshipProbe("graph_tables", strconv.Itoa(len(rowsByTable))),
		legacyDirectRelationshipProbe("graph_equality_role_seed_enabled", strconv.FormatBool(equalitySeedEnabled)),
		legacyDirectRelationshipProbe("graph_equality_role_seed_fields", strconv.Itoa(len(equalitySeedFields))),
		legacyDirectRelationshipProbe("graph_scratchpad_roles", strconv.Itoa(len(scratchpad.initialRowsByRole))),
		legacyDirectRelationshipProbe("graph_scratchpad_tables", strconv.Itoa(len(scratchpad.initialRowsByTable))),
		legacyDirectRelationshipProbe("phase_graph_initial_rows_elapsed", initialRowsElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_residual_prefilter_elapsed", prefilterElapsed.String()),
	}}
	result.Probes = append(result.Probes, pruneProbes...)
	result.Probes = append(result.Probes, initialRowProbes...)
	result.Probes = append(result.Probes, prefilterProbes...)
	iterations := 0
	singlePassApplied := false
	singlePassReason := "not_evaluated"
	reductionSummary := legacyDirectRelationshipGraphReductionSummary{}
	reductionStart := time.Now()
	for {
		iterations++
		changed := false
		if len(equalitySeedFields) > 0 {
			equalitySeedProbes, equalitySeedChanged, diagnostics, err := e.legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithPrefix(ctx, request, equalitySeedFields, rowsByTable, fullDomainInitialRowsByRole, fmt.Sprintf("graph_iter_%d_pre_", iterations))
			result.Probes = append(result.Probes, equalitySeedProbes...)
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			if err != nil || result.Diagnostics.BlocksNative() {
				return result, err
			}
			changed = equalitySeedChanged
		}
		edgeOrderPolicy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByTable, e.ApplyRecommendedEdgeOrder)
		result.Probes = append(result.Probes, legacyDirectRelationshipEdgeOrderPolicyProbes(fmt.Sprintf("graph_iter_%d_", iterations), edgeOrderPolicy)...)
		executionCandidates := legacyDirectRelationshipEdgeOrderExecutionCandidates(edges, edgeOrderPolicy)
		singlePassPolicy := legacyDirectRelationshipSinglePassParentToChildPolicy(executionCandidates)
		if iterations != 1 {
			singlePassPolicy.Eligible = false
			singlePassPolicy.Reason = "not_first_iteration"
		}
		if iterations == 1 {
			singlePassReason = singlePassPolicy.Reason
		}
		for edgeIndex, candidate := range executionCandidates {
			edge := candidate.Edge
			parentRows := rowsByTable[edge.parentKey()]
			childRows := rowsByTable[edge.childKey()]
			probePrefix := fmt.Sprintf("graph_iter_%d_edge_%d_", iterations, edgeIndex+1)
			projectionPolicy := legacyDirectRelationshipProjectionPolicy(
				edge,
				childRows,
				scratchpad,
				legacyDirectRelationshipEdgeOrderRemainingProjectionReuse(executionCandidates, edgeIndex, edge),
			)
			projectionRows, projectionPolicy := e.legacyDirectRelationshipProjectionRowsForGraphReduce(ctx, request, edge, childRows, scratchpad, projectionPolicy)
			edgeReduceStart := time.Now()
			reduceOptions := legacyDirectRelationshipReduceOptions{
				omitFullDomainTargetCandidates: scratchpad.fullDomainInitialRowsForRole(edge.childKey(), childRows),
			}
			joined, pairs, reduceTiming, diagnostics, err := e.legacyDirectRelationshipReduceWithProjectionRowsOptions(ctx, request, edge, parentRows, childRows, projectionRows, reduceOptions)
			edgeReduceElapsed := time.Since(edgeReduceStart)
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			if err != nil || result.Diagnostics.BlocksNative() {
				return result, err
			}
			result.Probes = append(result.Probes,
				legacyDirectRelationshipProbe(probePrefix+"input_edge", strconv.Itoa(candidate.InputOrdinal)),
				legacyDirectRelationshipProbe(probePrefix+"parent_role", edge.parentKey()),
				legacyDirectRelationshipProbe(probePrefix+"parent_table", edge.parentTable),
				legacyDirectRelationshipProbe(probePrefix+"parent_rows", strconv.Itoa(len(parentRows))),
				legacyDirectRelationshipProbe(probePrefix+"child_role", edge.childKey()),
				legacyDirectRelationshipProbe(probePrefix+"child_table", edge.childTable),
				legacyDirectRelationshipProbe(probePrefix+"child_rows", strconv.Itoa(len(childRows))),
				legacyDirectRelationshipProbe(probePrefix+"joined_rows", strconv.Itoa(len(joined))),
				legacyDirectRelationshipProbe(probePrefix+"reduce_elapsed", edgeReduceElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"domain_mapping_cache_hit", strconv.FormatBool(reduceTiming.domainMappingCacheHit)),
				legacyDirectRelationshipProbe(probePrefix+"domain_mapping_cache_mode", reduceTiming.domainMappingCacheMode),
				legacyDirectRelationshipProbe(probePrefix+"projection_rows", strconv.Itoa(reduceTiming.projectionRows)),
				legacyDirectRelationshipProbe(probePrefix+"projection_elapsed", reduceTiming.projectionElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"projection_cache_hit", strconv.FormatBool(reduceTiming.projectionCacheHit)),
				legacyDirectRelationshipProbe(probePrefix+"parent_key_rows", strconv.Itoa(reduceTiming.parentKeyRows)),
				legacyDirectRelationshipProbe(probePrefix+"parent_key_elapsed", reduceTiming.parentKeyElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"parent_key_materialization", strconv.FormatBool(reduceTiming.parentKeyMaterialization)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_used", strconv.FormatBool(reduceTiming.reverseArtifactUsed)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_skip_reason", reduceTiming.reverseArtifactSkipReason),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_mode", reduceTiming.reverseArtifactMode),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_cache_hit", strconv.FormatBool(reduceTiming.reverseArtifactCacheHit)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_source_values", strconv.Itoa(reduceTiming.reverseArtifactSourceValues)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_candidate_rows", strconv.Itoa(reduceTiming.reverseArtifactCandidateRows)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_narrowed_rows", strconv.Itoa(reduceTiming.reverseArtifactNarrowedRows)),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_elapsed", reduceTiming.reverseArtifactElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_lookup_elapsed", reduceTiming.reverseArtifactLookupElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_fanout_elapsed", reduceTiming.reverseArtifactFanoutElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_client_rpc_elapsed", reduceTiming.reverseArtifactClientRPCElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_client_rpc_max_elapsed", reduceTiming.reverseArtifactClientRPCMaxElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_response_merge_elapsed", reduceTiming.reverseArtifactResponseMergeElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_row_merge_elapsed", reduceTiming.reverseArtifactRowMergeElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_parent_merge_elapsed", reduceTiming.reverseArtifactParentMergeElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_sort_elapsed", reduceTiming.reverseArtifactSortElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_local_mode", reduceTiming.reverseArtifactLocalMode),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_target_candidate_mode", reduceTiming.reverseArtifactTargetCandidateMode),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_source_elapsed", reduceTiming.reverseArtifactSourceElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_read_request_elapsed", reduceTiming.reverseArtifactReadElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_row_conversion_elapsed", reduceTiming.reverseArtifactRowConversionElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_map_conversion_elapsed", reduceTiming.reverseArtifactMapConversionElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_narrow_elapsed", reduceTiming.reverseArtifactNarrowElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_parent_map_elapsed", reduceTiming.reverseArtifactParentElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_projection_intersect_elapsed", reduceTiming.reverseArtifactProjectElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_projection_intersect_mode", reduceTiming.reverseArtifactProjectMode),
				legacyDirectRelationshipProbe(probePrefix+"reverse_artifact_domain_cache_store_elapsed", reduceTiming.reverseArtifactCacheSetElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"matched_rows", strconv.Itoa(reduceTiming.matchedRows)),
				legacyDirectRelationshipProbe(probePrefix+"batch_equal_elapsed", reduceTiming.batchEqualElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"single_key_foundset_elapsed", reduceTiming.singleKeyFoundSetElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"single_key_equal_elapsed", reduceTiming.singleKeyEqualElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"value_vector_elapsed", reduceTiming.valueVectorElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"intersect_elapsed", reduceTiming.intersectElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"rownum_elapsed", reduceTiming.rownumElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"pair_elapsed", reduceTiming.pairElapsed.String()),
			)
			result.Probes = append(result.Probes, legacyDirectRelationshipProjectionPolicyProbes(probePrefix, projectionPolicy)...)
			childRetainMode := reduceTiming.childRetainMode
			childRetainStart := time.Now()
			nextChildRows := joined
			if !reduceTiming.childRetainCovered {
				childRetainMode = "intersect"
				nextChildRows = legacyDirectRelationshipIntersectRownums(childRows, joined)
			}
			scratchpad.storeAlignedParentRows(edge, nextChildRows, pairs)
			childRetainElapsed := time.Since(childRetainStart)
			reductionSummary.record(candidate.InputOrdinal, iterations, edgeIndex+1, edge, len(parentRows), len(childRows), len(joined), edgeReduceElapsed, reduceTiming, childRetainElapsed, len(nextChildRows))
			result.Probes = append(result.Probes,
				legacyDirectRelationshipProbe(probePrefix+"child_retain_elapsed", childRetainElapsed.String()),
				legacyDirectRelationshipProbe(probePrefix+"child_retain_rows", strconv.Itoa(len(nextChildRows))),
				legacyDirectRelationshipProbe(probePrefix+"child_retain_mode", childRetainMode),
				legacyDirectRelationshipProbe(probePrefix+"child_retain_covered", strconv.FormatBool(reduceTiming.childRetainCovered)),
			)
			if len(nextChildRows) != len(childRows) {
				changed = true
				rowsByTable[edge.childKey()] = nextChildRows
				fullDomainInitialRowsByRole[edge.childKey()] = false
				if len(equalitySeedFields) > 0 {
					equalitySeedProbes, equalitySeedChanged, diagnostics, err := e.legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithPrefix(ctx, request, equalitySeedFields, rowsByTable, fullDomainInitialRowsByRole, probePrefix+"post_")
					result.Probes = append(result.Probes, equalitySeedProbes...)
					result.Diagnostics = append(result.Diagnostics, diagnostics...)
					if err != nil || result.Diagnostics.BlocksNative() {
						return result, err
					}
					if equalitySeedChanged {
						changed = true
					}
				}
			}
		}
		singlePassAppliedThisIteration := changed && singlePassPolicy.Eligible
		result.Probes = append(result.Probes, legacyDirectRelationshipSinglePassPolicyProbes(fmt.Sprintf("graph_iter_%d_", iterations), singlePassPolicy, singlePassAppliedThisIteration)...)
		if !changed {
			break
		}
		if singlePassAppliedThisIteration {
			singlePassApplied = true
			break
		}
		if iterations > len(edges)+1 {
			return ExecutionResult{Diagnostics: legacyDirectRelationshipDiagnostic("relationship-vector graph reduction did not converge")}, nil
		}
	}
	reductionElapsed := time.Since(reductionStart)
	sinkRole, sink, diagnostics := legacyDirectRelationshipGraphSink(edges)
	if diagnostics.BlocksNative() {
		if len(request.SQLAggregates) > 0 {
			return e.legacyDirectRelationshipMultiSinkGraphAggregateResult(ctx, request, edges, rowsByTable, result)
		}
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	finalRows := rowsByTable[sinkRole]
	result.RowSet = qsbridge.QuantaProjectedRowSet{
		Index:   sink,
		Rownums: append([]qsbridge.QuantaRownum(nil), finalRows...),
	}
	result.Count = uint64(len(finalRows))
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_iterations", strconv.Itoa(iterations)),
		legacyDirectRelationshipProbe("graph_single_pass_applied", strconv.FormatBool(singlePassApplied)),
		legacyDirectRelationshipProbe("graph_single_pass_reason", singlePassReason),
		legacyDirectRelationshipProbe("phase_graph_reduction_elapsed", reductionElapsed.String()),
		legacyDirectRelationshipProbe("graph_sink_role", sinkRole),
		legacyDirectRelationshipProbe("graph_sink", sink),
		legacyDirectRelationshipProbe("graph_sink_rows", strconv.Itoa(len(finalRows))),
		legacyDirectRelationshipProbe("graph_reduced_roles", legacyDirectRelationshipAlignedRoleDebug(rowsByTable)),
	)
	result.Probes = append(result.Probes, reductionSummary.probes()...)
	result.Probes = append(result.Probes, legacyDirectRelationshipResidualPlacementPolicyProbes(prefilterProbes, rowsByTable)...)
	if len(request.SQLAggregates) == 0 {
		return e.legacyDirectRelationshipGraphProjectionResult(ctx, request, sink, finalRows, edges, scratchpad, result)
	}
	if len(request.GroupBy) > 0 {
		return e.legacyDirectRelationshipGraphGroupedAggregateResult(ctx, request, sink, finalRows, edges, scratchpad, result)
	}
	if !directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) || directBitmapHasResidualScanPredicates(request) {
		return e.legacyDirectRelationshipGraphAggregateResult(ctx, request, sink, finalRows, edges, scratchpad, result)
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return directBitmapCountAggregateResult(request, result), nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipMultiSinkGraphAggregateResult(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge, rowsByRole map[string][]qsbridge.QuantaRownum, result ExecutionResult) (ExecutionResult, error) {
	rootRole, rootTable, diagnostics := legacyDirectRelationshipGraphBestRoot(edges, rowsByRole)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rootRows := rowsByRole[rootRole]
	reduced, diagnostics, err := e.legacyDirectRelationshipReducedEdgesForRows(ctx, request, edges, rowsByRole)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipPostReductionMaterializationFields(request, fields)
	if len(fields) == 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("relationship-vector multi-sink graph aggregate requires materialized aggregate input fields")...)
		return result, nil
	}
	tupleRows, diagnostics := legacyDirectRelationshipTupleRowsFromReducedGraph(rootRole, rootRows, reduced)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	materializationStart := time.Now()
	values, materializationProbes, diagnostics, err := e.legacyDirectRelationshipSiblingRootMaterializedValues(ctx, request, tupleRows, fields)
	materializationElapsed := time.Since(materializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	result.Probes = append(result.Probes, materializationProbes...)
	projected, diagnostics := tupleRows.ToProjectedRowSet(rootTable, fields, values)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	filtered, filteredTupleRows, diagnostics := FilterRelationshipTupleProjectedResiduals(tupleRows, request, projected)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_shape", "multi_sink_tuple"),
		legacyDirectRelationshipProbe("graph_root_role", rootRole),
		legacyDirectRelationshipProbe("graph_root", rootTable),
		legacyDirectRelationshipProbe("graph_multi_sink_materialization_rows", strconv.Itoa(tupleRows.CandidateCount())),
		legacyDirectRelationshipProbe("graph_multi_sink_materialization_fields", strconv.Itoa(len(fields))),
		legacyDirectRelationshipProbe("graph_multi_sink_materialization_field_list", legacyDirectRelationshipProjectionFieldsDebug(fields)),
		legacyDirectRelationshipProbe("phase_graph_multi_sink_materialization_elapsed", materializationElapsed.String()),
	)
	result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded:           tupleRows,
		Filtered:           filteredTupleRows,
		MaterializedFields: fields,
		AggregateAlias:     relationshipTupleAggregateAlias(request),
	})...)
	if len(request.GroupBy) > 0 {
		return directBitmapMaterializedGroupedAggregateResult(request, filtered, result), nil
	}
	return directBitmapMaterializedAggregateResult(request, filtered, result), nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReducedEdgesForRows(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge, rowsByRole map[string][]qsbridge.QuantaRownum) ([]legacyDirectRelationshipSiblingRootReducedEdge, qsbridge.DiagnosticSet, error) {
	reduced := make([]legacyDirectRelationshipSiblingRootReducedEdge, 0, len(edges))
	for _, edge := range edges {
		parentRows := rowsByRole[edge.parentKey()]
		childRows := rowsByRole[edge.childKey()]
		joined, pairs, diagnostics, err := e.legacyDirectRelationshipReduce(ctx, request, edge, parentRows, childRows)
		if err != nil || diagnostics.BlocksNative() {
			return nil, diagnostics, err
		}
		reduced = append(reduced, legacyDirectRelationshipSiblingRootReducedEdge{edge: edge, childRows: joined, pairs: pairs})
	}
	return reduced, nil, nil
}

func legacyDirectRelationshipGraphBestRoot(edges []legacyDirectRelationshipEdge, rowsByRole map[string][]qsbridge.QuantaRownum) (string, string, qsbridge.DiagnosticSet) {
	roleTables := make(map[string]string, len(edges)*2)
	for _, edge := range edges {
		roleTables[edge.parentKey()] = edge.parentTable
		roleTables[edge.childKey()] = edge.childTable
	}
	roles := make([]string, 0, len(roleTables))
	for role := range roleTables {
		roles = append(roles, role)
	}
	sort.Slice(roles, func(i, j int) bool {
		leftRows := len(rowsByRole[roles[i]])
		rightRows := len(rowsByRole[roles[j]])
		if leftRows == rightRows {
			return roles[i] < roles[j]
		}
		return leftRows < rightRows
	})
	for _, role := range roles {
		if len(rowsByRole[role]) == 0 {
			continue
		}
		return role, roleTables[role], nil
	}
	return "", "", legacyDirectRelationshipDiagnostic("relationship-vector graph execution requires a non-empty root role")
}

func legacyDirectRelationshipGraphRoot(edges []legacyDirectRelationshipEdge) (string, string, qsbridge.DiagnosticSet) {
	parentRoles := make(map[string]string, len(edges))
	childRoles := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		parentRoles[edge.parentKey()] = edge.parentTable
		childRoles[edge.childKey()] = struct{}{}
	}
	var rootRole string
	var rootTable string
	for role, table := range parentRoles {
		if _, ok := childRoles[role]; ok {
			continue
		}
		if rootRole != "" {
			return "", "", legacyDirectRelationshipDiagnostic("relationship-vector graph execution requires a single root table")
		}
		rootRole = role
		rootTable = table
	}
	if rootRole == "" {
		return "", "", legacyDirectRelationshipDiagnostic("relationship-vector graph execution requires a root table")
	}
	return rootRole, rootTable, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipSiblingRootGraphAggregateResult(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge, shape legacyDirectRelationshipSiblingRootGraph) (ExecutionResult, error) {
	if len(request.SQLAggregates) == 0 {
		return legacyDirectRelationshipSiblingRootBlockedResult(edges, shape), nil
	}
	rootRows, reduced, result, err := e.legacyDirectRelationshipSiblingRootReducedEdges(ctx, request, edges, shape)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipPostReductionFields(request, fields)
	if len(fields) == 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("relationship-vector sibling-root aggregate requires materialized aggregate input fields")...)
		return result, nil
	}
	tupleRows, diagnostics := legacyDirectRelationshipSiblingRootTupleRowsFromReducedEdges(shape, rootRows, reduced)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	materializationStart := time.Now()
	values, materializationProbes, diagnostics, err := e.legacyDirectRelationshipSiblingRootMaterializedValues(ctx, request, tupleRows, fields)
	materializationElapsed := time.Since(materializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	result.Probes = append(result.Probes, materializationProbes...)
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("sibling_root_materialization_rows", strconv.Itoa(tupleRows.CandidateCount())),
		legacyDirectRelationshipProbe("sibling_root_materialization_fields", strconv.Itoa(len(fields))),
		legacyDirectRelationshipProbe("sibling_root_materialization_field_list", legacyDirectRelationshipProjectionFieldsDebug(fields)),
		legacyDirectRelationshipProbe("phase_sibling_root_materialization_elapsed", materializationElapsed.String()),
	)
	aggregateResult := legacyDirectRelationshipSiblingRootProjectedAggregateResult(shape, rootRows, reduced, shape.rootTable, fields, values, request, request)
	aggregateResult.Diagnostics = append(result.Diagnostics, aggregateResult.Diagnostics...)
	aggregateResult.Probes = append(result.Probes, aggregateResult.Probes...)
	return aggregateResult, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipSiblingRootReducedEdges(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge, shape legacyDirectRelationshipSiblingRootGraph) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipSiblingRootReducedEdge, ExecutionResult, error) {
	result := ExecutionResult{Probes: []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_edges", strconv.Itoa(len(edges))),
		legacyDirectRelationshipProbe("graph_shape", "sibling_root"),
		legacyDirectRelationshipProbe("graph_sibling_root", legacyDirectRelationshipGraphSiblingRootDebug(shape)),
	}}
	rootField := ""
	if len(edges) > 0 {
		rootField = edges[0].parentField
	}
	rootStart := time.Now()
	rootRows, diagnostics, err := e.legacyDirectRelationshipRownumsForTable(ctx, request, shape.rootTable, shape.rootRole, rootField)
	rootElapsed := time.Since(rootStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return nil, nil, result, err
	}
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("sibling_root_rows", strconv.Itoa(len(rootRows))),
		legacyDirectRelationshipProbe("phase_sibling_root_rows_elapsed", rootElapsed.String()),
	)
	reduced := make([]legacyDirectRelationshipSiblingRootReducedEdge, 0, len(edges))
	for i, edge := range edges {
		childStart := time.Now()
		childRows, diagnostics, err := e.legacyDirectRelationshipRownumsForTable(ctx, request, edge.childTable, edge.childRole, edge.childField)
		childElapsed := time.Since(childStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return nil, nil, result, err
		}
		reduceStart := time.Now()
		joined, pairs, diagnostics, err := e.legacyDirectRelationshipReduce(ctx, request, edge, rootRows, childRows)
		reduceElapsed := time.Since(reduceStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return nil, nil, result, err
		}
		prefix := fmt.Sprintf("sibling_edge_%d_", i+1)
		result.Probes = append(result.Probes,
			legacyDirectRelationshipProbe(prefix+"child_table", edge.childTable),
			legacyDirectRelationshipProbe(prefix+"child_rows", strconv.Itoa(len(childRows))),
			legacyDirectRelationshipProbe(prefix+"joined_rows", strconv.Itoa(len(joined))),
			legacyDirectRelationshipProbe(prefix+"joined_pairs", strconv.Itoa(len(pairs))),
			legacyDirectRelationshipProbe(prefix+"phase_child_rows_elapsed", childElapsed.String()),
			legacyDirectRelationshipProbe(prefix+"phase_reduce_elapsed", reduceElapsed.String()),
		)
		reduced = append(reduced, legacyDirectRelationshipSiblingRootReducedEdge{edge: edge, childRows: joined, pairs: pairs})
	}
	return rootRows, reduced, result, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipSiblingRootMaterializedValues(ctx context.Context, request ExecutionRequest, tupleRows RelationshipTupleRowSet, fields []qsbridge.QuantaProjectionField) (RelationshipTupleValueStore, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	materialization := e.projectionMaterializationKernel()
	rowsByRole := legacyDirectRelationshipTupleRowsByRole(tupleRows)
	values := make(RelationshipTupleValueStore)
	var probes []ExecutionProbe
	var materializationDiagnostics qsbridge.DiagnosticSet
	for _, field := range fields {
		table := field.Index
		role := legacyDirectRelationshipProjectionFieldRoleKey(field, table)
		if field.Role == "" {
			field.Role = qsbridge.TableInstanceID(role)
		}
		if field.Index == "" {
			field.Index = table
		}
		rownums, ok := rowsByRole[string(field.Role)]
		if !ok {
			return nil, probes, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector sibling-root cannot align materialization field %s.%s role %s; tuple roles=%s", field.Index, field.Field, field.Role, legacyDirectRelationshipTupleRoleDebug(rowsByRole))), nil
		}
		materialized, materializedProbes, diagnostics, err := e.legacyDirectRelationshipMaterializedValuesWithProbes(ctx, materialization, field.Index, rownums, []qsbridge.QuantaProjectionField{field}, e.legacyDirectRelationshipTimeMaterializationForRole(request, field.Index, string(field.Role)))
		probes = append(probes, materializedProbes...)
		materializationDiagnostics = append(materializationDiagnostics, diagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return nil, probes, diagnostics, err
		}
		fieldValues := materialized[legacyDirectRelationshipProjectionFieldKey(field)]
		if fieldValues == nil {
			return nil, probes, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector sibling-root materialization missing field %s.%s", field.Index, field.Field)), nil
		}
		values[RelationshipTupleValueKeyForField(field)] = fieldValues
	}
	return values, probes, materializationDiagnostics, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphAggregateResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, scratchpad legacyDirectRelationshipGraphScratchpad, result ExecutionResult) (ExecutionResult, error) {
	if len(request.GroupBy) > 0 {
		return e.legacyDirectRelationshipGraphGroupedAggregateResult(ctx, request, sink, rownums, edges, scratchpad, result)
	}
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipPostReductionFields(request, fields)
	if len(fields) == 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("relationship-vector graph aggregate requires materialized aggregate input fields")...)
		return result, nil
	}
	alignmentStart := time.Now()
	alignedRows, alignmentProbes, diagnostics, err := e.legacyDirectRelationshipGraphAlignedRownums(ctx, request, sink, rownums, edges, scratchpad)
	alignmentElapsed := time.Since(alignmentStart)
	result.Probes = append(result.Probes, alignmentProbes...)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	materializationStart := time.Now()
	materialized, materializationProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, rownums, fields, alignedRows, edges, "graph_aggregate_materialization_")
	materializationElapsed := time.Since(materializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	tupleExpansionStart := time.Now()
	tupleRows, diagnostics := NewRelationshipTupleRowSetFromAlignedRownums(alignedRows)
	tupleExpansionElapsed := time.Since(tupleExpansionStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	residuals := directBitmapResidualScanPredicates(request)
	residualStart := time.Now()
	filtered, filteredTupleRows, diagnostics := FilterRelationshipTupleProjectedResiduals(tupleRows, request, materialized)
	residualElapsed := time.Since(residualStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rowsBeforeResidual := materialized.CandidateCount()
	rowsAfterResidual := filtered.CandidateCount()
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_aggregate_aligned_roles", legacyDirectRelationshipAlignedRoleDebug(alignedRows)),
		legacyDirectRelationshipProbe("phase_graph_aggregate_alignment_elapsed", alignmentElapsed.String()),
		legacyDirectRelationshipProbe("graph_aggregate_materialization_rows", strconv.Itoa(rowsBeforeResidual)),
		legacyDirectRelationshipProbe("graph_aggregate_materialization_fields", strconv.Itoa(len(fields))),
		legacyDirectRelationshipProbe("graph_aggregate_materialization_field_list", legacyDirectRelationshipProjectionFieldsDebug(fields)),
		legacyDirectRelationshipProbe("graph_aggregate_residual_predicates", strconv.Itoa(len(residuals))),
		legacyDirectRelationshipProbe("graph_aggregate_residual_rows_before", strconv.Itoa(rowsBeforeResidual)),
		legacyDirectRelationshipProbe("graph_aggregate_residual_rows_after", strconv.Itoa(rowsAfterResidual)),
		legacyDirectRelationshipProbe("graph_aggregate_residual_rows_removed", strconv.Itoa(rowsBeforeResidual-rowsAfterResidual)),
		legacyDirectRelationshipProbe("phase_graph_aggregate_materialization_elapsed", materializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_aggregate_tuple_expansion_elapsed", tupleExpansionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_aggregate_residual_filter_elapsed", residualElapsed.String()),
	)
	result.Probes = append(result.Probes, materializationProbes...)
	result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded:           tupleRows,
		Filtered:           filteredTupleRows,
		MaterializedFields: fields,
		AggregateAlias:     relationshipTupleAggregateAlias(request),
	})...)
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return directBitmapMaterializedAggregateResult(request, filtered, result), nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphGroupedAggregateResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, scratchpad legacyDirectRelationshipGraphScratchpad, result ExecutionResult) (ExecutionResult, error) {
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	graphReductionElapsed := legacyDirectRelationshipProbeDuration(result.Probes, "phase_graph_reduction_elapsed")
	residuals := directBitmapResidualScanPredicates(request)
	tupleWorkNeeded := legacyDirectRelationshipGraphGroupedAggregateNeedsTupleRows(request, residuals)
	var alignmentRequiredRoles map[string]struct{}
	if !tupleWorkNeeded {
		alignmentFields := legacyDirectRelationshipPostReductionMaterializationFields(request, fields)
		alignmentRequiredRoles = legacyDirectRelationshipRequiredAlignmentRoles(request, sink, alignmentFields)
	}
	alignmentStart := time.Now()
	alignedRows, alignmentProbes, diagnostics, err := e.legacyDirectRelationshipGraphAlignedRownumsForRoles(ctx, request, sink, rownums, edges, scratchpad, alignmentRequiredRoles)
	alignmentElapsed := time.Since(alignmentStart)
	result.Probes = append(result.Probes, alignmentProbes...)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	if lateResult, handled, err := e.legacyDirectRelationshipQ18LargeOrderProjectionResult(ctx, request, sink, rownums, edges, fields, alignedRows, alignmentElapsed, result); handled || err != nil {
		return lateResult, err
	}
	if preAggResult, handled, err := e.legacyDirectRelationshipQ3OrderRevenueResult(ctx, request, sink, rownums, edges, fields, alignedRows, graphReductionElapsed, alignmentElapsed, result); handled || err != nil {
		return preAggResult, err
	}
	var tupleRows RelationshipTupleRowSet
	var filteredTupleRows RelationshipTupleRowSet
	var tupleExpansionElapsed time.Duration
	var sameRowElapsed time.Duration
	var membershipElapsed time.Duration
	var residualElapsed time.Duration
	if tupleWorkNeeded {
		tupleExpansionStart := time.Now()
		tupleRows, diagnostics = NewRelationshipTupleRowSetFromAlignedRownums(alignedRows)
		tupleExpansionElapsed = time.Since(tupleExpansionStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, nil
		}
		sameRowStart := time.Now()
		var sameRowProbes []ExecutionProbe
		tupleRows, alignedRows, request, sameRowProbes, diagnostics, err = e.legacyDirectRelationshipApplyTupleSameRowResiduals(ctx, request, tupleRows, alignedRows)
		sameRowElapsed = time.Since(sameRowStart)
		result.Probes = append(result.Probes, sameRowProbes...)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return result, err
		}
		if rows, ok := alignedRows[sink]; ok {
			rownums = rows
		} else if role := legacyDirectRelationshipUniqueSourceRoleKey(request, sink); role != "" {
			if rows, ok := alignedRows[role]; ok {
				rownums = rows
			}
		}
		membershipStart := time.Now()
		var membershipProbes []ExecutionProbe
		tupleRows, alignedRows, membershipProbes, diagnostics, err = e.legacyDirectRelationshipApplyTupleMemberships(ctx, request, tupleRows, alignedRows, edges)
		membershipElapsed = time.Since(membershipStart)
		result.Probes = append(result.Probes, membershipProbes...)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return result, err
		}
		if rows, ok := alignedRows[sink]; ok {
			rownums = rows
		} else if role := legacyDirectRelationshipUniqueSourceRoleKey(request, sink); role != "" {
			if rows, ok := alignedRows[role]; ok {
				rownums = rows
			}
		}
	}
	if preAggResult, handled, err := e.legacyDirectRelationshipDiscountedRevenueResult(ctx, request, sink, rownums, edges, fields, alignedRows, tupleRows, graphReductionElapsed, alignmentElapsed, tupleExpansionElapsed, sameRowElapsed, membershipElapsed, residualElapsed, result); handled || err != nil {
		return preAggResult, err
	}
	residuals = directBitmapResidualScanPredicates(request)
	fields = legacyDirectRelationshipPostReductionMaterializationFields(request, fields)
	if len(fields) == 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("relationship-vector graph grouped aggregate requires materialized group or aggregate fields")...)
		return result, nil
	}
	materializationStart := time.Now()
	materialized, materializationProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, rownums, fields, alignedRows, edges, "graph_grouped_aggregate_materialization_")
	materializationElapsed := time.Since(materializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	filtered := materialized
	if tupleWorkNeeded {
		residualStart := time.Now()
		filtered, filteredTupleRows, diagnostics = FilterRelationshipTupleProjectedResiduals(tupleRows, request, materialized)
		residualElapsed = time.Since(residualStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, nil
		}
	}
	rowsBeforeResidual := materialized.CandidateCount()
	rowsAfterResidual := filtered.CandidateCount()
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_grouped_aggregate_aligned_roles", legacyDirectRelationshipAlignedRoleDebug(alignedRows)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_alignment_elapsed", alignmentElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_materialization_rows", strconv.Itoa(rowsBeforeResidual)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_materialization_fields", strconv.Itoa(len(fields))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_materialization_field_list", legacyDirectRelationshipProjectionFieldsDebug(fields)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_residual_predicates", strconv.Itoa(len(residuals))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_residual_rows_before", strconv.Itoa(rowsBeforeResidual)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_residual_rows_after", strconv.Itoa(rowsAfterResidual)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_residual_rows_removed", strconv.Itoa(rowsBeforeResidual-rowsAfterResidual)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_rows", strconv.Itoa(filtered.CandidateCount())),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_fields", strconv.Itoa(len(fields))),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_materialization_elapsed", materializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_tuple_expansion_elapsed", tupleExpansionElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_tuple_expansion_skipped", strconv.FormatBool(!tupleWorkNeeded)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_same_row_elapsed", sameRowElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_membership_elapsed", membershipElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_residual_filter_elapsed", residualElapsed.String()),
	)
	result.Probes = append(result.Probes, materializationProbes...)
	if tupleWorkNeeded {
		result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
			Expanded:           tupleRows,
			Filtered:           filteredTupleRows,
			MaterializedFields: fields,
			AggregateAlias:     relationshipTupleAggregateAlias(request),
		})...)
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	aggregateStart := time.Now()
	aggregateResult := directBitmapMaterializedGroupedAggregateResult(request, filtered, result)
	aggregateElapsed := time.Since(aggregateStart)
	aggregateResult.Probes = append(aggregateResult.Probes,
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_execution_elapsed", aggregateElapsed.String()),
	)
	return aggregateResult, nil
}

// legacyDirectRelationshipApplyTupleMemberships applies SQL membership filters
// to a reduced graph tuple stream while preserving role-rownum alignment.
func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyTupleMemberships(ctx context.Context, request ExecutionRequest, tupleRows RelationshipTupleRowSet, alignedRows map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge) (RelationshipTupleRowSet, map[string][]qsbridge.QuantaRownum, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if len(request.Memberships) == 0 {
		return tupleRows, alignedRows, nil, nil, nil
	}
	materialization := e.projectionMaterializationKernel()
	if materialization == nil {
		return RelationshipTupleRowSet{}, nil, nil, legacyDirectRelationshipDiagnostic("relationship-vector graph membership requires a projection materialization kernel"), nil
	}
	sessions := e.Sessions
	if sessions == nil && e.Source != nil {
		sessions = LegacyQuantaSourceSessionProvider{Source: e.Source}
	}
	if sessions == nil {
		return RelationshipTupleRowSet{}, nil, nil, legacyDirectRelationshipDiagnostic("relationship-vector graph membership requires a direct session provider"), nil
	}
	baseRuntime := DirectBitmapRuntime{
		Sessions:          sessions,
		Materialization:   materialization,
		SameRowComparison: e.sameRowComparisonKernel(),
	}
	currentTuples := tupleRows
	currentAligned := legacyDirectRelationshipCloneAlignedRows(alignedRows)
	probes := make([]ExecutionProbe, 0, len(request.Memberships)*12)
	for index, membership := range request.Memberships {
		prefix := fmt.Sprintf("graph_membership_%d_", index+1)
		role, diagnostics := legacyDirectRelationshipMembershipTupleRole(membership.Left.Table, currentAligned)
		if diagnostics.BlocksNative() {
			return RelationshipTupleRowSet{}, nil, probes, diagnostics, nil
		}
		leftCandidates := legacyDirectRelationshipTupleUniqueRownums(currentTuples, role)
		derivation := legacyDirectRelationshipObserveTupleMembershipCandidateDerivation(membership, role, currentTuples, currentAligned, edges)
		probes = append(probes, legacyDirectRelationshipTupleMembershipCandidateDerivationProbes(prefix, derivation)...)
		runtime := baseRuntime
		if rightSeed, seedProbes, ok := e.legacyDirectRelationshipTupleMembershipDerivedRightCandidateSeed(ctx, request, membership, currentTuples, prefix, derivation); ok {
			runtime.CorrelatedSiblingRightCandidateSeed = &rightSeed
			runtime.CorrelatedSiblingRightCandidateSeedMode = "graph_parent_vector_expansion"
			probes = append(probes, seedProbes...)
		} else {
			probes = append(probes, seedProbes...)
		}
		if e.ProjectionBSIReader != nil && legacyDirectRelationshipTupleMembershipShouldUseBSIFastPath(leftCandidates, membership) {
			runtime.ProjectionBSIReader = e.ProjectionBSIReader
		}
		filtered, membershipProbes, diagnostics, err := runtime.directBitmapApplyMembership(ctx, request, BitmapQueryResult{
			Success: true,
			Count:   uint64(len(leftCandidates)),
			Rownums: leftCandidates,
		}, membership, BitmapQueryResult{})
		probes = append(probes, membershipProbes...)
		if err != nil || diagnostics.BlocksNative() {
			return RelationshipTupleRowSet{}, nil, probes, diagnostics, err
		}
		allowed := make(map[qsbridge.QuantaRownum]struct{}, len(filtered.Rownums))
		for _, rownum := range filtered.Rownums {
			allowed[rownum] = struct{}{}
		}
		keep := make([]int, 0, currentTuples.CandidateCount())
		for rowIndex, row := range currentTuples.Rows {
			rownum, ok := row.Rownum(qsbridge.TableInstanceID(role))
			if !ok {
				return RelationshipTupleRowSet{}, nil, probes, legacyDirectRelationshipDiagnostic("relationship-vector graph membership tuple row missing role " + role), nil
			}
			if _, ok := allowed[rownum]; ok {
				keep = append(keep, rowIndex)
			}
		}
		beforeRows := currentTuples.CandidateCount()
		currentTuples = currentTuples.FilterByIndexes(keep)
		currentAligned = legacyDirectRelationshipFilterAlignedRowsByTupleIndexes(currentAligned, keep)
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"kind", string(membership.Kind)),
			legacyDirectRelationshipProbe(prefix+"left_role", role),
			legacyDirectRelationshipProbe(prefix+"left_candidates", strconv.Itoa(len(leftCandidates))),
			legacyDirectRelationshipProbe(prefix+"left_rows_after", strconv.Itoa(len(filtered.Rownums))),
			legacyDirectRelationshipProbe(prefix+"tuple_rows_before", strconv.Itoa(beforeRows)),
			legacyDirectRelationshipProbe(prefix+"tuple_rows_after", strconv.Itoa(currentTuples.CandidateCount())),
		)
	}
	return currentTuples, currentAligned, probes, nil, nil
}

func legacyDirectRelationshipTupleMembershipShouldUseBSIFastPath(leftCandidates []qsbridge.QuantaRownum, membership qsbridge.MembershipEdge) bool {
	rightOnlyPredicates, correlatedPredicates := directBitmapSplitMembershipPredicates(membership)
	if len(correlatedPredicates) == 0 {
		return false
	}
	leftFields := directBitmapCorrelatedMembershipProjectionFields(membership.Left, correlatedPredicates, membership.Left.Table)
	rightFields := directBitmapCorrelatedMembershipProjectionFields(membership.Right, correlatedPredicates, membership.Right.Table)
	return directBitmapCorrelatedMembershipShouldReuseRightBSIVectors(leftCandidates, rightOnlyPredicates, leftFields, rightFields)
}

type legacyDirectRelationshipTupleMembershipCandidateDerivationObservation struct {
	available        bool
	reason           string
	mode             string
	edge             legacyDirectRelationshipEdge
	parentRole       string
	leftRole         string
	rightRole        string
	edgeDetail       string
	parentRows       int
	uniqueParentRows int
}

func legacyDirectRelationshipObserveTupleMembershipCandidateDerivation(membership qsbridge.MembershipEdge, leftRole string, tupleRows RelationshipTupleRowSet, alignedRows map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge) legacyDirectRelationshipTupleMembershipCandidateDerivationObservation {
	observation := legacyDirectRelationshipTupleMembershipCandidateDerivationObservation{
		reason:    "no_matching_parent_edge",
		leftRole:  leftRole,
		rightRole: legacyDirectRelationshipTableRoleKey(membership.Right.Table),
	}
	if len(edges) == 0 {
		observation.reason = "no_graph_edges"
		return observation
	}
	if !strings.EqualFold(membership.Left.Table.Table, membership.Right.Table.Table) || directBitmapSameTableInstance(membership.Left.Table, membership.Right.Table) {
		observation.reason = "not_repeated_table_sibling_membership"
		return observation
	}
	leftField := directBitmapFieldPhysicalName(membership.Left)
	rightField := directBitmapFieldPhysicalName(membership.Right)
	for _, edge := range edges {
		if !legacyDirectRelationshipEdgeChildMatchesRole(edge, leftRole) ||
			!strings.EqualFold(edge.childTable, membership.Left.Table.Table) ||
			!strings.EqualFold(edge.childField, leftField) ||
			!strings.EqualFold(edge.childField, rightField) {
			continue
		}
		if !legacyDirectRelationshipMembershipHasSiblingFKEquality(membership, edge) {
			observation.reason = "missing_sibling_fk_equality"
			continue
		}
		parentRole := edge.parentKey()
		parentRows, ok := alignedRows[parentRole]
		if !ok {
			observation.reason = "parent_role_not_aligned"
			continue
		}
		observation.available = true
		observation.reason = ""
		observation.mode = "parent_role_vector_expansion"
		observation.edge = edge
		observation.parentRole = parentRole
		observation.edgeDetail = fmt.Sprintf("%s.%s->%s.%s", edge.parentTable, edge.parentField, edge.childTable, edge.childField)
		observation.parentRows = len(parentRows)
		observation.uniqueParentRows = len(legacyDirectRelationshipTupleUniqueRownums(tupleRows, parentRole))
		return observation
	}
	return observation
}

func legacyDirectRelationshipTupleMembershipCandidateDerivationProbes(prefix string, observation legacyDirectRelationshipTupleMembershipCandidateDerivationObservation) []ExecutionProbe {
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_available", strconv.FormatBool(observation.available)),
	}
	if !observation.available {
		if observation.reason != "" {
			probes = append(probes, legacyDirectRelationshipProbe(prefix+"candidate_derivation_reason", observation.reason))
		}
		return probes
	}
	probes = append(probes,
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_mode", observation.mode),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_parent_role", observation.parentRole),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_left_role", observation.leftRole),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_right_role", observation.rightRole),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_edge", observation.edgeDetail),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_parent_rows", strconv.Itoa(observation.parentRows)),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_unique_parent_rows", strconv.Itoa(observation.uniqueParentRows)),
	)
	return probes
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipTupleMembershipDerivedRightCandidateSeed(ctx context.Context, request ExecutionRequest, membership qsbridge.MembershipEdge, tupleRows RelationshipTupleRowSet, prefix string, observation legacyDirectRelationshipTupleMembershipCandidateDerivationObservation) (BitmapQueryResult, []ExecutionProbe, bool) {
	if !observation.available {
		return BitmapQueryResult{}, nil, false
	}
	rightOnlyPredicates, _ := directBitmapSplitMembershipPredicates(membership)
	if !directBitmapMembershipRightOnlyPredicatesCanApplyAfterSeed(rightOnlyPredicates) {
		return BitmapQueryResult{}, []ExecutionProbe{
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_applied", "false"),
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_apply_reason", "right_only_predicates_need_pushdown"),
		}, false
	}
	if e.Source == nil && e.RelationshipProjectionReader == nil {
		return BitmapQueryResult{}, []ExecutionProbe{
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_applied", "false"),
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_apply_reason", "projection_reader_unavailable"),
		}, false
	}
	parentRows := legacyDirectRelationshipTupleUniqueRownums(tupleRows, observation.parentRole)
	if len(parentRows) == 0 {
		return BitmapQueryResult{}, []ExecutionProbe{
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_applied", "false"),
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_apply_reason", "empty_parent_domain"),
		}, false
	}
	projectionCache := e.relationshipVectorProjectionCache(ctx)
	if projectionCache == nil {
		projectionCache = NewLegacyDirectRelationshipVectorProjectionCache()
	}
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		Source:                             e.Source,
		Sessions:                           e.Sessions,
		TableCache:                         e.TableCache,
		ProjectionReader:                   e.RelationshipProjectionReader,
		SourceKeyReader:                    e.RelationshipSourceKeyReader,
		ProjectionCache:                    projectionCache,
		PreferDirectParentToChildCandidate: true,
		ReverseArtifacts:                   e.ReverseArtifacts,
		ReverseArtifactCandidateReader:     e.ReverseArtifactCandidateReader,
	}
	read := legacyDirectRelationshipTupleMembershipParentToChildReadRequest(observation.edge, parentRows)
	start := time.Now()
	vectorResult, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, read)
	elapsed := time.Since(start)
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_elapsed", elapsed.String()),
	}
	if err != nil || diagnostics.BlocksNative() {
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_applied", "false"),
			legacyDirectRelationshipProbe(prefix+"candidate_derivation_apply_reason", "candidate_read_failed"),
		)
		return BitmapQueryResult{}, probes, false
	}
	rows := append([]qsbridge.QuantaRownum(nil), vectorResult.TargetCandidates.Rownums...)
	probes = append(probes,
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_applied", "true"),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_rows", strconv.Itoa(len(rows))),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_projection_elapsed", vectorResult.ProjectionElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_projection_cache_hit", strconv.FormatBool(vectorResult.ProjectionCacheHit)),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_source_key_projection_used", strconv.FormatBool(vectorResult.SourceKeyProjectionUsed)),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_source_key_projection_reason", vectorResult.SourceKeyProjectionReason),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_source_value_count", strconv.Itoa(vectorResult.SourceValueCount)),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_cache_hit", strconv.FormatBool(vectorResult.CandidateCacheHit)),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_cache_mode", vectorResult.CandidateCacheMode),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_mode", vectorResult.CandidateMode),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_elapsed", vectorResult.CandidateElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_batch_equal_elapsed", vectorResult.BatchEqualElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_scan_elapsed", vectorResult.CandidateScanElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_fanout_elapsed", vectorResult.CandidateFanoutElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_client_rpc_elapsed", vectorResult.CandidateClientRPCElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_client_rpc_max_elapsed", vectorResult.CandidateClientRPCMaxElapsed.String()),
		legacyDirectRelationshipProbe(prefix+"candidate_derivation_candidate_response_merge_elapsed", vectorResult.CandidateResponseMergeElapsed.String()),
	)
	return BitmapQueryResult{
		Success: true,
		Count:   uint64(len(rows)),
		Rownums: rows,
	}, probes, true
}

func legacyDirectRelationshipTupleMembershipParentToChildReadRequest(edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum) LegacyDirectRelationshipVectorReadRequest {
	child := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{
			Table: edge.childTable,
			Alias: edge.childRole,
		},
		Name:         edge.childField,
		PhysicalName: edge.childField,
	}
	parent := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{
			Table: edge.parentTable,
			Alias: edge.parentRole,
		},
		Name:         edge.parentField,
		PhysicalName: edge.parentField,
	}
	return LegacyDirectRelationshipVectorReadRequest{
		SourceFragment: qsbridge.QuantaQueryFragment{
			Index: edge.parentTable,
			Field: edge.parentField,
		},
		SourceCandidates: qsbridge.QuantaCandidateSet{
			Index:   edge.parentTable,
			Rownums: append([]qsbridge.QuantaRownum(nil), parentRows...),
		},
		SourceDomain: edge.parentTable,
		TargetDomain: edge.childTable,
		Edge: qsbridge.RelationshipJoinPlanEdge{
			Left:            child,
			LeftRole:        qsbridge.TableInstanceID(edge.childRole),
			Right:           parent,
			RightRole:       qsbridge.TableInstanceID(edge.parentRole),
			SQLKind:         edge.sqlKind,
			Capabilities:    appendRelationshipCapabilityOnce(edge.capabilities, qsbridge.RelationshipCapabilityChildExpansion),
			ProjectionScope: edge.projectionScope,
		},
		Direction:              qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
		Strategy:               qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		VectorIndex:            edge.childTable,
		VectorField:            edge.childField,
		AllowCandidateSuperset: true,
	}
}

func legacyDirectRelationshipEdgeChildMatchesRole(edge legacyDirectRelationshipEdge, role string) bool {
	return strings.EqualFold(edge.childKey(), role) || strings.EqualFold(edge.childRole, role) || strings.EqualFold(edge.childTable, role)
}

func legacyDirectRelationshipMembershipHasSiblingFKEquality(membership qsbridge.MembershipEdge, edge legacyDirectRelationshipEdge) bool {
	if strings.EqualFold(directBitmapFieldPhysicalName(membership.Left), edge.childField) &&
		strings.EqualFold(directBitmapFieldPhysicalName(membership.Right), edge.childField) {
		return true
	}
	for _, predicate := range membership.Predicates {
		binary, ok := directBitmapBinaryExpr(predicate.Expr)
		if !ok || binary.Op != qsbridge.BinaryOpEqual {
			continue
		}
		left, leftOK := directBitmapExprField(binary.Left)
		right, rightOK := directBitmapExprField(binary.Right)
		if !leftOK || !rightOK {
			continue
		}
		if legacyDirectRelationshipMembershipFieldMatches(left, membership.Left) &&
			legacyDirectRelationshipMembershipFieldMatches(right, membership.Right) &&
			strings.EqualFold(directBitmapFieldPhysicalName(left), edge.childField) &&
			strings.EqualFold(directBitmapFieldPhysicalName(right), edge.childField) {
			return true
		}
		if legacyDirectRelationshipMembershipFieldMatches(left, membership.Right) &&
			legacyDirectRelationshipMembershipFieldMatches(right, membership.Left) &&
			strings.EqualFold(directBitmapFieldPhysicalName(left), edge.childField) &&
			strings.EqualFold(directBitmapFieldPhysicalName(right), edge.childField) {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipMembershipFieldMatches(candidate qsbridge.FieldRef, target qsbridge.FieldRef) bool {
	return legacyDirectRelationshipTableInstanceMatches(candidate.Table, target.Table) &&
		strings.EqualFold(directBitmapFieldPhysicalName(candidate), directBitmapFieldPhysicalName(target))
}

func legacyDirectRelationshipTableInstanceMatches(candidate qsbridge.TableInstance, target qsbridge.TableInstance) bool {
	if candidate.ID != "" && target.ID != "" {
		return candidate.ID == target.ID
	}
	return strings.EqualFold(candidate.Table, target.Table) &&
		strings.EqualFold(candidate.RefName(), target.RefName())
}

// legacyDirectRelationshipMembershipTupleRole resolves a membership field's
// table instance to the graph role key carried in aligned tuple rows.
func legacyDirectRelationshipMembershipTupleRole(table qsbridge.TableInstance, alignedRows map[string][]qsbridge.QuantaRownum) (string, qsbridge.DiagnosticSet) {
	candidates := []string{
		legacyDirectRelationshipTableRoleKey(table),
		strings.ToLower(table.Alias),
		strings.ToLower(string(table.ID)),
		strings.ToLower(table.Table),
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, ok := alignedRows[candidate]; ok {
			return candidate, nil
		}
	}
	return "", legacyDirectRelationshipDiagnostic("relationship-vector graph membership left role is not part of the reduced tuple graph")
}

// legacyDirectRelationshipTupleUniqueRownums returns distinct rownums for one
// role in tuple order so membership filtering avoids duplicate materialization.
func legacyDirectRelationshipTupleUniqueRownums(tupleRows RelationshipTupleRowSet, role string) []qsbridge.QuantaRownum {
	seen := make(map[qsbridge.QuantaRownum]struct{})
	rownums := make([]qsbridge.QuantaRownum, 0, tupleRows.CandidateCount())
	roleID := qsbridge.TableInstanceID(role)
	for _, row := range tupleRows.Rows {
		rownum, ok := row.Rownum(roleID)
		if !ok {
			continue
		}
		if _, ok := seen[rownum]; ok {
			continue
		}
		seen[rownum] = struct{}{}
		rownums = append(rownums, rownum)
	}
	return rownums
}

// legacyDirectRelationshipCloneAlignedRows copies aligned role rownum vectors
// before a membership filter mutates the graph's working rowset.
func legacyDirectRelationshipCloneAlignedRows(alignedRows map[string][]qsbridge.QuantaRownum) map[string][]qsbridge.QuantaRownum {
	cloned := make(map[string][]qsbridge.QuantaRownum, len(alignedRows))
	for role, rows := range alignedRows {
		cloned[role] = append([]qsbridge.QuantaRownum(nil), rows...)
	}
	return cloned
}

// legacyDirectRelationshipFilterAlignedRowsByTupleIndexes applies tuple keep
// indexes to every aligned role vector, preserving positional correspondence.
func legacyDirectRelationshipFilterAlignedRowsByTupleIndexes(alignedRows map[string][]qsbridge.QuantaRownum, keep []int) map[string][]qsbridge.QuantaRownum {
	filtered := make(map[string][]qsbridge.QuantaRownum, len(alignedRows))
	for role, rows := range alignedRows {
		next := make([]qsbridge.QuantaRownum, 0, len(keep))
		for _, index := range keep {
			if index >= 0 && index < len(rows) {
				next = append(next, rows[index])
			}
		}
		filtered[role] = next
	}
	return filtered
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphProjectionResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, scratchpad legacyDirectRelationshipGraphScratchpad, result ExecutionResult) (ExecutionResult, error) {
	projectionFields, diagnostics := legacyDirectRelationshipVisibleProjectionFields(request)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	if len(projectionFields) == 0 {
		result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
		return result, nil
	}
	limited := append([]qsbridge.QuantaRownum(nil), rownums...)
	limitPushed := false
	if len(request.OrderBy) == 0 && request.Result.Limit > 0 {
		start := request.Result.Offset
		if start < 0 {
			start = 0
		}
		if start > len(limited) {
			start = len(limited)
		}
		end := len(limited)
		if start+request.Result.Limit < end {
			end = start + request.Result.Limit
		}
		limited = limited[start:end]
		limitPushed = true
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("graph_projection_limit_pushed", strconv.FormatBool(limitPushed)))
	alignedRows, alignmentProbes, diagnostics, err := e.legacyDirectRelationshipGraphAlignedRownums(ctx, request, sink, limited, edges, scratchpad)
	result.Probes = append(result.Probes, alignmentProbes...)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	materializationFields := legacyDirectRelationshipGraphProjectionMaterializationFields(request, projectionFields)
	rowSet, materializationProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, limited, materializationFields, alignedRows, edges, "graph_projection_materialization_")
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	result.Probes = append(result.Probes, materializationProbes...)
	tupleRows, diagnostics := NewRelationshipTupleRowSetFromAlignedRownums(alignedRows)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rowSet, _, diagnostics = FilterRelationshipTupleProjectedResiduals(tupleRows, request, rowSet)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	if !limitPushed {
		rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit)
	}
	rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, request.ProjectionOrder)
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return result, nil
}

func legacyDirectRelationshipChainShapeDiagnostics(request ExecutionRequest, vector RelationshipVectorJoinRequest) qsbridge.DiagnosticSet {
	if vector.EdgeCount() < 2 {
		return nil
	}
	if len(request.GroupBy) > 0 {
		return legacyDirectRelationshipDiagnostic("chained relationship-vector execution does not support GROUP BY in this slice")
	}
	if len(request.Memberships) > 0 {
		return legacyDirectRelationshipDiagnostic("chained relationship-vector execution does not support membership filters in this slice")
	}
	if len(request.SQLAggregates) == 0 || !directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) {
		return legacyDirectRelationshipDiagnostic("chained relationship-vector execution only supports count(*) in this slice")
	}
	for _, edge := range vector.Edges {
		if edge.SQLKind != qsbridge.JoinKindInner || edge.ExecutionKind != qsbridge.RelationshipJoinExecutionVector {
			return legacyDirectRelationshipDiagnostic("chained relationship-vector execution only supports inner relationship-vector joins in this slice")
		}
	}
	return nil
}

func legacyDirectRelationshipGraphShapeDiagnostics(request ExecutionRequest, vector RelationshipVectorJoinRequest) qsbridge.DiagnosticSet {
	if vector.EdgeCount() < 2 {
		return nil
	}
	for _, edge := range vector.Edges {
		if edge.SQLKind != qsbridge.JoinKindInner || edge.ExecutionKind != qsbridge.RelationshipJoinExecutionVector {
			return legacyDirectRelationshipDiagnostic("relationship-vector graph execution only supports inner relationship-vector joins in this slice")
		}
	}
	return nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipInitialGraphRows(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge) (map[string][]qsbridge.QuantaRownum, []ExecutionProbe, map[string]bool, qsbridge.DiagnosticSet, error) {
	rowsByRole := make(map[string][]qsbridge.QuantaRownum)
	fallbackByRole := make(map[string]legacyDirectRelationshipRoleFallback)
	fullDomainRowsByRole := make(map[string]bool)
	for _, edge := range edges {
		if _, ok := fallbackByRole[edge.parentKey()]; !ok {
			fallbackByRole[edge.parentKey()] = legacyDirectRelationshipRoleFallback{table: edge.parentTable, role: edge.parentRole, field: edge.parentField}
		}
		if _, ok := fallbackByRole[edge.childKey()]; !ok {
			fallbackByRole[edge.childKey()] = legacyDirectRelationshipRoleFallback{table: edge.childTable, role: edge.childRole, field: edge.childField}
		}
	}
	roles := make([]string, 0, len(fallbackByRole))
	for role := range fallbackByRole {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	probes := make([]ExecutionProbe, 0, len(roles)*4)
	for _, role := range roles {
		fallback := fallbackByRole[role]
		rowsStart := time.Now()
		rows, seedKind, seedProbes, diagnostics, err := e.legacyDirectRelationshipInitialRownumsForRole(ctx, request, fallback, edges)
		rowsElapsed := time.Since(rowsStart)
		if err != nil || diagnostics.BlocksNative() {
			return nil, probes, fullDomainRowsByRole, diagnostics, err
		}
		rowsByRole[role] = rows
		fullDomainRowsByRole[role] = legacyDirectRelationshipInitialSeedIsFullDomain(seedKind)
		prefix := "graph_initial_rows_" + role + "_"
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"table", fallback.table),
			legacyDirectRelationshipProbe(prefix+"seed", seedKind),
			legacyDirectRelationshipProbe(prefix+"rows", strconv.Itoa(len(rows))),
			legacyDirectRelationshipProbe(prefix+"elapsed", rowsElapsed.String()),
		)
		probes = append(probes, legacyDirectRelationshipPrefixedProbes(prefix, seedProbes)...)
	}
	return rowsByRole, probes, fullDomainRowsByRole, nil, nil
}

func legacyDirectRelationshipInitialSeedIsFullDomain(seedKind string) bool {
	return seedKind == "relationship_vector_existence" || seedKind == "table_existence"
}

func legacyDirectRelationshipCloneRowsByRole(rowsByRole map[string][]qsbridge.QuantaRownum) map[string][]qsbridge.QuantaRownum {
	cloned := make(map[string][]qsbridge.QuantaRownum, len(rowsByRole))
	for role, rows := range rowsByRole {
		cloned[role] = append([]qsbridge.QuantaRownum(nil), rows...)
	}
	return cloned
}

func legacyDirectRelationshipRetainUnchangedFullDomainRoles(fullDomainRowsByRole map[string]bool, before map[string][]qsbridge.QuantaRownum, after map[string][]qsbridge.QuantaRownum) {
	for role, fullDomain := range fullDomainRowsByRole {
		if !fullDomain {
			continue
		}
		if !legacyDirectRelationshipRownumsEqual(before[role], after[role]) {
			fullDomainRowsByRole[role] = false
		}
	}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipInitialRownumsForRole(ctx context.Context, request ExecutionRequest, fallback legacyDirectRelationshipRoleFallback, edges []legacyDirectRelationshipEdge) ([]qsbridge.QuantaRownum, string, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if rows, ok := legacyDirectRelationshipCandidateRownumsForTable(request, fallback.table); ok {
		return rows, "candidate_set", nil, nil, nil
	}
	if fragments := legacyDirectRelationshipFragmentsForTable(request, fallback.table, fallback.role); len(fragments) > 0 {
		rows, diagnostics, err := e.legacyDirectRelationshipRownumsForTable(ctx, request, fallback.table, fallback.role, fallback.field)
		return rows, "fragments", nil, diagnostics, err
	}
	if rows, ok, seedProbes, diagnostics, err := e.legacyDirectRelationshipInitialRownumsFromRelationshipVector(ctx, request, fallback, edges); ok || diagnostics.BlocksNative() || err != nil {
		return rows, "relationship_vector_existence", seedProbes, diagnostics, err
	}
	rows, diagnostics, err := e.legacyDirectRelationshipAllRownums(ctx, fallback.table, fallback.field)
	return rows, "table_existence", nil, diagnostics, err
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipInitialRownumsFromRelationshipVector(ctx context.Context, request ExecutionRequest, fallback legacyDirectRelationshipRoleFallback, edges []legacyDirectRelationshipEdge) ([]qsbridge.QuantaRownum, bool, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	edge, ok := legacyDirectRelationshipInitialSeedEdge(fallback, edges)
	if !ok {
		return nil, false, nil, nil, nil
	}
	projectionStart := time.Now()
	fkBSI, cacheHit, diagnostics, err := e.legacyDirectRelationshipProjectedFullFKBSI(ctx, request, edge)
	projectionElapsed := time.Since(projectionStart)
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("relationship_vector_projection_elapsed", projectionElapsed.String()),
		legacyDirectRelationshipProbe("relationship_vector_projection_cache_hit", strconv.FormatBool(cacheHit)),
	}
	if err != nil || diagnostics.BlocksNative() {
		return nil, true, probes, diagnostics, err
	}
	if fkBSI == nil {
		return nil, false, probes, nil, nil
	}
	existence := fkBSI.GetExistenceBitmap()
	if existence == nil {
		return nil, true, probes, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector seed did not return existence bitmap for %s.%s", edge.childTable, edge.childField)), nil
	}
	rownumStart := time.Now()
	rows := legacyDirectRelationshipRownums(existence)
	rownumElapsed := time.Since(rownumStart)
	probes = append(probes,
		legacyDirectRelationshipProbe("relationship_vector_existence_cardinality", strconv.FormatUint(existence.GetCardinality(), 10)),
		legacyDirectRelationshipProbe("relationship_vector_existence_rownum_elapsed", rownumElapsed.String()),
	)
	return rows, true, probes, nil, nil
}

func legacyDirectRelationshipInitialSeedEdge(fallback legacyDirectRelationshipRoleFallback, edges []legacyDirectRelationshipEdge) (legacyDirectRelationshipEdge, bool) {
	roleKey := legacyDirectRelationshipRoleKey(fallback.role, fallback.table)
	for _, edge := range edges {
		if edge.childKey() == roleKey && strings.EqualFold(edge.childTable, fallback.table) && (fallback.field == "" || strings.EqualFold(edge.childField, fallback.field)) {
			return edge, true
		}
	}
	for _, edge := range edges {
		if edge.childKey() == roleKey && strings.EqualFold(edge.childTable, fallback.table) {
			return edge, true
		}
	}
	return legacyDirectRelationshipEdge{}, false
}

func legacyDirectRelationshipPruneRedundantParentEdges(request ExecutionRequest, edges []legacyDirectRelationshipEdge) ([]legacyDirectRelationshipEdge, []ExecutionProbe) {
	if len(edges) == 0 {
		return edges, nil
	}
	if !legacyDirectRelationshipCanPruneEdgesForResult(request, edges) {
		return edges, []ExecutionProbe{
			legacyDirectRelationshipProbe("graph_edges_before_prune", strconv.Itoa(len(edges))),
			legacyDirectRelationshipProbe("graph_pruned_edges", "0"),
			legacyDirectRelationshipProbe("graph_required_roles", legacyDirectRelationshipSortedRoleKeys(legacyDirectRelationshipRequiredGraphRoles(request))),
			legacyDirectRelationshipProbe("graph_prune_applied", "false"),
			legacyDirectRelationshipProbe("graph_prune_reason", "result_requires_join_tuple_multiplicity"),
		}
	}
	requiredRoles := legacyDirectRelationshipRequiredGraphRoles(request)
	if sinkRole, ok := legacyDirectRelationshipRequiredSinkRoleForCountGraph(request, edges); ok {
		requiredRoles[strings.ToLower(sinkRole)] = struct{}{}
	}
	requiredPathEdges := legacyDirectRelationshipRequiredPathEdges(edges, requiredRoles)
	pruned := 0
	result := make([]legacyDirectRelationshipEdge, 0, len(edges))
	for i, edge := range edges {
		if _, ok := requiredPathEdges[i]; ok {
			result = append(result, edge)
			continue
		}
		if legacyDirectRelationshipCanPruneParentEdge(request, edge, requiredRoles) {
			pruned++
			continue
		}
		result = append(result, edge)
	}
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_edges_before_prune", strconv.Itoa(len(edges))),
		legacyDirectRelationshipProbe("graph_pruned_edges", strconv.Itoa(pruned)),
		legacyDirectRelationshipProbe("graph_required_roles", legacyDirectRelationshipSortedRoleKeys(requiredRoles)),
	}
	if len(result) == 0 {
		probes = append(probes, legacyDirectRelationshipProbe("graph_prune_applied", "false"))
		probes = append(probes, legacyDirectRelationshipProbe("graph_prune_reason", "would_remove_all_edges"))
		return edges, probes
	}
	probes = append(probes, legacyDirectRelationshipProbe("graph_prune_applied", strconv.FormatBool(pruned > 0)))
	return result, probes
}

func legacyDirectRelationshipCanPruneEdgesForResult(request ExecutionRequest, edges []legacyDirectRelationshipEdge) bool {
	if len(edges) <= 1 || len(request.SQLAggregates) == 0 {
		return true
	}
	if directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) && !directBitmapHasResidualScanPredicates(request) && request.NativePredicates.Empty() {
		_, ok := legacyDirectRelationshipRequiredSinkRoleForCountGraph(request, edges)
		return ok
	}
	return true
}

func legacyDirectRelationshipGraphGroupedAggregateNeedsTupleRows(request ExecutionRequest, residuals []qsbridge.Predicate) bool {
	return len(residuals) > 0 || len(request.Memberships) > 0 || !request.NativePredicates.Empty()
}

func legacyDirectRelationshipRequiredSinkRoleForCountGraph(request ExecutionRequest, edges []legacyDirectRelationshipEdge) (string, bool) {
	if len(edges) <= 1 || len(request.SQLAggregates) == 0 {
		return "", false
	}
	if !directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) ||
		directBitmapHasResidualScanPredicates(request) ||
		!request.NativePredicates.Empty() {
		return "", false
	}
	for _, edge := range edges {
		if edge.sqlKind != qsbridge.JoinKindInner || edge.leftOuterPreservesParent {
			return "", false
		}
		if !edge.capabilities.Has(qsbridge.RelationshipCapabilityJoinReduction) &&
			!edge.capabilities.Has(qsbridge.RelationshipCapabilityParentLookup) {
			return "", false
		}
	}
	sinkRole, _, diagnostics := legacyDirectRelationshipGraphSink(edges)
	if diagnostics.BlocksNative() || sinkRole == "" {
		return "", false
	}
	return sinkRole, true
}

func legacyDirectRelationshipRequiredPathEdges(edges []legacyDirectRelationshipEdge, requiredRoles map[string]struct{}) map[int]struct{} {
	requiredGraphRoles := make(map[string]struct{})
	for _, edge := range edges {
		if legacyDirectRelationshipEndpointRequired(edge.parentKey(), edge.parentTable, requiredRoles) {
			requiredGraphRoles[edge.parentKey()] = struct{}{}
		}
		if legacyDirectRelationshipEndpointRequired(edge.childKey(), edge.childTable, requiredRoles) {
			requiredGraphRoles[edge.childKey()] = struct{}{}
		}
	}
	if len(requiredGraphRoles) < 2 {
		return nil
	}
	roles := make([]string, 0, len(requiredGraphRoles))
	for role := range requiredGraphRoles {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	retain := make(map[int]struct{})
	for i := 0; i < len(roles); i++ {
		for j := i + 1; j < len(roles); j++ {
			for _, edgeIndex := range legacyDirectRelationshipRolePathEdges(edges, roles[i], roles[j]) {
				retain[edgeIndex] = struct{}{}
			}
		}
	}
	return retain
}

func legacyDirectRelationshipEndpointRequired(role string, table string, requiredRoles map[string]struct{}) bool {
	if _, ok := requiredRoles[strings.ToLower(role)]; ok {
		return true
	}
	_, ok := requiredRoles[strings.ToLower(table)]
	return ok
}

func legacyDirectRelationshipRolePathEdges(edges []legacyDirectRelationshipEdge, fromRole string, toRole string) []int {
	type pathStep struct {
		role  string
		edges []int
	}
	seen := map[string]struct{}{fromRole: {}}
	queue := []pathStep{{role: fromRole}}
	for len(queue) > 0 {
		step := queue[0]
		queue = queue[1:]
		if step.role == toRole {
			return step.edges
		}
		for i, edge := range edges {
			var next string
			switch step.role {
			case edge.parentKey():
				next = edge.childKey()
			case edge.childKey():
				next = edge.parentKey()
			default:
				continue
			}
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			nextEdges := append(append([]int(nil), step.edges...), i)
			queue = append(queue, pathStep{role: next, edges: nextEdges})
		}
	}
	return nil
}

func legacyDirectRelationshipCanPruneParentEdge(request ExecutionRequest, edge legacyDirectRelationshipEdge, requiredRoles map[string]struct{}) bool {
	if edge.sqlKind != qsbridge.JoinKindInner || edge.leftOuterPreservesParent {
		return false
	}
	parentKey := edge.parentKey()
	if _, ok := requiredRoles[parentKey]; ok {
		return false
	}
	if _, ok := requiredRoles[strings.ToLower(edge.parentTable)]; ok {
		return false
	}
	return len(legacyDirectRelationshipFragmentsForTable(request, edge.parentTable, edge.parentRole)) == 0
}

func legacyDirectRelationshipRequiredGraphRoles(request ExecutionRequest) map[string]struct{} {
	required := make(map[string]struct{})
	addRole := func(role string) {
		role = strings.ToLower(role)
		if role != "" {
			required[role] = struct{}{}
		}
	}
	addTable := func(table qsbridge.TableInstance) {
		addRole(legacyDirectRelationshipTableRoleKey(table))
		if table.Table != "" {
			addRole(table.Table)
		}
	}
	addField := func(ref qsbridge.FieldRef) {
		addTable(ref.Table)
	}
	addExpr := func(expr qsbridge.Expr) {
		for _, ref := range qsbridge.FieldRefs(expr) {
			addField(ref)
		}
	}
	addProjectionField := func(field qsbridge.QuantaProjectionField) {
		if legacyDirectRelationshipProjectionFieldOnlySupportsJoin(field) {
			return
		}
		if field.Role != "" {
			addRole(string(field.Role))
		}
		if field.Index != "" {
			addRole(field.Index)
		}
	}
	for _, projection := range request.Projection {
		addExpr(projection.Expr)
	}
	for _, predicate := range request.Predicates {
		addExpr(predicate.Expr)
	}
	for _, predicate := range request.NativePredicates.CorrelatedAggregate {
		addField(predicate.KeyField)
		addField(predicate.ValueField)
	}
	for _, membership := range request.Memberships {
		addField(membership.Left)
		addField(membership.Right)
		for _, predicate := range membership.Predicates {
			addExpr(predicate.Expr)
		}
	}
	for _, expr := range request.GroupBy {
		addExpr(expr)
	}
	for _, aggregate := range request.SQLAggregates {
		addExpr(aggregate.Input)
		addExpr(aggregate.Filter)
	}
	for _, predicate := range request.Having {
		addExpr(predicate.Expr)
	}
	for _, sort := range request.OrderBy {
		addExpr(sort.Expr)
	}
	for _, field := range request.ProjectionOrder {
		addField(field)
	}
	for _, field := range request.Result.Columns {
		addField(field)
	}
	projectionFields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		projectionFields = request.Materialization.ProjectionFields
	}
	for _, field := range projectionFields {
		addProjectionField(field)
	}
	return required
}

func legacyDirectRelationshipProjectionFieldOnlySupportsJoin(field qsbridge.QuantaProjectionField) bool {
	semanticRoles := qsbridge.FieldRoleVisible |
		qsbridge.FieldRoleGroupKey |
		qsbridge.FieldRoleSortKey |
		qsbridge.FieldRoleResidualInput |
		qsbridge.FieldRoleMutationTarget |
		qsbridge.FieldRoleMutationValue
	return field.Roles.Has(qsbridge.FieldRoleJoinInput) && !field.Roles.Has(semanticRoles)
}

func legacyDirectRelationshipSortedRoleKeys(roles map[string]struct{}) string {
	keys := make([]string, 0, len(roles))
	for role := range roles {
		keys = append(keys, role)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphAlignedRownums(ctx context.Context, request ExecutionRequest, sink string, sinkRows []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, scratchpad legacyDirectRelationshipGraphScratchpad) (map[string][]qsbridge.QuantaRownum, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipGraphAlignedRownumsForRoles(ctx, request, sink, sinkRows, edges, scratchpad, nil)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphAlignedRownumsForRoles(ctx context.Context, request ExecutionRequest, sink string, sinkRows []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, scratchpad legacyDirectRelationshipGraphScratchpad, requiredRoles map[string]struct{}) (map[string][]qsbridge.QuantaRownum, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	sinkRole := legacyDirectRelationshipGraphRoleForTable(edges, sink)
	aligned := map[string][]qsbridge.QuantaRownum{
		sinkRole: append([]qsbridge.QuantaRownum(nil), sinkRows...),
	}
	var probes []ExecutionProbe
	limitedRoles := len(requiredRoles) > 0
	if limitedRoles {
		requiredRoles = legacyDirectRelationshipExpandRequiredAlignmentRoles(requiredRoles, sinkRole, edges)
		probes = append(probes, legacyDirectRelationshipProbe("graph_alignment_required_roles", legacyDirectRelationshipRoleSetDebug(requiredRoles)))
	}
	alignmentEdges := 0
	edgeByChild := make(map[string][]legacyDirectRelationshipEdge, len(edges))
	for _, edge := range edges {
		edgeByChild[edge.childKey()] = append(edgeByChild[edge.childKey()], edge)
	}
	for {
		if limitedRoles && legacyDirectRelationshipAlignedHasRoles(aligned, requiredRoles) {
			break
		}
		changed := false
		for childKey, childRows := range aligned {
			for _, edge := range edgeByChild[childKey] {
				parentKey := edge.parentKey()
				if _, ok := aligned[parentKey]; ok {
					continue
				}
				if limitedRoles {
					if _, ok := requiredRoles[parentKey]; !ok {
						continue
					}
				}
				start := time.Now()
				parentRows, source, diagnostics, err := e.legacyDirectRelationshipGraphAlignedParentRows(ctx, request, edge, childRows, scratchpad)
				elapsed := time.Since(start)
				if err != nil || diagnostics.BlocksNative() {
					return nil, probes, diagnostics, err
				}
				alignmentEdges++
				prefix := "graph_alignment_edge_" + strconv.Itoa(alignmentEdges) + "_"
				probes = append(probes,
					legacyDirectRelationshipProbe(prefix+"source", source),
					legacyDirectRelationshipProbe(prefix+"child_rows", strconv.Itoa(len(childRows))),
					legacyDirectRelationshipProbe(prefix+"parent_rows", strconv.Itoa(len(parentRows))),
					legacyDirectRelationshipProbe("phase_"+prefix+"elapsed", elapsed.String()),
				)
				aligned[parentKey] = parentRows
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	if len(probes) > 0 {
		recordExecutionProbes(ctx, probes)
	}
	return aligned, probes, nil, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphAlignedParentRows(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, scratchpad legacyDirectRelationshipGraphScratchpad) ([]qsbridge.QuantaRownum, string, qsbridge.DiagnosticSet, error) {
	if parentRows, ok := scratchpad.alignedParentRows(edge, childRows); ok {
		return parentRows, "reduction_scratchpad", nil, nil
	}
	projectionPolicy := legacyDirectRelationshipProjectionPolicy(edge, childRows, scratchpad, 1)
	projectionRows, _ := e.legacyDirectRelationshipProjectionRowsForGraphReduce(ctx, request, edge, childRows, scratchpad, projectionPolicy)
	parentByChild, diagnostics, err := e.legacyDirectRelationshipParentMapWithProjectionRows(ctx, request, edge, childRows, projectionRows)
	if err != nil || diagnostics.BlocksNative() {
		return nil, "parent_map", diagnostics, err
	}
	parentRows := make([]qsbridge.QuantaRownum, 0, len(childRows))
	for _, child := range childRows {
		parent, ok := parentByChild[child]
		if !ok {
			return nil, "parent_map", legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector graph could not align %s row %d to parent %s", edge.childTable, child, edge.parentTable)), nil
		}
		parentRows = append(parentRows, parent)
	}
	return parentRows, "parent_map", nil, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipParentMap(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum) (map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipParentMapWithProjectionRows(ctx, request, edge, childRows, childRows)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipParentMapWithProjectionRows(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, projectionRows []qsbridge.QuantaRownum) (map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	if len(childRows) == 0 {
		return map[qsbridge.QuantaRownum]qsbridge.QuantaRownum{}, nil, nil
	}
	if len(projectionRows) == 0 {
		projectionRows = childRows
	}
	fromTime, toTime := e.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, projectionRows)
	domainCacheKey := legacyDirectRelationshipDomainMappingCacheKey(edge, fromTime, toTime)
	domainCacheDetail := legacyDirectRelationshipDomainMappingCacheDetail(domainCacheKey, nil, childRows)
	if domainCache := DomainMappingCacheFromContext(ctx); domainCache != nil {
		if parentByChild, mode, ok := domainCache.GetByChildSubset(domainCacheKey, childRows); ok {
			recordQueryScratchpadCacheLookup(ctx, "domain_mapping_cache", true, mode, domainCacheDetail)
			return parentByChild, nil, nil
		}
		recordQueryScratchpadCacheLookup(ctx, "domain_mapping_cache", false, "miss", domainCacheDetail)
	}
	fkBSI, _, diagnostics, err := e.legacyDirectRelationshipProjectedFKBSI(ctx, request, edge, projectionRows)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	parentByChild, missingChild, complete := legacyDirectRelationshipParentMapFromFKBSI(fkBSI, childRows)
	if !complete {
		fullFKBSI, _, fullDiagnostics, fullErr := e.legacyDirectRelationshipProjectedFullFKBSI(ctx, request, edge)
		if fullErr != nil || fullDiagnostics.BlocksNative() {
			return nil, fullDiagnostics, fullErr
		}
		parentByChild, missingChild, complete = legacyDirectRelationshipParentMapFromFKBSI(fullFKBSI, childRows)
	}
	if !complete {
		return nil, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector FK projection did not contain child row %d", missingChild)), nil
	}
	return parentByChild, nil, nil
}

func legacyDirectRelationshipParentMapFromFKBSI(fkBSI *roaring64.BSI, childRows []qsbridge.QuantaRownum) (map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, qsbridge.QuantaRownum, bool) {
	if fkBSI == nil {
		var missing qsbridge.QuantaRownum
		if len(childRows) > 0 {
			missing = childRows[0]
		}
		return nil, missing, false
	}
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(childRows))
	for _, child := range childRows {
		parent, ok := fkBSI.GetValue(uint64(child))
		if !ok {
			return nil, child, false
		}
		parentByChild[child] = qsbridge.QuantaRownum(parent)
	}
	return parentByChild, 0, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectedFKBSI(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum) (*roaring64.BSI, bool, qsbridge.DiagnosticSet, error) {
	childFoundSet := legacyDirectRelationshipBitmap(childRows)
	fromTime, toTime := e.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, childRows)
	return e.legacyDirectRelationshipProjectedFKBSIForFoundSet(ctx, request, edge, childFoundSet, fromTime, toTime)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectedFullFKBSI(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge) (*roaring64.BSI, bool, qsbridge.DiagnosticSet, error) {
	fromTime, toTime := e.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
	cacheKey := e.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, nil)
	cache := e.relationshipVectorProjectionCache(ctx)
	if cache != nil {
		if fkBSI, ok := cache.Get(cacheKey); ok {
			recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", true, "exact", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
			return fkBSI, true, nil, nil
		}
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", false, "miss", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
	}
	if e.Source == nil && e.RelationshipProjectionReader == nil {
		return nil, false, nil, nil
	}
	return e.legacyDirectRelationshipProjectedFKBSIForFoundSet(ctx, request, edge, nil, fromTime, toTime)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectedFKBSIForFoundSet(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childFoundSet *roaring64.Bitmap, fromTime, toTime int64) (*roaring64.BSI, bool, qsbridge.DiagnosticSet, error) {
	cacheKey := e.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, childFoundSet)
	cache := e.relationshipVectorProjectionCache(ctx)
	if cache != nil {
		if fkBSI, ok := cache.Get(cacheKey); ok {
			recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", true, "exact", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
			return fkBSI, true, nil, nil
		}
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", false, "miss", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
	}
	if fullFKBSI, ok := e.legacyDirectRelationshipCachedFullFKBSI(ctx, edge, fromTime, toTime, childFoundSet); ok {
		return fullFKBSI, true, nil, nil
	}
	if e.Source == nil && e.RelationshipProjectionReader == nil {
		return nil, false, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join has no source for relationship-vector projection"),
		}, nil
	}
	if e.RelationshipProjectionReader != nil {
		read := e.legacyDirectRelationshipVectorReadRequest(edge, childFoundSet)
		fkBSI, diagnostics, err := e.RelationshipProjectionReader.ReadRelationshipVectorProjection(ctx, read)
		if err != nil || diagnostics.BlocksNative() {
			return nil, false, diagnostics, err
		}
		if fkBSI == nil {
			return nil, false, legacyDirectRelationshipDiagnostic(
				fmt.Sprintf("relationship-vector projection did not return %s.%s", edge.childTable, edge.childField),
			), nil
		}
		if cache != nil {
			cache.Put(cacheKey, fkBSI)
			recordQueryScratchpadCacheStore(ctx, "relationship_vector_projection_cache", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
		}
		return fkBSI, false, nil, nil
	}
	provider := LegacyQuantaSourceSessionProvider{Source: e.Source}
	childFKRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     edge.childTable,
		Field:     edge.childField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	session, diagnostics, err := provider.BorrowDirectSession(ctx, childFKRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, false, diagnostics, err
	}
	if session == nil {
		return nil, false, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join received nil child session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return nil, false, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join has no bitmap index"),
		}, nil
	}
	bsiByField, _, err := legacySession.Session.BitIndex.Projection(edge.childTable, []string{edge.childField}, fromTime, toTime, childFoundSet, false)
	if err != nil {
		return nil, false, nil, err
	}
	fkBSI := bsiByField[edge.childField]
	if fkBSI == nil {
		return nil, false, legacyDirectRelationshipDiagnostic(
			fmt.Sprintf("relationship-vector projection did not return %s.%s (%s)",
				edge.childTable, edge.childField,
				legacyDirectRelationshipProjectionDebug(request, edge.childTable, e.legacyDirectCachedTable(edge.childTable), childFoundSet, fromTime, toTime, bsiByField),
			),
		), nil
	}
	if cache != nil {
		cache.Put(cacheKey, fkBSI)
		recordQueryScratchpadCacheStore(ctx, "relationship_vector_projection_cache", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
	}
	return fkBSI, false, nil, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipVectorReadRequest(edge legacyDirectRelationshipEdge, childFoundSet *roaring64.Bitmap) LegacyDirectRelationshipVectorReadRequest {
	sourceRows := legacyDirectRelationshipRownums(childFoundSet)
	planEdge := qsbridge.RelationshipJoinPlanEdge{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{
				Table: edge.childTable,
				Alias: edge.childRole,
			},
			Name:         edge.childField,
			PhysicalName: edge.childField,
		},
		LeftRole: qsbridge.TableInstanceID(edge.childRole),
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{
				Table: edge.parentTable,
				Alias: edge.parentRole,
			},
			Name:         edge.parentField,
			PhysicalName: edge.parentField,
		},
		RightRole:       qsbridge.TableInstanceID(edge.parentRole),
		SQLKind:         edge.sqlKind,
		ProjectionScope: edge.projectionScope,
	}
	return LegacyDirectRelationshipVectorReadRequest{
		SourceCandidates: qsbridge.QuantaCandidateSet{
			Index:   edge.childTable,
			Rownums: sourceRows,
		},
		SourceDomain: edge.childTable,
		TargetDomain: edge.parentTable,
		Edge:         planEdge,
		Direction:    qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
		VectorIndex:  edge.childTable,
		VectorField:  edge.childField,
	}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipCachedFullFKBSI(ctx context.Context, edge legacyDirectRelationshipEdge, fromTime, toTime int64, childFoundSet *roaring64.Bitmap) (*roaring64.BSI, bool) {
	if childFoundSet == nil {
		return nil, false
	}
	cache := e.relationshipVectorProjectionCache(ctx)
	if cache == nil {
		return nil, false
	}
	cacheKey := e.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, nil)
	fkBSI, ok := cache.Get(cacheKey)
	if !ok || fkBSI == nil || fkBSI.GetExistenceBitmap() == nil {
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", false, "miss", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
		return nil, false
	}
	if legacyDirectRelationshipBitmapCovers(fkBSI.GetExistenceBitmap(), childFoundSet) {
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", true, "retained_subset", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
		return fkBSI, true
	}
	recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", false, "coverage_miss", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
	return nil, false
}

func legacyDirectRelationshipBitmapCovers(container *roaring64.Bitmap, subset *roaring64.Bitmap) bool {
	if subset == nil {
		return true
	}
	if container == nil {
		return subset.GetCardinality() == 0
	}
	overlap := subset.Clone()
	overlap.And(container)
	return overlap.GetCardinality() == subset.GetCardinality()
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectionRowsForGraphReduce(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, scratchpad legacyDirectRelationshipGraphScratchpad, policy legacyDirectRelationshipProjectionPolicyResult) ([]qsbridge.QuantaRownum, legacyDirectRelationshipProjectionPolicyResult) {
	initialRows, ok := scratchpad.initialRowsForTable(edge.childTable)
	if !ok || len(initialRows) == 0 || len(initialRows) == len(childRows) {
		return legacyDirectRelationshipProjectionRowsForAppliedPolicy(edge, childRows, scratchpad, policy), policy
	}
	cache := e.relationshipVectorProjectionCache(ctx)
	if edge.projectionScope != qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset || cache == nil {
		return legacyDirectRelationshipProjectionRowsForAppliedPolicy(edge, childRows, scratchpad, policy), policy
	}
	fromTime, toTime := e.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, initialRows)
	cacheKey := e.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, legacyDirectRelationshipBitmap(initialRows))
	if _, ok := cache.Get(cacheKey); !ok {
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", false, "miss", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
		return legacyDirectRelationshipProjectionRowsForAppliedPolicy(edge, childRows, scratchpad, policy), policy
	}
	recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", true, "exact", legacyDirectRelationshipProjectionCacheDetail(cacheKey))
	policy.Strategy = legacyDirectRelationshipProjectionStrategyBroadFromScratchpad
	policy.AppliedStrategy = legacyDirectRelationshipProjectionStrategyBroadFromScratchpad
	policy.Reason = "broad_projection_cache_hit"
	policy.InitialChildRows = len(initialRows)
	policy.ObserveOnly = false
	return initialRows, policy
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectionCacheKey(table string, field string, fromTime int64, toTime int64, foundSet *roaring64.Bitmap) string {
	return strings.Join([]string{
		table,
		field,
		strconv.FormatInt(fromTime, 10),
		strconv.FormatInt(toTime, 10),
		legacyDirectRelationshipVectorFoundSetCacheKey(foundSet),
	}, "\x00")
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipRowsFromChildCandidateSet(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, []qsbridge.QuantaRownum, []legacyDirectRelationshipPair, bool, qsbridge.DiagnosticSet, error) {
	childRows, ok := legacyDirectRelationshipCandidateRownumsForTable(request, edge.childTable)
	if !ok {
		return nil, nil, nil, nil, false, nil, nil
	}
	if len(legacyDirectRelationshipFragmentsForTable(request, edge.parentTable, edge.parentRole)) > 0 {
		return nil, nil, nil, nil, false, nil, nil
	}
	parentByChild, diagnostics, err := e.legacyDirectRelationshipParentMap(ctx, request, edge, childRows)
	if err != nil || diagnostics.BlocksNative() {
		return nil, nil, nil, nil, true, diagnostics, err
	}
	parentRows, joined, pairs := legacyDirectRelationshipRowsFromParentMap(childRows, parentByChild)
	return parentRows, childRows, joined, pairs, true, nil, nil
}

func legacyDirectRelationshipRowsFromParentMap(childRows []qsbridge.QuantaRownum, parentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, []legacyDirectRelationshipPair) {
	pairs := make([]legacyDirectRelationshipPair, 0, len(childRows))
	joined := make([]qsbridge.QuantaRownum, 0, len(childRows))
	parentRows := make([]qsbridge.QuantaRownum, 0, len(childRows))
	for _, child := range childRows {
		parent, ok := parentByChild[child]
		if !ok {
			continue
		}
		joined = append(joined, child)
		parentRows = append(parentRows, parent)
		pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parent})
	}
	return legacyDirectRelationshipUniqueRownums(parentRows), joined, pairs
}

type legacyDirectRelationshipGraphMaterializationFieldState struct {
	field       qsbridge.QuantaProjectionField
	table       string
	roleKey     string
	tableRows   []qsbridge.QuantaRownum
	probePrefix string
	vector      qsbridge.QuantaProjectionVector
	probes      []ExecutionProbe
}

type legacyDirectRelationshipGraphMaterializationGroup struct {
	table     string
	roleKey   string
	tableRows []qsbridge.QuantaRownum
	fields    []int
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipGraphMaterializedRowSet(ctx context.Context, request ExecutionRequest, sink string, sinkRows []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, probePrefix string) (qsbridge.QuantaProjectedRowSet, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	materialization := e.projectionMaterializationKernel()
	probes := make([]ExecutionProbe, 0, len(fields)*8)
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   sink,
		Rownums: append([]qsbridge.QuantaRownum(nil), sinkRows...),
	}
	states := make([]legacyDirectRelationshipGraphMaterializationFieldState, len(fields))
	groups := map[string]*legacyDirectRelationshipGraphMaterializationGroup{}
	groupOrder := []string{}
	for i, field := range fields {
		table := field.Index
		if table == "" {
			table = sink
			field.Index = sink
		}
		roleKey := legacyDirectRelationshipProjectionFieldRoleKey(field, table)
		tableRows, ok := alignedRows[roleKey]
		if !ok {
			if sourceRole := legacyDirectRelationshipUniqueSourceRoleKey(request, table); sourceRole != "" {
				roleKey = sourceRole
				tableRows, ok = alignedRows[roleKey]
			}
		}
		if !ok {
			return qsbridge.QuantaProjectedRowSet{}, probes, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector graph cannot align materialization table %s role %s from sink %s; aligned roles=%s", table, roleKey, sink, legacyDirectRelationshipAlignedRoleDebug(alignedRows))), nil
		}
		fieldProbePrefix := legacyDirectRelationshipMaterializationFieldProbePrefix(probePrefix, i+1, field)
		states[i] = legacyDirectRelationshipGraphMaterializationFieldState{
			field:       field,
			table:       table,
			roleKey:     roleKey,
			tableRows:   tableRows,
			probePrefix: fieldProbePrefix,
		}
		if vector, syntheticProbes, ok := legacyDirectRelationshipSyntheticEndpointProjection(field, roleKey, tableRows, edges, alignedRows, fieldProbePrefix); ok {
			states[i].vector = vector
			states[i].probes = append(states[i].probes, syntheticProbes...)
			continue
		}
		groupKey := table + "\x00" + roleKey
		group := groups[groupKey]
		if group == nil {
			group = &legacyDirectRelationshipGraphMaterializationGroup{
				table:     table,
				roleKey:   roleKey,
				tableRows: tableRows,
			}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}
		group.fields = append(group.fields, i)
	}
	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		groupFields := make([]qsbridge.QuantaProjectionField, 0, len(group.fields))
		for _, fieldIndex := range group.fields {
			groupFields = append(groupFields, states[fieldIndex].field)
		}
		fetchStart := time.Now()
		values, materializationProbes, diagnostics, err := e.legacyDirectRelationshipMaterializedValuesWithProbes(ctx, materialization, group.table, group.tableRows, groupFields, e.legacyDirectRelationshipTimeMaterialization(request, group.table))
		fetchElapsed := time.Since(fetchStart)
		if err != nil || diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, probes, diagnostics, err
		}
		probes = append(probes, materializationProbes...)
		for _, fieldIndex := range group.fields {
			state := &states[fieldIndex]
			fieldValues := values[legacyDirectRelationshipProjectionFieldKey(state.field)]
			vector := qsbridge.QuantaProjectionVector{Field: state.field}
			attachStart := time.Now()
			for _, rownum := range state.tableRows {
				cell, ok := fieldValues[rownum]
				if !ok {
					return qsbridge.QuantaProjectedRowSet{}, probes, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector graph materialization missing value for %s.%s row %d", state.field.Index, state.field.Field, rownum)), nil
				}
				vector.Values = append(vector.Values, cell)
			}
			attachElapsed := time.Since(attachStart)
			state.vector = vector
			state.probes = append(state.probes,
				legacyDirectRelationshipProbe(state.probePrefix+"role", state.roleKey),
				legacyDirectRelationshipProbe(state.probePrefix+"table", state.table),
				legacyDirectRelationshipProbe(state.probePrefix+"field", state.field.Field),
				legacyDirectRelationshipProbe(state.probePrefix+"rows", strconv.Itoa(len(state.tableRows))),
				legacyDirectRelationshipProbe(state.probePrefix+"fetch_elapsed", fetchElapsed.String()),
				legacyDirectRelationshipProbe(state.probePrefix+"attach_elapsed", attachElapsed.String()),
			)
		}
	}
	for _, state := range states {
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, state.vector)
		probes = append(probes, state.probes...)
	}
	return rowSet, probes, rowSet.ValidateShape(), nil
}

type legacyDirectRelationshipSiblingRootExpansion struct {
	edge              legacyDirectRelationshipEdge
	childRowsByParent map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum
}

type legacyDirectRelationshipSiblingRootReducedEdge struct {
	edge      legacyDirectRelationshipEdge
	childRows []qsbridge.QuantaRownum
	pairs     []legacyDirectRelationshipPair
}

func legacyDirectRelationshipTupleRowsFromReducedGraph(rootRole string, rootRows []qsbridge.QuantaRownum, reduced []legacyDirectRelationshipSiblingRootReducedEdge) (RelationshipTupleRowSet, qsbridge.DiagnosticSet) {
	if rootRole == "" {
		return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic("relationship-vector tuple graph execution requires a root role")
	}
	pending := append([]legacyDirectRelationshipSiblingRootReducedEdge(nil), reduced...)
	rowSet := NewRelationshipTupleRowSet(qsbridge.TableInstanceID(rootRole), rootRows)
	for len(pending) > 0 {
		expanded := false
		next := pending[:0]
		for _, edge := range pending {
			parentRole := qsbridge.TableInstanceID(edge.edge.parentKey())
			childRole := qsbridge.TableInstanceID(edge.edge.childKey())
			switch {
			case legacyDirectRelationshipTupleHasRole(rowSet, parentRole):
				rowSet = rowSet.Expand(RelationshipTupleExpansion{
					ParentRole:        parentRole,
					ChildRole:         childRole,
					ChildRowsByParent: legacyDirectRelationshipChildRowsByParent(legacyDirectRelationshipTupleRoleRows(rowSet, parentRole), edge.childRows, edge.pairs),
				})
				expanded = true
			case legacyDirectRelationshipTupleHasRole(rowSet, childRole):
				rowSet = rowSet.Expand(RelationshipTupleExpansion{
					ParentRole:        childRole,
					ChildRole:         parentRole,
					ChildRowsByParent: legacyDirectRelationshipParentRowsByChild(legacyDirectRelationshipTupleRoleRows(rowSet, childRole), edge.pairs),
				})
				expanded = true
			default:
				next = append(next, edge)
			}
		}
		if !expanded {
			return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic("relationship-vector tuple graph has edges that cannot be reached from root " + rootRole)
		}
		pending = next
	}
	return rowSet, nil
}

func legacyDirectRelationshipTupleHasRole(rowSet RelationshipTupleRowSet, role qsbridge.TableInstanceID) bool {
	for _, row := range rowSet.Rows {
		if _, ok := row.Rownums[role]; ok {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipTupleRoleRows(rowSet RelationshipTupleRowSet, role qsbridge.TableInstanceID) []qsbridge.QuantaRownum {
	seen := make(map[qsbridge.QuantaRownum]struct{})
	rows := make([]qsbridge.QuantaRownum, 0)
	for _, row := range rowSet.Rows {
		rownum, ok := row.Rownums[role]
		if !ok {
			continue
		}
		if _, ok := seen[rownum]; ok {
			continue
		}
		seen[rownum] = struct{}{}
		rows = append(rows, rownum)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
	return rows
}

func legacyDirectRelationshipSiblingRootTupleRowsFromReducedEdges(shape legacyDirectRelationshipSiblingRootGraph, rootRows []qsbridge.QuantaRownum, reduced []legacyDirectRelationshipSiblingRootReducedEdge) (RelationshipTupleRowSet, qsbridge.DiagnosticSet) {
	expansions := make([]legacyDirectRelationshipSiblingRootExpansion, 0, len(reduced))
	for _, edge := range reduced {
		expansions = append(expansions, legacyDirectRelationshipSiblingRootExpansion{
			edge:              edge.edge,
			childRowsByParent: legacyDirectRelationshipChildRowsByParent(rootRows, edge.childRows, edge.pairs),
		})
	}
	return legacyDirectRelationshipSiblingRootTupleRows(shape, rootRows, expansions)
}

func legacyDirectRelationshipSiblingRootTuplePreviewResult(shape legacyDirectRelationshipSiblingRootGraph, rootRows []qsbridge.QuantaRownum, reduced []legacyDirectRelationshipSiblingRootReducedEdge) ExecutionResult {
	rowSet, diagnostics := legacyDirectRelationshipSiblingRootTupleRowsFromReducedEdges(shape, rootRows, reduced)
	result := ExecutionResult{Diagnostics: diagnostics, Count: uint64(rowSet.CandidateCount())}
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_shape", "sibling_root"),
		legacyDirectRelationshipProbe("graph_sibling_root", legacyDirectRelationshipGraphSiblingRootDebug(shape)),
		legacyDirectRelationshipProbe("graph_sibling_children", strings.Join(shape.childTables, ",")),
	)
	result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded: rowSet,
		Filtered: rowSet,
	})...)
	return result
}

func legacyDirectRelationshipSiblingRootProjectedAggregateResult(shape legacyDirectRelationshipSiblingRootGraph, rootRows []qsbridge.QuantaRownum, reduced []legacyDirectRelationshipSiblingRootReducedEdge, index string, fields []qsbridge.QuantaProjectionField, values RelationshipTupleValueStore, residualRequest ExecutionRequest, aggregateRequest ExecutionRequest) ExecutionResult {
	tupleRows, diagnostics := legacyDirectRelationshipSiblingRootTupleRowsFromReducedEdges(shape, rootRows, reduced)
	result := ExecutionResult{Diagnostics: diagnostics}
	if result.Diagnostics.BlocksNative() {
		return result
	}
	projected, diagnostics := tupleRows.ToProjectedRowSet(index, fields, values)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	filteredProjected, filteredTupleRows, diagnostics := FilterRelationshipTupleProjectedResiduals(tupleRows, residualRequest, projected)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_shape", "sibling_root"),
		legacyDirectRelationshipProbe("graph_sibling_root", legacyDirectRelationshipGraphSiblingRootDebug(shape)),
		legacyDirectRelationshipProbe("graph_sibling_children", strings.Join(shape.childTables, ",")),
	)
	result.Probes = append(result.Probes, RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded:           tupleRows,
		Filtered:           filteredTupleRows,
		MaterializedFields: fields,
		AggregateAlias:     relationshipTupleAggregateAlias(aggregateRequest),
	})...)
	if len(aggregateRequest.GroupBy) > 0 {
		return directBitmapMaterializedGroupedAggregateResult(aggregateRequest, filteredProjected, result)
	}
	return directBitmapMaterializedAggregateResult(aggregateRequest, filteredProjected, result)
}

func legacyDirectRelationshipTupleRowsByRole(tupleRows RelationshipTupleRowSet) map[string][]qsbridge.QuantaRownum {
	byRole := make(map[string][]qsbridge.QuantaRownum)
	seen := make(map[string]map[qsbridge.QuantaRownum]struct{})
	for _, row := range tupleRows.Rows {
		for role, rownum := range row.Rownums {
			roleKey := string(role)
			if seen[roleKey] == nil {
				seen[roleKey] = make(map[qsbridge.QuantaRownum]struct{})
			}
			if _, ok := seen[roleKey][rownum]; ok {
				continue
			}
			seen[roleKey][rownum] = struct{}{}
			byRole[roleKey] = append(byRole[roleKey], rownum)
		}
	}
	for role := range byRole {
		sort.Slice(byRole[role], func(i, j int) bool { return byRole[role][i] < byRole[role][j] })
	}
	return byRole
}

func legacyDirectRelationshipTupleRoleDebug(rowsByRole map[string][]qsbridge.QuantaRownum) string {
	roles := make([]string, 0, len(rowsByRole))
	for role, rows := range rowsByRole {
		roles = append(roles, fmt.Sprintf("%s:%d", role, len(rows)))
	}
	sort.Strings(roles)
	return strings.Join(roles, ",")
}

func legacyDirectRelationshipParentRowsByChild(childRows []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum {
	childAllowed := make(map[qsbridge.QuantaRownum]struct{}, len(childRows))
	for _, row := range childRows {
		childAllowed[row] = struct{}{}
	}
	seen := make(map[legacyDirectRelationshipPair]struct{}, len(pairs))
	byChild := make(map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum)
	for _, pair := range pairs {
		if _, ok := childAllowed[pair.child]; !ok {
			continue
		}
		if _, ok := seen[pair]; ok {
			continue
		}
		seen[pair] = struct{}{}
		byChild[pair.child] = append(byChild[pair.child], pair.parent)
	}
	return byChild
}

func legacyDirectRelationshipChildRowsByParent(rootRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum {
	rootAllowed := make(map[qsbridge.QuantaRownum]struct{}, len(rootRows))
	for _, row := range rootRows {
		rootAllowed[row] = struct{}{}
	}
	childAllowed := make(map[qsbridge.QuantaRownum]struct{}, len(childRows))
	for _, row := range childRows {
		childAllowed[row] = struct{}{}
	}
	seen := make(map[legacyDirectRelationshipPair]struct{}, len(pairs))
	byParent := make(map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum)
	for _, pair := range pairs {
		if _, ok := rootAllowed[pair.parent]; !ok {
			continue
		}
		if _, ok := childAllowed[pair.child]; !ok {
			continue
		}
		if _, ok := seen[pair]; ok {
			continue
		}
		seen[pair] = struct{}{}
		byParent[pair.parent] = append(byParent[pair.parent], pair.child)
	}
	return byParent
}

func legacyDirectRelationshipSiblingRootTupleRows(shape legacyDirectRelationshipSiblingRootGraph, rootRows []qsbridge.QuantaRownum, expansions []legacyDirectRelationshipSiblingRootExpansion) (RelationshipTupleRowSet, qsbridge.DiagnosticSet) {
	if shape.rootRole == "" {
		return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic("relationship-vector sibling-root tuple execution requires a root role")
	}
	if len(expansions) == 0 {
		return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic("relationship-vector sibling-root tuple execution requires child expansions")
	}
	tupleExpansions := make([]RelationshipTupleExpansion, 0, len(expansions))
	for _, expansion := range expansions {
		if expansion.edge.parentKey() != shape.rootRole {
			return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector sibling expansion parent %s does not match root %s", expansion.edge.parentKey(), shape.rootRole))
		}
		if len(expansion.childRowsByParent) == 0 {
			return RelationshipTupleRowSet{}, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector sibling expansion for %s has no parent-child rows", expansion.edge.childKey()))
		}
		tupleExpansions = append(tupleExpansions, RelationshipTupleExpansion{
			ParentRole:        qsbridge.TableInstanceID(shape.rootRole),
			ChildRole:         qsbridge.TableInstanceID(expansion.edge.childKey()),
			ChildRowsByParent: expansion.childRowsByParent,
		})
	}
	return NewRelationshipTupleRowSetFromRootExpansions(qsbridge.TableInstanceID(shape.rootRole), rootRows, tupleExpansions), nil
}

func legacyDirectRelationshipSiblingRootBlockedResult(edges []legacyDirectRelationshipEdge, shape legacyDirectRelationshipSiblingRootGraph) ExecutionResult {
	debug := legacyDirectRelationshipGraphSiblingRootDebug(shape)
	return ExecutionResult{
		Diagnostics: legacyDirectRelationshipDiagnostic("relationship-vector sibling-root tuple execution is not wired in this slice: " + debug),
		Probes: []ExecutionProbe{
			legacyDirectRelationshipProbe("graph_edges", strconv.Itoa(len(edges))),
			legacyDirectRelationshipProbe("graph_shape", "sibling_root"),
			legacyDirectRelationshipProbe("graph_sibling_root", debug),
			legacyDirectRelationshipProbe("graph_sibling_children", strings.Join(shape.childTables, ",")),
		},
	}
}

func legacyDirectRelationshipGraphSinkTable(edges []legacyDirectRelationshipEdge) (string, qsbridge.DiagnosticSet) {
	_, sink, diagnostics := legacyDirectRelationshipGraphSink(edges)
	return sink, diagnostics
}

type legacyDirectRelationshipSiblingRootGraph struct {
	rootRole    string
	rootTable   string
	childRoles  []string
	childTables []string
}

func legacyDirectRelationshipSiblingRootGraphShape(edges []legacyDirectRelationshipEdge) (legacyDirectRelationshipSiblingRootGraph, bool, qsbridge.DiagnosticSet) {
	if len(edges) < 2 {
		return legacyDirectRelationshipSiblingRootGraph{}, false, nil
	}
	childRoles := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		childRoles[edge.childKey()] = struct{}{}
	}
	rootRole := ""
	rootTable := ""
	for _, edge := range edges {
		parentRole := edge.parentKey()
		if _, isChild := childRoles[parentRole]; isChild {
			continue
		}
		if rootRole != "" && rootRole != parentRole {
			return legacyDirectRelationshipSiblingRootGraph{}, false, nil
		}
		rootRole = parentRole
		rootTable = edge.parentTable
	}
	if rootRole == "" {
		return legacyDirectRelationshipSiblingRootGraph{}, false, nil
	}
	shape := legacyDirectRelationshipSiblingRootGraph{rootRole: rootRole, rootTable: rootTable}
	seenChildren := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		if edge.parentKey() != rootRole {
			return legacyDirectRelationshipSiblingRootGraph{}, false, nil
		}
		childRole := edge.childKey()
		if _, ok := seenChildren[childRole]; ok {
			return legacyDirectRelationshipSiblingRootGraph{}, false, legacyDirectRelationshipDiagnostic("relationship-vector sibling graph requires unique child roles")
		}
		seenChildren[childRole] = struct{}{}
		shape.childRoles = append(shape.childRoles, childRole)
		shape.childTables = append(shape.childTables, edge.childTable)
	}
	if len(shape.childRoles) < 2 {
		return legacyDirectRelationshipSiblingRootGraph{}, false, nil
	}
	sort.Strings(shape.childRoles)
	sort.Strings(shape.childTables)
	return shape, true, nil
}

func legacyDirectRelationshipGraphSiblingRootDebug(shape legacyDirectRelationshipSiblingRootGraph) string {
	return fmt.Sprintf("%s:%s->%s", shape.rootRole, shape.rootTable, strings.Join(shape.childRoles, ","))
}

func legacyDirectRelationshipGraphSink(edges []legacyDirectRelationshipEdge) (string, string, qsbridge.DiagnosticSet) {
	parentRoles := make(map[string]struct{}, len(edges))
	childRoles := make(map[string]struct{}, len(edges))
	childNameByRole := make(map[string]string, len(edges))
	for _, edge := range edges {
		parentRoles[edge.parentKey()] = struct{}{}
		key := edge.childKey()
		childRoles[key] = struct{}{}
		childNameByRole[key] = edge.childTable
	}
	var sinkRole string
	var sinkTable string
	for key := range childRoles {
		if _, ok := parentRoles[key]; ok {
			continue
		}
		if sinkRole != "" {
			return "", "", legacyDirectRelationshipDiagnostic("relationship-vector graph execution requires a single sink table")
		}
		sinkRole = key
		sinkTable = childNameByRole[key]
	}
	if sinkRole == "" {
		return "", "", legacyDirectRelationshipDiagnostic("relationship-vector graph execution requires a sink table")
	}
	return sinkRole, sinkTable, nil
}

func legacyDirectRelationshipGraphRoleForTable(edges []legacyDirectRelationshipEdge, table string) string {
	fallback := strings.ToLower(table)
	for _, edge := range edges {
		if strings.EqualFold(edge.childTable, table) {
			return edge.childKey()
		}
		if strings.EqualFold(edge.parentTable, table) {
			fallback = edge.parentKey()
		}
	}
	return fallback
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipOrderedChainEdges(vector RelationshipVectorJoinRequest) ([]legacyDirectRelationshipEdge, qsbridge.DiagnosticSet) {
	edges := make([]legacyDirectRelationshipEdge, 0, len(vector.Edges))
	childTables := make(map[string]struct{}, len(vector.Edges))
	for _, planned := range vector.Edges {
		edge, diagnostics := e.legacyDirectRelationshipEdge(planned)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		edges = append(edges, edge)
		childTables[strings.ToLower(edge.childTable)] = struct{}{}
	}
	start := -1
	for i, edge := range edges {
		if _, ok := childTables[strings.ToLower(edge.parentTable)]; !ok {
			if start >= 0 {
				return nil, legacyDirectRelationshipDiagnostic("chained relationship-vector execution requires a single root parent table")
			}
			start = i
		}
	}
	if start < 0 {
		return nil, legacyDirectRelationshipDiagnostic("chained relationship-vector execution requires an acyclic parent-to-child chain")
	}
	ordered := make([]legacyDirectRelationshipEdge, 0, len(edges))
	used := make([]bool, len(edges))
	current := edges[start]
	for {
		ordered = append(ordered, current)
		used[start] = true
		next := -1
		for i, edge := range edges {
			if used[i] {
				continue
			}
			if strings.EqualFold(edge.parentTable, current.childTable) {
				if next >= 0 {
					return nil, legacyDirectRelationshipDiagnostic("chained relationship-vector execution does not support branching chains in this slice")
				}
				next = i
			}
		}
		if next < 0 {
			break
		}
		start = next
		current = edges[next]
	}
	if len(ordered) != len(edges) {
		return nil, legacyDirectRelationshipDiagnostic("chained relationship-vector execution requires connected parent-to-child edges")
	}
	return ordered, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectSingleRelationshipEdge(vector RelationshipVectorJoinRequest) (legacyDirectRelationshipEdge, qsbridge.DiagnosticSet) {
	if vector.EdgeCount() != 1 {
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic("relationship-vector execution only supports one edge in this slice")
	}
	planned, _ := vector.FirstEdge()
	if planned.ExecutionKind != qsbridge.RelationshipJoinExecutionVector {
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic("relationship-vector execution only supports relationship-vector joins in this slice")
	}
	if planned.SQLKind != qsbridge.JoinKindInner && planned.SQLKind != qsbridge.JoinKindLeftOuter {
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic("relationship-vector execution only supports inner and left outer relationship-vector joins in this slice")
	}
	edge, diagnostics := e.legacyDirectRelationshipEdge(planned)
	if diagnostics.BlocksNative() {
		return legacyDirectRelationshipEdge{}, diagnostics
	}
	if edge.sqlKind == qsbridge.JoinKindLeftOuter && !edge.leftOuterPreservesParent {
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic("left outer relationship-vector execution only supports preserving the relationship parent side in this slice")
	}
	return edge, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipEdge(planned RelationshipJoinPlanEdge) (legacyDirectRelationshipEdge, qsbridge.DiagnosticSet) {
	left, leftOK := e.legacyDirectEndpointRelationship(planned, planned.Left, planned.Right)
	right, rightOK := e.legacyDirectEndpointRelationship(planned, planned.Right, planned.Left)
	switch {
	case leftOK && !rightOK:
		return left, nil
	case rightOK && !leftOK:
		return right, nil
	case leftOK && rightOK:
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic("ambiguous relationship-vector edge has ParentRelation metadata on both sides")
	default:
		return legacyDirectRelationshipEdge{}, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector edge has no ParentRelation endpoint: %s = %s", planned.Left.QualifiedName(), planned.Right.QualifiedName()))
	}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectEndpointRelationship(planned RelationshipJoinPlanEdge, endpoint qsbridge.FieldRef, other qsbridge.FieldRef) (legacyDirectRelationshipEdge, bool) {
	table := e.legacyDirectCachedTable(endpoint.Table.Table)
	if table == nil {
		return legacyDirectRelationshipEdge{}, false
	}
	attr, err := table.GetAttribute(endpoint.Name)
	if err != nil || attr == nil || !strings.EqualFold(attr.MappingStrategy, "ParentRelation") {
		return legacyDirectRelationshipEdge{}, false
	}
	parentTable, parentField := parseLegacyForeignKey(attr.ForeignKey)
	if parentTable == "" || !strings.EqualFold(parentTable, other.Table.Table) {
		return legacyDirectRelationshipEdge{}, false
	}
	if parentField == "" {
		parentField = other.Name
	}
	childField := attr.FieldName
	if childField == "" {
		childField = attr.SourceName
	}
	return legacyDirectRelationshipEdge{
		childRole:                legacyDirectRelationshipTableRoleKey(endpoint.Table),
		childTable:               table.Name,
		childField:               childField,
		parentRole:               legacyDirectRelationshipTableRoleKey(other.Table),
		parentTable:              parentTable,
		parentField:              parentField,
		capabilities:             planned.Capabilities,
		sqlKind:                  planned.SQLKind,
		leftOuterPreservesParent: planned.SQLKind == qsbridge.JoinKindLeftOuter && legacyDirectRelationshipSameTableInstance(planned.Left.Table, other.Table),
		projectionScope:          planned.ProjectionScope,
	}, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectCachedTable(name string) *core.Table {
	if table := legacyDirectRelationshipCachedTable(e.TableCache, name); table != nil {
		return table
	}
	if e.Source == nil || e.Source.GetSessionPool() == nil {
		return nil
	}
	return legacyDirectRelationshipCachedTable(e.Source.GetSessionPool().TableCache, name)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipSessionProvider() DirectSessionProvider {
	if e.Sessions != nil {
		return e.Sessions
	}
	if e.Source != nil {
		return LegacyQuantaSourceSessionProvider{Source: e.Source}
	}
	return nil
}

func legacyDirectRelationshipCachedTable(cache *core.TableCacheStruct, name string) *core.Table {
	if cache == nil {
		return nil
	}
	cache.TableCacheLock.RLock()
	defer cache.TableCacheLock.RUnlock()
	for tableName, table := range cache.TableCache {
		if strings.EqualFold(tableName, name) || (table != nil && strings.EqualFold(table.Name, name)) {
			return table
		}
	}
	return nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipAllRownums(ctx context.Context, table string, field string) ([]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	provider := e.legacyDirectRelationshipSessionProvider()
	if provider == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join has no session provider"),
		}, nil
	}
	request := e.legacyDirectRelationshipAllRownumRequest(table, field)
	session, diagnostics, err := provider.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join received nil session"),
		}, nil
	}
	bitmapResult, queryDiagnostics, queryErr := session.QueryBitmap(ctx, request)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, queryErr
	}
	return append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...), diagnostics, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipAllRownumRequest(tableName string, fallbackField string) ExecutionRequest {
	if table := e.legacyDirectCachedTable(tableName); table != nil {
		field, shardWindow := legacyDirectRelationshipAllRownumSeedField(table, fallbackField)
		if shardWindow {
			begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, field)
			return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
				Seeds: []qsbridge.QuantaSeed{{
					Index:       tableName,
					Field:       field,
					Kind:        qsbridge.QuantaSeedTableExistence,
					Begin:       big.NewInt(begin),
					End:         big.NewInt(end),
					ShardWindow: true,
				}},
				ProjectionFields: []qsbridge.QuantaProjectionField{{
					Index: tableName,
					Field: field,
					Type:  qsbridge.DataTypeTime,
				}},
			})
		}
		if field != "" {
			return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Seeds: []qsbridge.QuantaSeed{{
				Index: tableName,
				Field: field,
				Kind:  qsbridge.QuantaSeedTableExistence,
			}}})
		}
	}
	if fallbackField == "" {
		return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	}
	return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Seeds: []qsbridge.QuantaSeed{{
		Index: tableName,
		Field: fallbackField,
		Kind:  qsbridge.QuantaSeedTableExistence,
	}}})
}

func legacyDirectRelationshipAllRownumSeedField(table *core.Table, fallbackField string) (string, bool) {
	if table == nil {
		return "", false
	}
	if timeField := legacyDirectRelationshipTimeQuantumField(table); timeField != "" && legacyDirectTableHasPhysicalShardWindow(table) {
		return timeField, true
	}
	for _, part := range strings.Split(table.PrimaryKey, "+") {
		if field := strings.TrimSpace(part); field != "" {
			return field, false
		}
	}
	for _, attribute := range table.Attributes {
		if strings.EqualFold(attribute.FieldName, "rownum") {
			return attribute.FieldName, false
		}
		if strings.EqualFold(attribute.SourceName, "rownum") {
			return attribute.SourceName, false
		}
	}
	if fallbackField != "" {
		return fallbackField, false
	}
	if timeField := legacyDirectRelationshipTimeQuantumField(table); timeField != "" {
		return timeField, false
	}
	for _, attribute := range table.Attributes {
		if attribute.FieldName != "" && !strings.EqualFold(attribute.FieldName, "rownum") {
			return attribute.FieldName, false
		}
		if attribute.SourceName != "" && !strings.EqualFold(attribute.SourceName, "rownum") {
			return attribute.SourceName, false
		}
	}
	return "", false
}

func legacyDirectRelationshipTimeQuantumField(table *core.Table) string {
	if table == nil {
		return ""
	}
	if table.TimeQuantumField != "" {
		return table.TimeQuantumField
	}
	for _, attribute := range table.Attributes {
		fieldName := attribute.FieldName
		if fieldName == "" {
			fieldName = attribute.SourceName
		}
		if fieldName == "" {
			continue
		}
		if legacyDataType(attribute.Type) == qsbridge.DataTypeTime &&
			qsbridge.LegacyEncodingProfile(attribute.MappingStrategy, qsbridge.LegacyEncodingOptions{
				Granularity: LegacyTimeBSIGranularity(attribute.MapperConfig),
			}).Kind == qsbridge.EncodingTimeBSI {
			return fieldName
		}
	}
	return ""
}

func legacyDirectRelationshipTimeAttribute(table *core.Table, field string) *core.Attribute {
	if table == nil || field == "" {
		return nil
	}
	if table.AttributeNameMap != nil {
		if attr := table.AttributeNameMap[field]; attr != nil {
			return attr
		}
		for name, attr := range table.AttributeNameMap {
			if strings.EqualFold(name, field) {
				return attr
			}
		}
	}
	for i := range table.Attributes {
		attribute := table.Attributes[i]
		if strings.EqualFold(attribute.FieldName, field) || strings.EqualFold(attribute.SourceName, field) {
			return &table.Attributes[i]
		}
	}
	return nil
}

func legacyDirectRelationshipFullTimeRangeEncoded(table *core.Table, field string) (int64, int64) {
	return legacyDirectRelationshipTimeMillisToEncoded(table, field, legacyDirectRelationshipFullTimeRangeBeginMillis),
		legacyDirectRelationshipTimeMillisToEncoded(table, field, legacyDirectRelationshipFullTimeRangeEndMillis)
}

func legacyDirectRelationshipTimeMillisToEncoded(table *core.Table, field string, epochMillis int64) int64 {
	switch legacyDirectRelationshipTimeGranularity(table, field) {
	case qsbridge.TimeGranularityMicrosecond:
		return epochMillis * int64(time.Millisecond/time.Microsecond)
	case qsbridge.TimeGranularitySecond:
		return epochMillis / int64(time.Second/time.Millisecond)
	case qsbridge.TimeGranularityNanosecond:
		return epochMillis * int64(time.Millisecond)
	default:
		return epochMillis
	}
}

func legacyDirectRelationshipEncodedTimeToNanos(table *core.Table, field string, encoded int64) int64 {
	switch legacyDirectRelationshipTimeGranularity(table, field) {
	case qsbridge.TimeGranularityMicrosecond:
		return encoded * int64(time.Microsecond)
	case qsbridge.TimeGranularitySecond:
		return encoded * int64(time.Second)
	case qsbridge.TimeGranularityNanosecond:
		return encoded
	default:
		return encoded * int64(time.Millisecond)
	}
}

func legacyDirectRelationshipTimeGranularity(table *core.Table, field string) qsbridge.TimeGranularity {
	if attr := legacyDirectRelationshipTimeAttribute(table, field); attr != nil {
		profile := qsbridge.LegacyEncodingProfile(attr.MappingStrategy, qsbridge.LegacyEncodingOptions{
			Granularity: LegacyTimeBSIGranularity(attr.MapperConfig),
		})
		if profile.Granularity != qsbridge.TimeGranularityUnknown {
			return profile.Granularity
		}
	}
	return qsbridge.TimeGranularityMillisecond
}

func legacyDirectRelationshipCandidateRownumsForTable(request ExecutionRequest, table string) ([]qsbridge.QuantaRownum, bool) {
	if !request.HasCandidateSet || !strings.EqualFold(request.CandidateSet.Index, table) {
		return nil, false
	}
	if len(request.CandidateSet.Rownums) == 0 && legacyDirectRelationshipEmptyCandidateSetShouldDeferToResiduals(request) {
		return nil, false
	}
	return append([]qsbridge.QuantaRownum(nil), request.CandidateSet.Rownums...), true
}

func legacyDirectRelationshipEmptyCandidateSetShouldDeferToResiduals(request ExecutionRequest) bool {
	return len(request.Query.Fragments) == 0 &&
		request.Query.Filter.Empty() &&
		len(request.Joins) > 0 &&
		len(request.Predicates) > 0
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipRownumsForTable(ctx context.Context, request ExecutionRequest, table string, role string, fallbackField string) ([]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	if rows, ok := legacyDirectRelationshipCandidateRownumsForTable(request, table); ok {
		return rows, nil, nil
	}
	fragments := legacyDirectRelationshipFragmentsForTable(request, table, role)
	if len(fragments) == 0 {
		return e.legacyDirectRelationshipAllRownums(ctx, table, fallbackField)
	}
	query := qsbridge.QuantaIntermediateQuery{Fragments: fragments}
	tableRequest := NewExecutionRequest(query)
	tableRequest.Sources = append([]qsbridge.TableInstance(nil), request.Sources...)
	tableRequest.Materialization = e.legacyDirectRelationshipTimeMaterializationForRole(request, table, role)
	provider := e.legacyDirectRelationshipSessionProvider()
	if provider == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join has no session provider"),
		}, nil
	}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, tableRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship join received nil filtered session"),
		}, nil
	}
	bitmapResult, queryDiagnostics, queryErr := session.QueryBitmap(ctx, tableRequest)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, queryErr
	}
	return append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...), diagnostics, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReduce(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, qsbridge.DiagnosticSet, error) {
	joined, pairs, _, diagnostics, err := e.legacyDirectRelationshipReduceWithTiming(ctx, request, edge, parentRows, childRows)
	return joined, pairs, diagnostics, err
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReduceWithTiming(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, legacyDirectRelationshipReduceTiming, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipReduceWithProjectionRows(ctx, request, edge, parentRows, childRows, childRows)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReduceWithProjectionRows(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum, projectionRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, legacyDirectRelationshipReduceTiming, qsbridge.DiagnosticSet, error) {
	return e.legacyDirectRelationshipReduceWithProjectionRowsOptions(ctx, request, edge, parentRows, childRows, projectionRows, legacyDirectRelationshipReduceOptions{})
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReduceWithProjectionRowsOptions(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum, projectionRows []qsbridge.QuantaRownum, options legacyDirectRelationshipReduceOptions) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, legacyDirectRelationshipReduceTiming, qsbridge.DiagnosticSet, error) {
	var timing legacyDirectRelationshipReduceTiming
	if len(parentRows) == 0 || len(childRows) == 0 {
		return nil, nil, timing, nil, nil
	}
	if len(projectionRows) == 0 {
		projectionRows = childRows
	}
	timing.projectionRows = len(projectionRows)
	timing.domainMappingCacheMode = "miss"
	fromTime, toTime := e.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, projectionRows)
	domainCacheKey := legacyDirectRelationshipDomainMappingCacheKey(edge, fromTime, toTime)
	domainCacheDetail := legacyDirectRelationshipDomainMappingCacheDetail(domainCacheKey, parentRows, childRows)
	if domainCache := DomainMappingCacheFromContext(ctx); domainCache != nil {
		if parentByChild, mode, ok := domainCache.Get(domainCacheKey, parentRows, childRows); ok {
			recordQueryScratchpadCacheLookup(ctx, "domain_mapping_cache", true, mode, domainCacheDetail)
			_, joined, pairs := legacyDirectRelationshipRowsFromParentMap(childRows, parentByChild)
			timing.domainMappingCacheHit = true
			timing.domainMappingCacheMode = mode
			timing.parentKeyRows = len(legacyDirectRelationshipUniqueRownums(parentRows))
			timing.matchedRows = len(joined)
			timing.fkProjectionScope = "domain_mapping_cache"
			timing.childRetainCovered = true
			timing.childRetainMode = "domain_mapping_cache"
			return joined, pairs, timing, nil, nil
		}
		recordQueryScratchpadCacheLookup(ctx, "domain_mapping_cache", false, "miss", domainCacheDetail)
	}
	parentKeyStart := time.Now()
	parentKeyRows, parentKeyMaterialized, diagnostics, err := e.legacyDirectRelationshipParentKeyRows(ctx, request, edge, parentRows)
	timing.parentKeyElapsed = time.Since(parentKeyStart)
	timing.parentKeyMaterialization = parentKeyMaterialized
	timing.parentKeyRows = len(parentKeyRows)
	if err != nil || diagnostics.BlocksNative() {
		return nil, nil, timing, diagnostics, err
	}
	effectiveChildRows := childRows
	effectiveProjectionRows := projectionRows
	narrowedRows, artifactResult, artifactParentByChild, artifactPairs, artifactTiming, artifactOK, artifactDiagnostics, artifactErr := e.legacyDirectRelationshipReverseArtifactChildRows(ctx, edge, childRows, parentKeyRows, options)
	if !artifactOK {
		timing.reverseArtifactSkipReason = artifactTiming.mode
		timing.reverseArtifactSourceValues = artifactTiming.sourceValues
		timing.reverseArtifactCandidateRows = artifactTiming.candidateRows
		timing.reverseArtifactElapsed = artifactTiming.elapsed
		timing.reverseArtifactLookupElapsed = artifactTiming.lookupElapsed
		timing.reverseArtifactFanoutElapsed = artifactTiming.fanoutElapsed
		timing.reverseArtifactClientRPCElapsed = artifactTiming.clientRPCElapsed
		timing.reverseArtifactClientRPCMaxElapsed = artifactTiming.maxClientRPCElapsed
		timing.reverseArtifactResponseMergeElapsed = artifactTiming.responseMergeElapsed
		timing.reverseArtifactRowMergeElapsed = artifactTiming.rowMergeElapsed
		timing.reverseArtifactParentMergeElapsed = artifactTiming.parentMergeElapsed
		timing.reverseArtifactSortElapsed = artifactTiming.sortElapsed
		timing.reverseArtifactSourceElapsed = artifactTiming.sourceElapsed
		timing.reverseArtifactReadElapsed = artifactTiming.readElapsed
	} else {
		timing.reverseArtifactUsed = true
		timing.reverseArtifactMode = artifactResult.CandidateMode
		timing.reverseArtifactCacheHit = artifactResult.CandidateCacheHit
		timing.reverseArtifactSourceValues = artifactResult.SourceValueCount
		timing.reverseArtifactCandidateRows = len(artifactResult.TargetCandidates.Rownums)
		timing.reverseArtifactNarrowedRows = len(narrowedRows)
		timing.reverseArtifactElapsed = artifactResult.CandidateElapsed
		timing.reverseArtifactLookupElapsed = artifactResult.CandidateScanElapsed
		timing.reverseArtifactFanoutElapsed = artifactResult.CandidateFanoutElapsed
		timing.reverseArtifactClientRPCElapsed = artifactResult.CandidateClientRPCElapsed
		timing.reverseArtifactClientRPCMaxElapsed = artifactResult.CandidateClientRPCMaxElapsed
		timing.reverseArtifactResponseMergeElapsed = artifactResult.CandidateResponseMergeElapsed
		timing.reverseArtifactRowMergeElapsed = artifactTiming.rowMergeElapsed
		timing.reverseArtifactParentMergeElapsed = artifactTiming.parentMergeElapsed
		timing.reverseArtifactSortElapsed = artifactTiming.sortElapsed
		timing.reverseArtifactLocalMode = artifactTiming.mode
		timing.reverseArtifactTargetCandidateMode = artifactTiming.targetCandidateMode
		timing.reverseArtifactSourceElapsed = artifactTiming.sourceElapsed
		timing.reverseArtifactReadElapsed = artifactTiming.readElapsed
		timing.reverseArtifactRowConversionElapsed = artifactTiming.rowConversionElapsed
		timing.reverseArtifactMapConversionElapsed = artifactTiming.mapConversionElapsed
		timing.reverseArtifactNarrowElapsed = artifactTiming.narrowElapsed
		timing.reverseArtifactParentElapsed = artifactTiming.parentElapsed
		if artifactErr != nil || artifactDiagnostics.BlocksNative() {
			return nil, nil, timing, artifactDiagnostics, artifactErr
		}
		effectiveChildRows = narrowedRows
		timing.fkProjectionScope = "reverse_artifact_narrowed"
		if len(effectiveChildRows) == 0 {
			if domainCache := DomainMappingCacheFromContext(ctx); domainCache != nil {
				cacheSetStart := time.Now()
				domainCache.Set(domainCacheKey, parentRows, childRows, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum{})
				timing.reverseArtifactCacheSetElapsed = time.Since(cacheSetStart)
				recordQueryScratchpadCacheStore(ctx, "domain_mapping_cache", domainCacheDetail)
			}
			timing.reverseArtifactProjectMode = "skipped_empty"
			timing.childRetainCovered = true
			timing.childRetainMode = "empty_reverse_artifact"
			return nil, nil, timing, nil, nil
		}
		if len(artifactParentByChild) > 0 {
			joined := effectiveChildRows
			pairs := artifactPairs
			if len(pairs) == 0 {
				pairStart := time.Now()
				_, joined, pairs = legacyDirectRelationshipRowsFromParentMap(effectiveChildRows, artifactParentByChild)
				timing.pairElapsed = time.Since(pairStart)
			}
			timing.matchedRows = len(joined)
			timing.fkProjectionScope = "reverse_artifact_parent_map"
			timing.projectionRows = 0
			timing.reverseArtifactProjectMode = "skipped_parent_map"
			timing.childRetainCovered = true
			timing.childRetainMode = "reverse_artifact_parent_map"
			if domainCache := DomainMappingCacheFromContext(ctx); domainCache != nil {
				cacheSetStart := time.Now()
				domainCache.Set(domainCacheKey, parentRows, childRows, artifactParentByChild)
				timing.reverseArtifactCacheSetElapsed = time.Since(cacheSetStart)
				recordQueryScratchpadCacheStore(ctx, "domain_mapping_cache", domainCacheDetail)
			}
			return joined, pairs, timing, nil, nil
		}
		projectionIntersectStart := time.Now()
		effectiveProjectionRows = legacyDirectRelationshipIntersectRownums(projectionRows, narrowedRows)
		timing.reverseArtifactProjectElapsed = time.Since(projectionIntersectStart)
		timing.reverseArtifactProjectMode = "intersect"
		if len(effectiveProjectionRows) == 0 && len(narrowedRows) > 0 {
			effectiveProjectionRows = narrowedRows
		}
	}
	childFoundSet := legacyDirectRelationshipBitmap(effectiveChildRows)
	projectionStart := time.Now()
	fkBSI, projectionCacheHit, diagnostics, err := e.legacyDirectRelationshipProjectedFKBSI(ctx, request, edge, effectiveProjectionRows)
	timing.projectionElapsed = time.Since(projectionStart)
	timing.projectionCacheHit = projectionCacheHit
	if err != nil || diagnostics.BlocksNative() {
		return nil, nil, timing, diagnostics, err
	}
	if timing.fkProjectionScope == "" {
		timing.fkProjectionScope = "foundset"
	}
	timing.projectionRows = len(effectiveProjectionRows)
	timing.fkProjectionRows, timing.fkChildOverlapRows = legacyDirectRelationshipFKProjectionStats(fkBSI, childFoundSet)
	timing.fkProjectionCoverage = qsbridge.NewRelationshipVectorProjectionCoverage(len(effectiveChildRows), timing.fkProjectionRows, timing.fkChildOverlapRows)
	timing.fkProjectionInitialCoverage = timing.fkProjectionCoverage
	if timing.fkProjectionCoverage.NeedsRecovery() {
		fullFKBSI, fullCacheHit, fullDiagnostics, fullErr := e.legacyDirectRelationshipProjectedFullFKBSI(ctx, request, edge)
		if fullErr != nil || fullDiagnostics.BlocksNative() {
			return nil, nil, timing, fullDiagnostics, fullErr
		}
		fullRows, fullOverlap := legacyDirectRelationshipFKProjectionStats(fullFKBSI, childFoundSet)
		timing.fkProjectionRetryRows = fullRows
		timing.fkProjectionRetryOverlap = fullOverlap
		timing.fkProjectionRetryCoverage = qsbridge.NewRelationshipVectorProjectionCoverage(len(effectiveChildRows), fullRows, fullOverlap).WithRecoveryPolicy(qsbridge.RelationshipVectorProjectionRecoveryBroadenAndIntersect)
		if fullFKBSI != nil && fullOverlap > timing.fkChildOverlapRows {
			fkBSI = fullFKBSI
			projectionCacheHit = fullCacheHit
			timing.projectionCacheHit = fullCacheHit
			timing.fkProjectionRows = fullRows
			timing.fkChildOverlapRows = fullOverlap
			timing.fkProjectionCoverage = timing.fkProjectionRetryCoverage
			timing.fkProjectionScope = "full_retry"
		}
	}
	matchStart := time.Now()
	joined, pairs, projectedTiming, diagnostics := legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI, effectiveChildRows, parentKeyRows)
	timing.matchedRows = len(joined)
	if projectedTiming.batchEqualElapsed == 0 &&
		projectedTiming.singleKeyFoundSetElapsed == 0 &&
		projectedTiming.singleKeyEqualElapsed == 0 &&
		projectedTiming.valueVectorElapsed == 0 &&
		projectedTiming.intersectElapsed == 0 &&
		projectedTiming.rownumElapsed == 0 &&
		projectedTiming.pairElapsed == 0 {
		timing.batchEqualElapsed = time.Since(matchStart)
	} else {
		timing.batchEqualElapsed = projectedTiming.batchEqualElapsed
		timing.singleKeyFoundSetElapsed = projectedTiming.singleKeyFoundSetElapsed
		timing.singleKeyEqualElapsed = projectedTiming.singleKeyEqualElapsed
		timing.valueVectorElapsed = projectedTiming.valueVectorElapsed
		timing.intersectElapsed = projectedTiming.intersectElapsed
		timing.rownumElapsed = projectedTiming.rownumElapsed
		timing.pairElapsed = projectedTiming.pairElapsed
	}
	if diagnostics.BlocksNative() {
		return nil, nil, timing, diagnostics, nil
	}
	if domainCache := DomainMappingCacheFromContext(ctx); domainCache != nil {
		domainCache.Set(domainCacheKey, parentRows, childRows, legacyDirectRelationshipParentMapFromPairs(pairs))
		recordQueryScratchpadCacheStore(ctx, "domain_mapping_cache", domainCacheDetail)
	}
	return joined, pairs, timing, nil, nil
}

func legacyDirectRelationshipDomainMappingCacheKey(edge legacyDirectRelationshipEdge, fromTime, toTime int64) DomainMappingCacheKey {
	return DomainMappingCacheKey{
		SourceDomain:  edge.childKey(),
		TargetDomain:  edge.parentKey(),
		VectorIndex:   edge.childTable,
		VectorField:   edge.childField,
		Direction:     "child_to_parent",
		FromTimeNanos: fromTime,
		ToTimeNanos:   toTime,
	}
}

func legacyDirectRelationshipDomainMappingCacheDetail(key DomainMappingCacheKey, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum) string {
	return "source=" + key.SourceDomain +
		" target=" + key.TargetDomain +
		" vector=" + key.VectorIndex + "." + key.VectorField +
		" direction=" + key.Direction +
		" parent_rows=" + strconv.Itoa(len(parentRows)) +
		" child_rows=" + strconv.Itoa(len(childRows))
}

func legacyDirectRelationshipParentMapFromPairs(pairs []legacyDirectRelationshipPair) map[qsbridge.QuantaRownum]qsbridge.QuantaRownum {
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(pairs))
	for _, pair := range pairs {
		parentByChild[pair.child] = pair.parent
	}
	return parentByChild
}

func legacyDirectRelationshipFKProjectionStats(fkBSI *roaring64.BSI, childFoundSet *roaring64.Bitmap) (int, int) {
	if fkBSI == nil || fkBSI.GetExistenceBitmap() == nil {
		return 0, 0
	}
	existence := fkBSI.GetExistenceBitmap()
	overlap := existence.Clone()
	if childFoundSet != nil {
		overlap.And(childFoundSet)
	}
	return int(existence.GetCardinality()), int(overlap.GetCardinality())
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipParentKeyRows(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum) (map[int64]qsbridge.QuantaRownum, bool, qsbridge.DiagnosticSet, error) {
	parentRows = legacyDirectRelationshipUniqueRownums(parentRows)
	if len(parentRows) == 0 {
		return map[int64]qsbridge.QuantaRownum{}, false, nil, nil
	}
	parentKeyRows := make(map[int64]qsbridge.QuantaRownum, len(parentRows))
	for _, parentRow := range parentRows {
		parentKeyRows[int64(parentRow)] = parentRow
	}
	return parentKeyRows, false, nil, nil
}

func legacyDirectRelationshipParentKeyValues(parentKeyRows map[int64]qsbridge.QuantaRownum) []int64 {
	values := make([]int64, 0, len(parentKeyRows))
	for value := range parentKeyRows {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func legacyDirectRelationshipParentRowsFromKeyRows(parentKeyRows map[int64]qsbridge.QuantaRownum) []qsbridge.QuantaRownum {
	rows := make([]qsbridge.QuantaRownum, 0, len(parentKeyRows))
	for _, row := range parentKeyRows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
	return rows
}

func legacyDirectRelationshipParentMapFromArtifactValues(childRows []qsbridge.QuantaRownum, parentValueByChild map[qsbridge.QuantaRownum]int64, parentKeyRows map[int64]qsbridge.QuantaRownum) map[qsbridge.QuantaRownum]qsbridge.QuantaRownum {
	if len(childRows) == 0 || len(parentValueByChild) == 0 || len(parentKeyRows) == 0 {
		return nil
	}
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(childRows))
	for _, child := range childRows {
		parentValue, ok := parentValueByChild[child]
		if !ok {
			return nil
		}
		parentRow, ok := parentKeyRows[parentValue]
		if !ok {
			return nil
		}
		parentByChild[child] = parentRow
	}
	return parentByChild
}

func legacyDirectRelationshipRowsFromArtifactParentValues(childRows []qsbridge.QuantaRownum, parentValueByChild map[qsbridge.QuantaRownum]int64, parentKeyRows map[int64]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, bool) {
	if len(childRows) == 0 || len(parentValueByChild) == 0 || len(parentKeyRows) == 0 {
		return nil, nil, nil, false
	}
	narrowedRows := make([]qsbridge.QuantaRownum, 0, len(parentValueByChild))
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(parentValueByChild))
	pairs := make([]legacyDirectRelationshipPair, 0, len(parentValueByChild))
	for _, child := range childRows {
		parentValue, ok := parentValueByChild[child]
		if !ok {
			continue
		}
		parentRow, ok := parentKeyRows[parentValue]
		if !ok {
			return nil, nil, nil, false
		}
		narrowedRows = append(narrowedRows, child)
		parentByChild[child] = parentRow
		pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parentRow})
	}
	return narrowedRows, parentByChild, pairs, true
}

func legacyDirectRelationshipRowsFromSortedArtifactCandidates(childRows []qsbridge.QuantaRownum, candidateRows []qsbridge.QuantaRownum, parentValueByChild map[qsbridge.QuantaRownum]int64, parentKeyRows map[int64]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, bool) {
	if len(childRows) == 0 || len(candidateRows) == 0 || len(parentValueByChild) == 0 || len(parentKeyRows) == 0 {
		return nil, nil, nil, false
	}
	if len(candidateRows) >= len(childRows) ||
		!legacyDirectRelationshipRownumsAscending(childRows) ||
		!legacyDirectRelationshipRownumsAscending(candidateRows) {
		return nil, nil, nil, false
	}
	narrowedRows := make([]qsbridge.QuantaRownum, 0, len(candidateRows))
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(candidateRows))
	pairs := make([]legacyDirectRelationshipPair, 0, len(candidateRows))
	childIndex := 0
	for _, candidate := range candidateRows {
		for childIndex < len(childRows) && childRows[childIndex] < candidate {
			childIndex++
		}
		if childIndex >= len(childRows) {
			break
		}
		if childRows[childIndex] != candidate {
			continue
		}
		parentValue, ok := parentValueByChild[candidate]
		if !ok {
			return nil, nil, nil, false
		}
		parentRow, ok := parentKeyRows[parentValue]
		if !ok {
			return nil, nil, nil, false
		}
		narrowedRows = append(narrowedRows, candidate)
		parentByChild[candidate] = parentRow
		pairs = append(pairs, legacyDirectRelationshipPair{child: candidate, parent: parentRow})
	}
	return narrowedRows, parentByChild, pairs, true
}

func legacyDirectRelationshipRownumsAscending(rownums []qsbridge.QuantaRownum) bool {
	for i := 1; i < len(rownums); i++ {
		if rownums[i] < rownums[i-1] {
			return false
		}
	}
	return true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipReverseArtifactChildRows(ctx context.Context, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum, options legacyDirectRelationshipReduceOptions) ([]qsbridge.QuantaRownum, qsbridge.FilterDomainRelationshipVectorResult, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, legacyDirectRelationshipReverseArtifactLocalTiming, bool, qsbridge.DiagnosticSet, error) {
	var localTiming legacyDirectRelationshipReverseArtifactLocalTiming
	if e.ReverseArtifactCandidateReader == nil {
		localTiming.mode = "skip_nil_reader"
		return nil, qsbridge.FilterDomainRelationshipVectorResult{}, nil, nil, localTiming, false, nil, nil
	}
	if len(childRows) == 0 {
		localTiming.mode = "skip_empty_child_rows"
		return nil, qsbridge.FilterDomainRelationshipVectorResult{}, nil, nil, localTiming, false, nil, nil
	}
	if len(parentKeyRows) == 0 {
		localTiming.mode = "skip_empty_parent_keys"
		return nil, qsbridge.FilterDomainRelationshipVectorResult{}, nil, nil, localTiming, false, nil, nil
	}
	sourceStart := time.Now()
	sourceValues := legacyDirectRelationshipParentKeyValues(parentKeyRows)
	localTiming.sourceElapsed = time.Since(sourceStart)
	if len(sourceValues) == 0 {
		localTiming.mode = "skip_empty_source_values"
		return nil, qsbridge.FilterDomainRelationshipVectorResult{}, nil, nil, localTiming, false, nil, nil
	}
	backend := LegacyDirectBitIndexRelationshipVectorBackend{
		TableCache:                     e.TableCache,
		ReverseArtifactCandidateReader: e.ReverseArtifactCandidateReader,
	}
	readStart := time.Now()
	read := legacyDirectRelationshipTupleMembershipParentToChildReadRequest(edge, legacyDirectRelationshipParentRowsFromKeyRows(parentKeyRows))
	read.MaxEstimatedTargetRows = len(childRows)
	if options.omitFullDomainTargetCandidates {
		localTiming.targetCandidateMode = "omitted_full_domain"
		read.PreserveArtifactOrder = true
	} else {
		localTiming.targetCandidateMode = "retained"
		read.TargetCandidateRows = append([]qsbridge.QuantaRownum(nil), childRows...)
	}
	projectionKey := backend.relationshipVectorProjectionCacheKey(read)
	localTiming.readElapsed = time.Since(readStart)
	start := time.Now()
	candidates, parentValueByChild, artifactTiming, diagnostics, err, ok := backend.readRelationshipVectorReverseArtifactCandidates(ctx, projectionKey, read, sourceValues)
	elapsed := time.Since(start)
	localTiming.sourceValues = artifactTiming.SourceValues
	localTiming.candidateRows = artifactTiming.TargetRows
	localTiming.elapsed = elapsed
	localTiming.lookupElapsed = artifactTiming.LookupElapsed
	localTiming.fanoutElapsed = artifactTiming.FanoutElapsed
	localTiming.clientRPCElapsed = artifactTiming.ClientRPCElapsed
	localTiming.maxClientRPCElapsed = artifactTiming.MaxClientRPCElapsed
	localTiming.responseMergeElapsed = artifactTiming.ResponseMergeElapsed
	localTiming.rowMergeElapsed = artifactTiming.RowMergeElapsed
	localTiming.parentMergeElapsed = artifactTiming.ParentMergeElapsed
	localTiming.sortElapsed = artifactTiming.SortElapsed
	localTiming.rowConversionElapsed = artifactTiming.RowConversionElapsed
	localTiming.mapConversionElapsed = artifactTiming.MapConversionElapsed
	if !ok {
		localTiming.mode = artifactTiming.Mode
		if localTiming.mode == "" {
			localTiming.mode = "skip_no_artifact"
		}
		return nil, qsbridge.FilterDomainRelationshipVectorResult{}, nil, nil, localTiming, false, diagnostics, err
	}
	result := qsbridge.FilterDomainRelationshipVectorResult{
		TargetCandidates:              candidates,
		VectorIndex:                   read.VectorIndex,
		VectorField:                   read.VectorField,
		Direction:                     read.Direction,
		SourceValueCount:              artifactTiming.SourceValues,
		CandidateCacheHit:             artifactTiming.CacheHit,
		CandidateCacheMode:            "reverse_artifact",
		CandidateMode:                 artifactTiming.Mode,
		CandidateElapsed:              elapsed,
		CandidateScanElapsed:          artifactTiming.LookupElapsed,
		CandidateFanoutElapsed:        artifactTiming.FanoutElapsed,
		CandidateClientRPCElapsed:     artifactTiming.ClientRPCElapsed,
		CandidateClientRPCMaxElapsed:  artifactTiming.MaxClientRPCElapsed,
		CandidateResponseMergeElapsed: artifactTiming.ResponseMergeElapsed,
	}
	if err != nil || diagnostics.BlocksNative() {
		return nil, result, nil, nil, localTiming, true, diagnostics, err
	}
	narrowStart := time.Now()
	if localTiming.targetCandidateMode == "omitted_full_domain" && len(parentValueByChild) >= len(candidates.Rownums) {
		narrowedRows, parentByChild, pairs, ok := legacyDirectRelationshipRowsFromArtifactCandidateRows(candidates.Rownums, parentValueByChild, parentKeyRows)
		localTiming.narrowElapsed = time.Since(narrowStart)
		if ok {
			localTiming.mode = "omitted_target_candidate_rows"
			return narrowedRows, result, parentByChild, pairs, localTiming, true, diagnostics, nil
		}
	}
	if len(parentValueByChild) >= len(candidates.Rownums) {
		narrowedRows, parentByChild, pairs, ok := legacyDirectRelationshipRowsFromSortedArtifactCandidates(childRows, candidates.Rownums, parentValueByChild, parentKeyRows)
		localTiming.narrowElapsed = time.Since(narrowStart)
		if ok {
			localTiming.mode = "sorted_candidate_single_pass"
			return narrowedRows, result, parentByChild, pairs, localTiming, true, diagnostics, nil
		}
	}
	if len(parentValueByChild) >= len(candidates.Rownums) {
		narrowedRows, parentByChild, pairs, ok := legacyDirectRelationshipRowsFromArtifactParentValues(childRows, parentValueByChild, parentKeyRows)
		localTiming.narrowElapsed = time.Since(narrowStart)
		if ok {
			localTiming.mode = "parent_value_single_pass"
			return narrowedRows, result, parentByChild, pairs, localTiming, true, diagnostics, nil
		}
	}
	narrowedRows := legacyDirectRelationshipIntersectRownums(childRows, candidates.Rownums)
	localTiming.narrowElapsed = time.Since(narrowStart)
	parentStart := time.Now()
	parentByChild := legacyDirectRelationshipParentMapFromArtifactValues(narrowedRows, parentValueByChild, parentKeyRows)
	localTiming.parentElapsed = time.Since(parentStart)
	localTiming.mode = "candidate_intersect_parent_map"
	return narrowedRows, result, parentByChild, nil, localTiming, true, diagnostics, nil
}

func legacyDirectRelationshipRowsFromArtifactCandidateRows(candidateRows []qsbridge.QuantaRownum, parentValueByChild map[qsbridge.QuantaRownum]int64, parentKeyRows map[int64]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, bool) {
	if len(candidateRows) == 0 || len(parentValueByChild) == 0 || len(parentKeyRows) == 0 {
		return nil, nil, nil, false
	}
	rows := candidateRows
	parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(rows))
	pairs := make([]legacyDirectRelationshipPair, 0, len(rows))
	for _, child := range rows {
		parentValue, ok := parentValueByChild[child]
		if !ok {
			return nil, nil, nil, false
		}
		parentRow, ok := parentKeyRows[parentValue]
		if !ok {
			return nil, nil, nil, false
		}
		parentByChild[child] = parentRow
		pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parentRow})
	}
	return rows, parentByChild, pairs, true
}

func legacyDirectRelationshipReduceProjectedFKBSI(fkBSI *roaring64.BSI, childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, qsbridge.DiagnosticSet) {
	joined, pairs, _, diagnostics := legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI, childRows, parentKeyRows)
	return joined, pairs, diagnostics
}

func legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI *roaring64.BSI, childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, legacyDirectRelationshipProjectedFKReduceTiming, qsbridge.DiagnosticSet) {
	var timing legacyDirectRelationshipProjectedFKReduceTiming
	if fkBSI == nil {
		return nil, nil, timing, legacyDirectRelationshipDiagnostic("relationship-vector FK projection returned nil BSI")
	}
	if len(childRows) == 0 || len(parentKeyRows) == 0 {
		return nil, nil, timing, nil
	}
	if legacyDirectRelationshipShouldUseSingleKeyEqualReduce(childRows, parentKeyRows) {
		parentKey, parentRow := legacyDirectRelationshipSingleParentKey(parentKeyRows)
		timing.singleKeyEqualUsed = true

		foundSetStart := time.Now()
		childFoundSet := legacyDirectRelationshipBitmap(childRows)
		timing.singleKeyFoundSetElapsed = time.Since(foundSetStart)

		equalStart := time.Now()
		matched := fkBSI.CompareValue(0, roaring64.EQ, parentKey, 0, childFoundSet)
		if matched == nil {
			matched = roaring64.NewBitmap()
		}
		timing.singleKeyEqualElapsed = time.Since(equalStart)

		rownumStart := time.Now()
		joined := make([]qsbridge.QuantaRownum, 0, int(matched.GetCardinality()))
		for _, child := range childRows {
			if matched.Contains(uint64(child)) {
				joined = append(joined, child)
			}
		}
		timing.rownumElapsed = time.Since(rownumStart)

		pairStart := time.Now()
		pairs := make([]legacyDirectRelationshipPair, 0, len(joined))
		for _, child := range joined {
			pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parentRow})
		}
		timing.pairElapsed = time.Since(pairStart)
		return joined, pairs, timing, nil
	}
	if legacyDirectRelationshipShouldUseBatchEqualReduce(childRows, parentKeyRows) {
		timing.batchEqualUsed = true
		childFoundSet := legacyDirectRelationshipBitmap(childRows)
		parentKeyValues := legacyDirectRelationshipParentKeyValues(parentKeyRows)
		batchStart := time.Now()
		valuePairs := fkBSI.BatchEqualValues(0, parentKeyValues, childFoundSet)
		timing.batchEqualElapsed = time.Since(batchStart)

		pairStart := time.Now()
		parentByChild := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(valuePairs))
		for _, valuePair := range valuePairs {
			parent, ok := parentKeyRows[valuePair.Value]
			if !ok {
				continue
			}
			parentByChild[qsbridge.QuantaRownum(valuePair.ColumnID)] = parent
		}
		joined := make([]qsbridge.QuantaRownum, 0, len(parentByChild))
		pairs := make([]legacyDirectRelationshipPair, 0, len(parentByChild))
		for _, child := range childRows {
			parent, ok := parentByChild[child]
			if !ok {
				continue
			}
			joined = append(joined, child)
			pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parent})
		}
		timing.pairElapsed = time.Since(pairStart)
		return joined, pairs, timing, nil
	}
	if legacyDirectRelationshipShouldUseValueVectorReduce(childRows, parentKeyRows) {
		timing.valueVectorUsed = true
		valueStart := time.Now()
		parentKeys := fkBSI.GetBigValues(nativeProjectionRownumColumnIDs(childRows))
		timing.valueVectorElapsed = time.Since(valueStart)

		joined := make([]qsbridge.QuantaRownum, 0, len(childRows))
		pairs := make([]legacyDirectRelationshipPair, 0, len(childRows))
		pairStart := time.Now()
		for i, child := range childRows {
			if i >= len(parentKeys) || parentKeys[i] == nil {
				continue
			}
			parent, ok := parentKeyRows[parentKeys[i].Int64()]
			if !ok {
				continue
			}
			joined = append(joined, child)
			pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parent})
		}
		timing.pairElapsed = time.Since(pairStart)
		return joined, pairs, timing, nil
	}
	joined := make([]qsbridge.QuantaRownum, 0, len(childRows))
	pairs := make([]legacyDirectRelationshipPair, 0, len(childRows))
	pairStart := time.Now()
	for _, child := range childRows {
		parentKey, ok := fkBSI.GetValue(uint64(child))
		if !ok {
			continue
		}
		parent, ok := parentKeyRows[parentKey]
		if !ok {
			continue
		}
		joined = append(joined, child)
		pairs = append(pairs, legacyDirectRelationshipPair{child: child, parent: parent})
	}
	timing.pairElapsed = time.Since(pairStart)
	return joined, pairs, timing, nil
}

func legacyDirectRelationshipShouldUseBatchEqualReduce(childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum) bool {
	return len(childRows) >= 1024 && len(parentKeyRows) > 1 && len(parentKeyRows) <= 32
}

func legacyDirectRelationshipShouldUseValueVectorReduce(childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum) bool {
	return len(childRows) >= 1024 && len(parentKeyRows) > 32
}

func legacyDirectRelationshipShouldUseSingleKeyEqualReduce(childRows []qsbridge.QuantaRownum, parentKeyRows map[int64]qsbridge.QuantaRownum) bool {
	return len(childRows) >= 1024 && len(parentKeyRows) == 1
}

func legacyDirectRelationshipSingleParentKey(parentKeyRows map[int64]qsbridge.QuantaRownum) (int64, qsbridge.QuantaRownum) {
	for parentKey, parentRow := range parentKeyRows {
		return parentKey, parentRow
	}
	return 0, 0
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipProjectionResult(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, joined []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair, result ExecutionResult, optionalParentRows ...[]qsbridge.QuantaRownum) (ExecutionResult, error) {
	projectionFields, diagnostics := legacyDirectRelationshipVisibleProjectionFields(request)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	if len(projectionFields) == 0 {
		result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
		return result, nil
	}
	var parentRows []qsbridge.QuantaRownum
	if len(optionalParentRows) > 0 {
		parentRows = optionalParentRows[0]
	}
	joined, pairs = legacyDirectRelationshipProjectionOuterRows(edge, joined, pairs, parentRows)
	var limitPushed bool
	if len(request.OrderBy) == 0 && !directBitmapHasResidualScanPredicates(request) && !request.Result.Distinct {
		joined, pairs, limitPushed = legacyDirectRelationshipPushProjectionLimit(request, joined, pairs)
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("projection_limit_pushed", strconv.FormatBool(limitPushed)))
	materialization := e.projectionMaterializationKernel()
	childFields := legacyDirectRelationshipFieldsForIndex(projectionFields, edge.childTable)
	parentFields := legacyDirectRelationshipFieldsForIndex(projectionFields, edge.parentTable)
	result.Probes = append(result.Probes, legacyDirectRelationshipMaterializationProbes("child", joined, childFields)...)
	childMaterializationStart := time.Now()
	childValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.childTable, joined, childFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.childTable, edge.childRole))
	childMaterializationElapsed := time.Since(childMaterializationStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_child_materialization_elapsed", childMaterializationElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	parentRownums := legacyDirectRelationshipParentRownums(pairs)
	result.Probes = append(result.Probes, legacyDirectRelationshipMaterializationProbes("parent", parentRownums, parentFields)...)
	parentMaterializationStart := time.Now()
	parentValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.parentTable, parentRownums, parentFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.parentTable, edge.parentRole))
	parentMaterializationElapsed := time.Since(parentMaterializationStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_parent_materialization_elapsed", parentMaterializationElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	assembleStart := time.Now()
	rowSet, diagnostics := legacyDirectRelationshipAssembleProjectedRows(edge, pairs, projectionFields, childValues, parentValues)
	assembleElapsed := time.Since(assembleStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_assemble_rows_elapsed", assembleElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	residualStart := time.Now()
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(request, rowSet)
	residualElapsed := time.Since(residualStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_projection_residual_filter_elapsed", residualElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rowSet, orderDiagnostics := directBitmapOrderProjectedRows(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, orderDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rowSet, projectionDiagnostics := directBitmapEvaluateProjectionRowSet(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, projectionDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	if request.Result.Distinct {
		rowSet = directBitmapDistinctProjectedRowSet(rowSet)
	}
	if !limitPushed {
		rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit)
	}
	if len(request.Projection) == 0 {
		rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, request.ProjectionOrder)
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return result, nil
}

func legacyDirectRelationshipProjectionOuterRows(edge legacyDirectRelationshipEdge, joined []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair, parentRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair) {
	if edge.sqlKind != qsbridge.JoinKindLeftOuter || !edge.leftOuterPreservesParent || len(parentRows) == 0 {
		return joined, pairs
	}
	extendedPairs := legacyDirectRelationshipLeftOuterPairs(parentRows, pairs)
	extendedJoined := legacyDirectRelationshipChildRownums(extendedPairs)
	return extendedJoined, extendedPairs
}

func legacyDirectRelationshipLeftOuterPairs(parentRows []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) []legacyDirectRelationshipPair {
	matchedParents := make(map[qsbridge.QuantaRownum]struct{}, len(pairs))
	for _, pair := range pairs {
		matchedParents[pair.parent] = struct{}{}
	}
	extendedPairs := append([]legacyDirectRelationshipPair(nil), pairs...)
	for _, parent := range parentRows {
		if _, ok := matchedParents[parent]; ok {
			continue
		}
		extendedPairs = append(extendedPairs, legacyDirectRelationshipPair{parent: parent})
	}
	return extendedPairs
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipLeftOuterAggregateResult(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, parentRows []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair, result ExecutionResult) (ExecutionResult, error) {
	if !directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("left outer relationship-vector execution only supports count(*) aggregates in this slice")...)
		return result, nil
	}
	whereResiduals := legacyDirectRelationshipResidualPredicatesForScope(request, qsbridge.PredicateScopeWhere)
	if len(whereResiduals) > 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("left outer relationship-vector execution does not support residual WHERE predicates in this slice")...)
		return result, nil
	}
	onResiduals := legacyDirectRelationshipResidualPredicatesForScope(request, qsbridge.PredicateScopeOn)
	filteredPairs, diagnostics, err := e.legacyDirectRelationshipFilterPairsByPredicates(ctx, request, edge, pairs, onResiduals)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	outerPairs := legacyDirectRelationshipLeftOuterPairs(parentRows, filteredPairs)
	outerCount := len(outerPairs)
	result.Count = uint64(outerCount)
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("left_outer_parent_rows", strconv.Itoa(len(parentRows))),
		legacyDirectRelationshipProbe("left_outer_matched_pairs", strconv.Itoa(len(filteredPairs))),
		legacyDirectRelationshipProbe("left_outer_unmatched_parents", strconv.Itoa(outerCount-len(filteredPairs))),
	)
	if len(request.GroupBy) > 0 {
		outerJoined := legacyDirectRelationshipChildRownums(outerPairs)
		return e.legacyDirectRelationshipAggregateResult(ctx, request, edge, outerJoined, outerPairs, result)
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return directBitmapCountAggregateResult(request, result), nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipFilterPairsByPredicates(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, pairs []legacyDirectRelationshipPair, predicates []qsbridge.Predicate) ([]legacyDirectRelationshipPair, qsbridge.DiagnosticSet, error) {
	if len(predicates) == 0 || len(pairs) == 0 {
		return append([]legacyDirectRelationshipPair(nil), pairs...), nil, nil
	}
	materialization := e.projectionMaterializationKernel()
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipFieldsForEdge(fields, edge)
	fields = legacyDirectRelationshipPostReductionFields(request, fields)
	childFields := legacyDirectRelationshipFieldsForIndex(fields, edge.childTable)
	childValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.childTable, legacyDirectRelationshipChildRownums(pairs), childFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.childTable, edge.childRole))
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	parentFields := legacyDirectRelationshipFieldsForIndex(fields, edge.parentTable)
	parentValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.parentTable, legacyDirectRelationshipParentRownums(pairs), parentFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.parentTable, edge.parentRole))
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rowSet, diagnostics := legacyDirectRelationshipAssembleProjectedRows(edge, pairs, fields, childValues, parentValues)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	filtered := make([]legacyDirectRelationshipPair, 0, len(pairs))
	for i, pair := range pairs {
		matched, diagnostics := directBitmapEvaluateResidualPredicates(predicates, rowSet, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics, nil
		}
		if matched {
			filtered = append(filtered, pair)
		}
	}
	return filtered, nil, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipAggregateResult(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, joined []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair, result ExecutionResult) (ExecutionResult, error) {
	if len(request.GroupBy) == 0 && directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) && !directBitmapHasResidualScanPredicates(request) && request.NativePredicates.Empty() {
		result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
		return directBitmapCountAggregateResult(request, result), nil
	}
	materialization := e.projectionMaterializationKernel()
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipFieldsForEdge(fields, edge)
	fields = legacyDirectRelationshipPostReductionFields(request, fields)
	childFields := legacyDirectRelationshipFieldsForIndex(fields, edge.childTable)
	parentFields := legacyDirectRelationshipFieldsForIndex(fields, edge.parentTable)
	result.Probes = append(result.Probes, legacyDirectRelationshipMaterializationProbes("child", joined, childFields)...)
	childMaterializationStart := time.Now()
	childValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.childTable, joined, childFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.childTable, edge.childRole))
	childMaterializationElapsed := time.Since(childMaterializationStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_child_materialization_elapsed", childMaterializationElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	parentRownums := legacyDirectRelationshipParentRownums(pairs)
	result.Probes = append(result.Probes, legacyDirectRelationshipMaterializationProbes("parent", parentRownums, parentFields)...)
	parentMaterializationStart := time.Now()
	parentValues, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, edge.parentTable, parentRownums, parentFields, e.legacyDirectRelationshipTimeMaterializationForRole(request, edge.parentTable, edge.parentRole))
	parentMaterializationElapsed := time.Since(parentMaterializationStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_parent_materialization_elapsed", parentMaterializationElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	assembleStart := time.Now()
	materialized, diagnostics := legacyDirectRelationshipAssembleProjectedRows(edge, pairs, fields, childValues, parentValues)
	assembleElapsed := time.Since(assembleStart)
	result.Probes = append(result.Probes, legacyDirectRelationshipProbe("phase_assemble_rows_elapsed", assembleElapsed.String()))
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	residuals := directBitmapResidualScanPredicates(request)
	residualStart := time.Now()
	filtered, diagnostics := directBitmapFilterResidualScanPredicates(request, materialized)
	residualElapsed := time.Since(residualStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}
	rowsBeforeResidual := materialized.CandidateCount()
	rowsAfterResidual := filtered.CandidateCount()
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("aggregate_residual_predicates", strconv.Itoa(len(residuals))),
		legacyDirectRelationshipProbe("aggregate_residual_rows_before", strconv.Itoa(rowsBeforeResidual)),
		legacyDirectRelationshipProbe("aggregate_residual_rows_after", strconv.Itoa(rowsAfterResidual)),
		legacyDirectRelationshipProbe("aggregate_residual_rows_removed", strconv.Itoa(rowsBeforeResidual-rowsAfterResidual)),
		legacyDirectRelationshipProbe("phase_aggregate_residual_filter_elapsed", residualElapsed.String()),
	)
	if len(request.GroupBy) > 0 {
		result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
		return directBitmapMaterializedGroupedAggregateResult(request, filtered, result), nil
	}
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return directBitmapMaterializedAggregateResult(request, filtered, result), nil
}

func relationshipTupleAggregateAlias(request ExecutionRequest) string {
	if len(request.SQLAggregates) == 0 {
		return ""
	}
	return request.SQLAggregates[0].Alias
}

func legacyDirectRelationshipProbe(name string, value string) ExecutionProbe {
	return ExecutionProbe{Section: "relationship_join", Name: name, Value: value}
}

func (s *legacyDirectRelationshipGraphReductionSummary) record(inputOrdinal, iteration, edgeOrdinal int, edge legacyDirectRelationshipEdge, parentRows, childRows, joinedRows int, reduceElapsed time.Duration, timing legacyDirectRelationshipReduceTiming, childRetainElapsed time.Duration, childRetainRows int) {
	if s == nil {
		return
	}
	label := legacyDirectRelationshipGraphReductionEdgeLabel(inputOrdinal, iteration, edgeOrdinal, edge, parentRows, childRows)
	s.edges++
	s.totalReduceElapsed += reduceElapsed
	s.totalProjectionElapsed += timing.projectionElapsed
	s.totalParentKeyElapsed += timing.parentKeyElapsed
	s.totalReverseArtifactElapsed += timing.reverseArtifactElapsed
	s.totalReverseArtifactRPC += timing.reverseArtifactClientRPCElapsed
	s.totalReverseArtifactRPCMax += timing.reverseArtifactClientRPCMaxElapsed
	s.totalValueVectorElapsed += timing.valueVectorElapsed
	s.totalBatchEqualElapsed += timing.batchEqualElapsed
	s.totalIntersectElapsed += timing.intersectElapsed
	s.totalPairElapsed += timing.pairElapsed
	s.totalChildRetainElapsed += childRetainElapsed
	s.totalProjectionRows += timing.projectionRows
	s.totalParentRows += parentRows
	s.totalChildRows += childRows
	s.totalJoinedRows += joinedRows
	s.totalReverseArtifactSource += timing.reverseArtifactSourceValues
	s.totalReverseArtifactCandidate += timing.reverseArtifactCandidateRows
	s.totalReverseArtifactNarrowed += timing.reverseArtifactNarrowedRows
	s.totalMatchedRows += timing.matchedRows
	if reduceElapsed > s.maxReduceElapsed {
		s.maxReduceElapsed = reduceElapsed
		s.maxReduceLabel = label
	}
	if timing.projectionElapsed > s.maxProjectionElapsed {
		s.maxProjectionElapsed = timing.projectionElapsed
		s.maxProjectionLabel = label
	}
	if timing.reverseArtifactElapsed > s.maxReverseArtifactElapsed {
		s.maxReverseArtifactElapsed = timing.reverseArtifactElapsed
		s.maxReverseArtifactLabel = label
	}
	if childRetainElapsed > s.maxChildRetainElapsed {
		s.maxChildRetainElapsed = childRetainElapsed
		s.maxChildRetainLabel = label
	}
	s.edgeSummaries = append(s.edgeSummaries, fmt.Sprintf(
		"%s joined=%d retained=%d reduce=%s projection=%s parent_key=%s reverse_artifact=%s rpc=%s rpc_max=%s value_vector=%s intersect=%s pair=%s retain=%s matched=%d reverse_source=%d reverse_candidate=%d reverse_narrowed=%d",
		label,
		joinedRows,
		childRetainRows,
		reduceElapsed,
		timing.projectionElapsed,
		timing.parentKeyElapsed,
		timing.reverseArtifactElapsed,
		timing.reverseArtifactClientRPCElapsed,
		timing.reverseArtifactClientRPCMaxElapsed,
		timing.valueVectorElapsed,
		timing.intersectElapsed,
		timing.pairElapsed,
		childRetainElapsed,
		timing.matchedRows,
		timing.reverseArtifactSourceValues,
		timing.reverseArtifactCandidateRows,
		timing.reverseArtifactNarrowedRows,
	))
}

func (s legacyDirectRelationshipGraphReductionSummary) probes() []ExecutionProbe {
	if s.edges == 0 {
		return nil
	}
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_reduction_edges_evaluated", strconv.Itoa(s.edges)),
		legacyDirectRelationshipProbe("graph_reduction_parent_rows_seen", strconv.Itoa(s.totalParentRows)),
		legacyDirectRelationshipProbe("graph_reduction_child_rows_seen", strconv.Itoa(s.totalChildRows)),
		legacyDirectRelationshipProbe("graph_reduction_joined_rows_seen", strconv.Itoa(s.totalJoinedRows)),
		legacyDirectRelationshipProbe("graph_reduction_projection_rows", strconv.Itoa(s.totalProjectionRows)),
		legacyDirectRelationshipProbe("graph_reduction_reverse_artifact_source_values", strconv.Itoa(s.totalReverseArtifactSource)),
		legacyDirectRelationshipProbe("graph_reduction_reverse_artifact_candidate_rows", strconv.Itoa(s.totalReverseArtifactCandidate)),
		legacyDirectRelationshipProbe("graph_reduction_reverse_artifact_narrowed_rows", strconv.Itoa(s.totalReverseArtifactNarrowed)),
		legacyDirectRelationshipProbe("graph_reduction_matched_rows", strconv.Itoa(s.totalMatchedRows)),
		legacyDirectRelationshipProbe("phase_graph_reduction_edge_reduce_total_elapsed", s.totalReduceElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_edge_projection_total_elapsed", s.totalProjectionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_parent_key_total_elapsed", s.totalParentKeyElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_reverse_artifact_total_elapsed", s.totalReverseArtifactElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_reverse_artifact_rpc_total_elapsed", s.totalReverseArtifactRPC.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_reverse_artifact_rpc_max_sum_elapsed", s.totalReverseArtifactRPCMax.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_value_vector_total_elapsed", s.totalValueVectorElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_batch_equal_total_elapsed", s.totalBatchEqualElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_intersect_total_elapsed", s.totalIntersectElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_pair_total_elapsed", s.totalPairElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_child_retain_total_elapsed", s.totalChildRetainElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_reduction_max_edge_reduce_elapsed", s.maxReduceElapsed.String()),
		legacyDirectRelationshipProbe("graph_reduction_max_edge_reduce", s.maxReduceLabel),
		legacyDirectRelationshipProbe("phase_graph_reduction_max_edge_projection_elapsed", s.maxProjectionElapsed.String()),
		legacyDirectRelationshipProbe("graph_reduction_max_edge_projection", s.maxProjectionLabel),
		legacyDirectRelationshipProbe("phase_graph_reduction_max_reverse_artifact_elapsed", s.maxReverseArtifactElapsed.String()),
		legacyDirectRelationshipProbe("graph_reduction_max_reverse_artifact", s.maxReverseArtifactLabel),
		legacyDirectRelationshipProbe("phase_graph_reduction_max_child_retain_elapsed", s.maxChildRetainElapsed.String()),
		legacyDirectRelationshipProbe("graph_reduction_max_child_retain", s.maxChildRetainLabel),
	}
	for i, summary := range s.edgeSummaries {
		probes = append(probes, legacyDirectRelationshipProbe(fmt.Sprintf("graph_reduction_edge_summary_%d", i+1), summary))
	}
	return probes
}

func legacyDirectRelationshipGraphReductionEdgeLabel(inputOrdinal, iteration, edgeOrdinal int, edge legacyDirectRelationshipEdge, parentRows, childRows int) string {
	return fmt.Sprintf("iter=%d edge=%d input=%d %s:%s[%d] -> %s:%s[%d]",
		iteration,
		edgeOrdinal,
		inputOrdinal,
		edge.parentKey(),
		edge.parentTable,
		parentRows,
		edge.childKey(),
		edge.childTable,
		childRows,
	)
}

func legacyDirectRelationshipProbeDuration(probes []ExecutionProbe, name string) time.Duration {
	for _, probe := range probes {
		if probe.Section != "relationship_join" || probe.Name != name {
			continue
		}
		duration, err := time.ParseDuration(probe.Value)
		if err == nil {
			return duration
		}
	}
	return 0
}

func legacyDirectRelationshipPrefixedProbes(prefix string, probes []ExecutionProbe) []ExecutionProbe {
	if len(probes) == 0 {
		return nil
	}
	prefixed := make([]ExecutionProbe, 0, len(probes))
	for _, probe := range probes {
		probe.Name = prefix + probe.Name
		prefixed = append(prefixed, probe)
	}
	return prefixed
}

func legacyDirectRelationshipInitialReadSeed(usedCandidateSet bool, rowCount int) string {
	if rowCount == 0 {
		return ""
	}
	if usedCandidateSet {
		return "relationship_vector"
	}
	return "query_fragment"
}

func legacyDirectRelationshipChildInitialReadSeed(usedCandidateSet bool, rowCount int) string {
	if rowCount == 0 {
		return ""
	}
	if usedCandidateSet {
		return "candidate_set"
	}
	return "query_fragment"
}

func legacyDirectRelationshipNodeInteractionSummaryProbes(probes []ExecutionProbe) []ExecutionProbe {
	initialRowReads := 0
	vectorProjectionReads := 0
	materializationReads := 0
	for _, probe := range probes {
		if probe.Section != "relationship_join" {
			continue
		}
		switch {
		case strings.HasPrefix(probe.Name, "graph_initial_rows_") && strings.HasSuffix(probe.Name, "_seed"):
			if probe.Value != "" && probe.Value != "candidate_set" {
				initialRowReads++
			}
		case strings.HasPrefix(probe.Name, "single_") && strings.HasSuffix(probe.Name, "_rows_seed"):
			if probe.Value != "" && probe.Value != "candidate_set" {
				initialRowReads++
			}
		case strings.HasSuffix(probe.Name, "_projection_cache_hit"):
			if probe.Value == "false" {
				vectorProjectionReads++
			}
		case strings.HasPrefix(probe.Name, "residual_prefilter_") && strings.HasSuffix(probe.Name, "_fields"):
			if legacyDirectRelationshipProbeInt(probe.Value) > 0 {
				materializationReads++
			}
		case strings.HasSuffix(probe.Name, "_materialization_fields"):
			if legacyDirectRelationshipProbeInt(probe.Value) > 0 {
				materializationReads++
			}
		}
	}
	total := initialRowReads + vectorProjectionReads + materializationReads
	return []ExecutionProbe{
		legacyDirectRelationshipProbe("node_interaction_estimate_initial_row_reads", strconv.Itoa(initialRowReads)),
		legacyDirectRelationshipProbe("node_interaction_estimate_vector_projection_reads", strconv.Itoa(vectorProjectionReads)),
		legacyDirectRelationshipProbe("node_interaction_estimate_materialization_reads", strconv.Itoa(materializationReads)),
		legacyDirectRelationshipProbe("node_interaction_estimate_total_reads", strconv.Itoa(total)),
	}
}

func legacyDirectRelationshipProbeInt(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func legacyDirectRelationshipPushProjectionLimit(request ExecutionRequest, joined []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, bool) {
	if len(request.OrderBy) > 0 || (request.Result.Offset <= 0 && request.Result.Limit <= 0) {
		return joined, pairs, false
	}
	start := request.Result.Offset
	if start < 0 {
		start = 0
	}
	if start > len(pairs) {
		start = len(pairs)
	}
	end := len(pairs)
	if request.Result.Limit > 0 && start+request.Result.Limit < end {
		end = start + request.Result.Limit
	}
	return append([]qsbridge.QuantaRownum(nil), joined[start:end]...), append([]legacyDirectRelationshipPair(nil), pairs[start:end]...), true
}

func legacyDirectRelationshipMaterializationProbes(side string, rownums []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField) []ExecutionProbe {
	prefix := side + "_materialization_"
	return []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"rows", strconv.Itoa(len(rownums))),
		legacyDirectRelationshipProbe(prefix+"unique_rows", strconv.Itoa(len(legacyDirectRelationshipUniqueRownums(rownums)))),
		legacyDirectRelationshipProbe(prefix+"fields", strconv.Itoa(len(fields))),
	}
}

func legacyDirectRelationshipGraphProjectionMaterializationFields(request ExecutionRequest, visible []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	fields = legacyDirectRelationshipPostReductionFields(request, fields)
	result := make([]qsbridge.QuantaProjectionField, 0, len(visible)+len(fields))
	seen := make(map[string]struct{}, len(visible)+len(fields))
	add := func(field qsbridge.QuantaProjectionField) {
		key := legacyDirectRelationshipProjectionFieldKey(field)
		if key == "" {
			return
		}
		key = strings.ToLower(key)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		result = append(result, field)
	}
	for _, field := range visible {
		add(field)
	}
	for _, field := range fields {
		add(field)
	}
	return result
}

func legacyDirectRelationshipVisibleProjectionFields(request ExecutionRequest) ([]qsbridge.QuantaProjectionField, qsbridge.DiagnosticSet) {
	if len(request.ProjectionOrder) > 0 {
		fields := make([]qsbridge.QuantaProjectionField, 0, len(request.ProjectionOrder))
		for _, field := range request.ProjectionOrder {
			fields = append(fields, legacyDirectRelationshipProjectionField(field))
		}
		return fields, nil
	}
	fields := request.Query.ProjectionFields
	if len(request.Materialization.ProjectionFields) > 0 {
		fields = request.Materialization.ProjectionFields
	}
	result := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if field.Visible {
			result = append(result, field)
		}
	}
	return result, nil
}

func legacyDirectRelationshipProjectionField(field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	roles := field.Roles | qsbridge.FieldRoleVisible
	return qsbridge.QuantaProjectionField{
		Index:        field.Table.Table,
		Field:        directBitmapFieldPhysicalName(field),
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        roles,
		Visible:      true,
	}
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipMaterializedValues(ctx context.Context, kernel ProjectionMaterializationKernel, index string, rownums []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField, template qsbridge.QuantaMaterializationRequest) (map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell, qsbridge.DiagnosticSet, error) {
	values, _, diagnostics, err := e.legacyDirectRelationshipMaterializedValuesWithProbes(ctx, kernel, index, rownums, fields, template)
	return values, diagnostics, err
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipMaterializedValuesWithProbes(ctx context.Context, kernel ProjectionMaterializationKernel, index string, rownums []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField, template qsbridge.QuantaMaterializationRequest) (map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	values := make(map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell)
	if len(fields) == 0 {
		return values, nil, nil, nil
	}
	if kernel == nil {
		return nil, nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector materialization kernel is not configured"),
		}, nil
	}
	materializationRequest := template
	materializationRequest.Index = index
	materializationRequest.Rownums = legacyDirectRelationshipUniqueRownums(rownums)
	materializationRequest.ProjectionFields = fields
	if materializationRequest.DependencyID == "" {
		materializationRequest.DependencyID = "relationship_materialization." + index
	}
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID:          "relationship_projection_materialization",
		ProbePrefix: "relationship_projection_materialization_",
		Requests:    []qsbridge.QuantaMaterializationRequest{materializationRequest},
	}
	result, err := ExecuteProjectionMaterializationKernel(ctx, kernel, request)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return nil, result.Probes, diagnostics, err
	}
	if len(result.Results) == 0 {
		return nil, result.Probes, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector materialization kernel returned no results"),
		}, nil
	}
	rowSet := result.Results[0].RowSet
	diagnostics = append(diagnostics, result.Results[0].Diagnostics...)
	if diagnostics.BlocksNative() {
		return nil, result.Probes, diagnostics, nil
	}
	for _, vector := range rowSet.ProjectionVectors {
		fieldKey := legacyDirectRelationshipProjectionFieldKey(vector.Field)
		values[fieldKey] = make(map[qsbridge.QuantaRownum]qsbridge.ResultCell, len(rowSet.Rownums))
		for i, rownum := range rowSet.Rownums {
			if i >= len(vector.Values) {
				return nil, result.Probes, qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("projection vector %s has %d values for %d rownums", vector.Field.Field, len(vector.Values), len(rowSet.Rownums))),
				}, nil
			}
			values[fieldKey][rownum] = vector.Values[i]
		}
	}
	return values, result.Probes, diagnostics, nil
}

func legacyDirectRelationshipFieldsForIndex(fields []qsbridge.QuantaProjectionField, index string) []qsbridge.QuantaProjectionField {
	result := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if field.Index == "" {
			field.Index = index
		}
		if strings.EqualFold(field.Index, index) {
			result = append(result, field)
		}
	}
	return result
}

func legacyDirectRelationshipFieldsForEdge(fields []qsbridge.QuantaProjectionField, edge legacyDirectRelationshipEdge) []qsbridge.QuantaProjectionField {
	result := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if strings.EqualFold(field.Index, edge.childTable) || strings.EqualFold(field.Index, edge.parentTable) {
			result = append(result, field)
		}
	}
	return result
}

func legacyDirectRelationshipPostReductionFields(request ExecutionRequest, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	required := legacyDirectRelationshipPostReductionFieldKeys(request)
	return legacyDirectRelationshipFilterPostReductionFields(required, fields)
}

func legacyDirectRelationshipPostReductionMaterializationFields(request ExecutionRequest, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	required := legacyDirectRelationshipPostReductionMaterializationFieldKeys(request)
	return legacyDirectRelationshipFilterPostReductionFields(required, fields)
}

func legacyDirectRelationshipFilterPostReductionFields(required map[string]struct{}, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	if len(required) == 0 {
		return fields
	}
	result := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if legacyDirectRelationshipProjectionFieldIsRequired(required, field) {
			result = append(result, field)
		}
	}
	if len(result) == 0 {
		return fields
	}
	return result
}

func legacyDirectRelationshipPostReductionFieldKeys(request ExecutionRequest) map[string]struct{} {
	return legacyDirectRelationshipPostReductionFieldKeysWithPredicateFilter(request, func(qsbridge.Predicate) bool {
		return true
	})
}

func legacyDirectRelationshipPostReductionMaterializationFieldKeys(request ExecutionRequest) map[string]struct{} {
	return legacyDirectRelationshipPostReductionFieldKeysWithPredicateFilter(request, legacyDirectRelationshipPredicateNeedsPostReductionMaterialization)
}

func legacyDirectRelationshipPostReductionFieldKeysWithPredicateFilter(request ExecutionRequest, predicateRequired func(qsbridge.Predicate) bool) map[string]struct{} {
	keys := make(map[string]struct{})
	addField := func(ref qsbridge.FieldRef) {
		name := directBitmapFieldPhysicalName(ref)
		role := legacyDirectRelationshipTableRoleKey(ref.Table)
		if role == "" || name == "" {
			return
		}
		keys[strings.ToLower(role+"."+name)] = struct{}{}
		if ref.Table.Table != "" {
			keys[strings.ToLower(ref.Table.Table+"."+name)] = struct{}{}
		}
	}
	addExpr := func(expr qsbridge.Expr) {
		for _, ref := range qsbridge.FieldRefs(expr) {
			addField(ref)
		}
	}
	for _, projection := range request.Projection {
		addExpr(projection.Expr)
	}
	for _, predicate := range request.Predicates {
		if predicateRequired == nil || predicateRequired(predicate) {
			addExpr(predicate.Expr)
		}
	}
	for _, predicate := range request.NativePredicates.CorrelatedAggregate {
		addField(predicate.KeyField)
		addField(predicate.ValueField)
	}
	for _, join := range request.Joins {
		for _, predicate := range join.On {
			if predicateRequired == nil || predicateRequired(predicate) {
				addExpr(predicate.Expr)
			}
		}
	}
	for _, expr := range request.GroupBy {
		addExpr(expr)
	}
	for _, aggregate := range request.SQLAggregates {
		addExpr(aggregate.Input)
		addExpr(aggregate.Filter)
	}
	for _, predicate := range request.Having {
		addExpr(predicate.Expr)
	}
	for _, sort := range request.OrderBy {
		addExpr(sort.Expr)
	}
	for _, field := range request.Result.Columns {
		addField(field)
	}
	for _, field := range request.Result.Hidden {
		addField(field)
	}
	return keys
}

func legacyDirectRelationshipPredicateNeedsPostReductionMaterialization(predicate qsbridge.Predicate) bool {
	return predicate.Placement == qsbridge.PredicateResidualScan || predicate.Placement == qsbridge.PredicateResidualJoin
}

func legacyDirectRelationshipProjectionFieldIsRequired(required map[string]struct{}, field qsbridge.QuantaProjectionField) bool {
	for _, key := range legacyDirectRelationshipProjectionFieldRequiredKeys(field) {
		if _, ok := required[key]; ok {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipProjectionFieldRequiredKeys(field qsbridge.QuantaProjectionField) []string {
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	if name == "" {
		return nil
	}
	keys := make([]string, 0, 2)
	if field.Role != "" {
		keys = append(keys, strings.ToLower(string(field.Role)+"."+name))
	}
	if field.Index != "" {
		keys = append(keys, strings.ToLower(field.Index+"."+name))
	}
	return keys
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyMemberships(ctx context.Context, request ExecutionRequest, edge legacyDirectRelationshipEdge, joined []qsbridge.QuantaRownum, pairs []legacyDirectRelationshipPair) ([]qsbridge.QuantaRownum, []legacyDirectRelationshipPair, qsbridge.DiagnosticSet, error) {
	if len(request.Memberships) == 0 {
		return joined, pairs, nil, nil
	}
	materialization := e.projectionMaterializationKernel()
	runtime := DirectBitmapRuntime{
		Sessions:            LegacyQuantaSourceSessionProvider{Source: e.Source},
		Materialization:     materialization,
		ProjectionBSIReader: e.ProjectionBSIReader,
		SameRowComparison:   e.sameRowComparisonKernel(),
	}
	filteredPairs := append([]legacyDirectRelationshipPair(nil), pairs...)
	for _, membership := range request.Memberships {
		rightValues, diagnostics, err := runtime.directBitmapMembershipRightValues(ctx, membership)
		if err != nil || diagnostics.BlocksNative() {
			return nil, nil, diagnostics, err
		}
		nextPairs, diagnostics, err := e.legacyDirectRelationshipApplyMembership(ctx, materialization, edge, filteredPairs, membership, rightValues)
		if err != nil || diagnostics.BlocksNative() {
			return nil, nil, diagnostics, err
		}
		filteredPairs = nextPairs
	}
	filteredJoined := make([]qsbridge.QuantaRownum, len(filteredPairs))
	for i, pair := range filteredPairs {
		filteredJoined[i] = pair.child
	}
	return filteredJoined, filteredPairs, nil, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyMembership(ctx context.Context, materialization ProjectionMaterializationKernel, edge legacyDirectRelationshipEdge, pairs []legacyDirectRelationshipPair, membership qsbridge.MembershipEdge, rightValues map[string]struct{}) ([]legacyDirectRelationshipPair, qsbridge.DiagnosticSet, error) {
	if len(rightValues) == 0 {
		if membership.Kind == qsbridge.MembershipAnti {
			return append([]legacyDirectRelationshipPair(nil), pairs...), nil, nil
		}
		return nil, nil, nil
	}
	leftTable := membership.Left.Table.Table
	leftField := directBitmapMembershipProjectionField(membership.Left)
	var rownums []qsbridge.QuantaRownum
	switch {
	case strings.EqualFold(leftTable, edge.childTable):
		rownums = legacyDirectRelationshipChildRownums(pairs)
	case strings.EqualFold(leftTable, edge.parentTable):
		rownums = legacyDirectRelationshipParentRownums(pairs)
	default:
		return nil, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector membership left table %s is not part of the relationship edge", leftTable)), nil
	}
	values, diagnostics, err := e.legacyDirectRelationshipMaterializedValues(ctx, materialization, leftTable, rownums, []qsbridge.QuantaProjectionField{leftField}, qsbridge.QuantaMaterializationRequest{})
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	fieldKey := legacyDirectRelationshipProjectionFieldKey(leftField)
	leftValues := values[fieldKey]
	if len(leftValues) == 0 && len(rownums) > 0 {
		return nil, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector membership missing left values for %s.%s", leftField.Index, leftField.Field)), nil
	}
	filtered := make([]legacyDirectRelationshipPair, 0, len(pairs))
	for _, pair := range pairs {
		rownum := pair.child
		if strings.EqualFold(leftTable, edge.parentTable) {
			rownum = pair.parent
		}
		cell, ok := leftValues[rownum]
		if !ok {
			return nil, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector membership missing left value for %s.%s", leftField.Index, leftField.Field)), nil
		}
		_, matched := rightValues[directBitmapGroupKey(cell)]
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered = append(filtered, pair)
		}
	}
	return filtered, nil, nil
}

func legacyDirectRelationshipChildRownums(pairs []legacyDirectRelationshipPair) []qsbridge.QuantaRownum {
	rownums := make([]qsbridge.QuantaRownum, len(pairs))
	for i, pair := range pairs {
		rownums[i] = pair.child
	}
	return rownums
}

func legacyDirectRelationshipParentRownums(pairs []legacyDirectRelationshipPair) []qsbridge.QuantaRownum {
	rownums := make([]qsbridge.QuantaRownum, len(pairs))
	for i, pair := range pairs {
		rownums[i] = pair.parent
	}
	return rownums
}

func legacyDirectRelationshipUniqueRownums(rownums []qsbridge.QuantaRownum) []qsbridge.QuantaRownum {
	seen := make(map[qsbridge.QuantaRownum]struct{}, len(rownums))
	result := make([]qsbridge.QuantaRownum, 0, len(rownums))
	for _, rownum := range rownums {
		if _, ok := seen[rownum]; ok {
			continue
		}
		seen[rownum] = struct{}{}
		result = append(result, rownum)
	}
	return result
}

func legacyDirectRelationshipAssembleProjectedRows(edge legacyDirectRelationshipEdge, pairs []legacyDirectRelationshipPair, fields []qsbridge.QuantaProjectionField, childValues map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell, parentValues map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   edge.childTable,
		Rownums: make([]qsbridge.QuantaRownum, len(pairs)),
	}
	for i, pair := range pairs {
		rowSet.Rownums[i] = pair.child
	}
	for _, field := range fields {
		values := make([]qsbridge.ResultCell, 0, len(pairs))
		for _, pair := range pairs {
			cell, ok := legacyDirectRelationshipCell(edge, field, pair, childValues, parentValues)
			if !ok {
				return qsbridge.QuantaProjectedRowSet{}, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector projection missing value for %s.%s", field.Index, field.Field))
			}
			values = append(values, cell)
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field:  field,
			Values: values,
		})
	}
	return rowSet, rowSet.ValidateShape()
}

func legacyDirectRelationshipCell(edge legacyDirectRelationshipEdge, field qsbridge.QuantaProjectionField, pair legacyDirectRelationshipPair, childValues map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell, parentValues map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell) (qsbridge.ResultCell, bool) {
	fieldKey := legacyDirectRelationshipProjectionFieldKey(field)
	switch {
	case strings.EqualFold(field.Index, edge.childTable):
		if edge.sqlKind == qsbridge.JoinKindLeftOuter && edge.leftOuterPreservesParent && pair.child == 0 {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, true
		}
		cell, ok := childValues[fieldKey][pair.child]
		return cell, ok
	case strings.EqualFold(field.Index, edge.parentTable):
		cell, ok := parentValues[fieldKey][pair.parent]
		return cell, ok
	default:
		return qsbridge.ResultCell{}, false
	}
}

func legacyDirectRelationshipSameTableInstance(left qsbridge.TableInstance, right qsbridge.TableInstance) bool {
	leftRole := legacyDirectRelationshipTableRoleKey(left)
	rightRole := legacyDirectRelationshipTableRoleKey(right)
	if leftRole != "" && rightRole != "" {
		return strings.EqualFold(leftRole, rightRole)
	}
	return strings.EqualFold(left.Table, right.Table)
}

func legacyDirectRelationshipResidualPredicatesForScope(request ExecutionRequest, scope qsbridge.PredicateScope) []qsbridge.Predicate {
	var result []qsbridge.Predicate
	for _, predicate := range request.Predicates {
		if predicate.Scope == scope && predicate.Placement == qsbridge.PredicateResidualScan {
			result = append(result, predicate)
		}
	}
	for _, join := range request.Joins {
		for _, predicate := range join.On {
			if predicate.Scope != scope {
				continue
			}
			if predicate.Placement == qsbridge.PredicateResidualScan || predicate.Placement == qsbridge.PredicateResidualJoin {
				result = append(result, predicate)
			}
		}
	}
	return result
}

func legacyDirectRelationshipProjectionFieldKey(field qsbridge.QuantaProjectionField) string {
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	return strings.ToLower(field.Index) + "\x00" + name
}

func legacyDirectRelationshipShapeDiagnostics(request ExecutionRequest, vector RelationshipVectorJoinRequest) qsbridge.DiagnosticSet {
	if vector.RootIndex == "" {
		return legacyDirectRelationshipDiagnostic("relationship-vector execution requires a root index")
	}
	if len(request.GroupBy) > 0 {
		for _, groupExpr := range request.GroupBy {
			if _, ok := directBitmapExprField(groupExpr); !ok {
				return legacyDirectRelationshipDiagnostic("relationship-vector execution only supports field GROUP BY in this slice")
			}
		}
		if len(request.SQLAggregates) == 0 {
			return legacyDirectRelationshipDiagnostic("relationship-vector execution requires aggregates for GROUP BY in this slice")
		}
	}
	if len(request.SQLAggregates) == 0 {
		return nil
	}
	return nil
}

func legacyDirectRelationshipFragmentsForTable(request ExecutionRequest, table string, role string) []qsbridge.QuantaQueryFragment {
	result := make([]qsbridge.QuantaQueryFragment, 0, len(request.Query.Fragments))
	for _, fragment := range request.Query.Fragments {
		if legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, table, role) {
			result = append(result, fragment)
		}
	}
	return result
}

func legacyDirectRelationshipBitmap(rownums []qsbridge.QuantaRownum) *roaring64.Bitmap {
	bitmap := roaring64.NewBitmap()
	for _, rownum := range rownums {
		bitmap.Add(uint64(rownum))
	}
	return bitmap
}

func legacyDirectRelationshipVectorProjectionWindow(request qsbridge.QuantaMaterializationRequest) (int64, int64) {
	// This preserves ordinary materialization scope. Relationship-vector FK reads
	// with a foundset use legacyDirectRelationshipVectorProjectionWindowForEdge so
	// optimizer-selected broad scope can keep FK vectors visible.
	return nativeProjectionTimeWindowNanos(request.FromEpochMillis, request.ToEpochMillis)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipVectorProjectionWindow(request ExecutionRequest, childTable string) (int64, int64) {
	return e.legacyDirectRelationshipVectorProjectionWindowForRole(request, childTable, "")
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipVectorProjectionWindowForEdge(request ExecutionRequest, edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum) (int64, int64) {
	if edge.projectionScope == qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset && len(childRows) > 0 {
		return e.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
	}
	if len(childRows) > 0 &&
		legacyDirectRelationshipTimeQuantumField(e.legacyDirectCachedTable(edge.childTable)) != "" &&
		!e.legacyDirectRelationshipHasShardTimeFragmentForRole(request, edge.childTable, edge.childRole) &&
		e.legacyDirectRelationshipHasAnyShardTimeFragment(request) {
		return e.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
	}
	return e.legacyDirectRelationshipVectorProjectionWindowForRole(request, edge.childTable, edge.childRole)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipVectorProjectionWindowForRole(request ExecutionRequest, childTable string, childRole string) (int64, int64) {
	for _, fragment := range request.Query.Fragments {
		if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, childTable, childRole) || fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !e.legacyDirectRelationshipFragmentIsShardTimeField(childTable, fragment) {
			continue
		}
		table := e.legacyDirectCachedTable(childTable)
		return legacyDirectRelationshipEncodedTimeToNanos(table, fragment.Field, fragment.Begin.Int64()), legacyDirectRelationshipEncodedTimeToNanos(table, fragment.Field, fragment.End.Int64())
	}
	fromTime, toTime := legacyDirectRelationshipVectorProjectionWindow(request.Materialization)
	if fromTime != 0 || toTime != 0 {
		return fromTime, toTime
	}
	if table := e.legacyDirectCachedTable(childTable); legacyDirectRelationshipTimeQuantumField(table) != "" {
		return legacyDirectRelationshipFullTimeRangeBeginMillis * int64(time.Millisecond), legacyDirectRelationshipFullTimeRangeEndMillis * int64(time.Millisecond)
	}
	return 0, 0
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipHasShardTimeFragmentForRole(request ExecutionRequest, tableName string, role string) bool {
	for _, fragment := range request.Query.Fragments {
		if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, tableName, role) {
			continue
		}
		if e.legacyDirectRelationshipFragmentIsShardTimeField(tableName, fragment) {
			return true
		}
	}
	return false
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipHasAnyShardTimeFragment(request ExecutionRequest) bool {
	for _, fragment := range request.Query.Fragments {
		if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		for _, source := range request.Sources {
			if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, source.Table, source.RefName()) {
				continue
			}
			if e.legacyDirectRelationshipFragmentIsShardTimeField(source.Table, fragment) {
				return true
			}
		}
		if e.legacyDirectRelationshipFragmentIsShardTimeField(fragment.Index, fragment) {
			return true
		}
	}
	return false
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipBroadVectorProjectionWindow(childTable string) (int64, int64) {
	if table := e.legacyDirectCachedTable(childTable); legacyDirectRelationshipTimeQuantumField(table) != "" {
		return legacyDirectRelationshipFullTimeRangeBeginMillis * int64(time.Millisecond), legacyDirectRelationshipFullTimeRangeEndMillis * int64(time.Millisecond)
	}
	return 0, 0
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipTimeMaterialization(request ExecutionRequest, tableName string) qsbridge.QuantaMaterializationRequest {
	return e.legacyDirectRelationshipTimeMaterializationForRole(request, tableName, "")
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipTimeMaterializationForRole(request ExecutionRequest, tableName string, role string) qsbridge.QuantaMaterializationRequest {
	for _, fragment := range request.Query.Fragments {
		if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, tableName, role) || fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !e.legacyDirectRelationshipFragmentIsShardTimeField(tableName, fragment) {
			continue
		}
		return qsbridge.QuantaMaterializationRequest{
			Index:           tableName,
			FromEpochMillis: fragment.Begin.Int64(),
			ToEpochMillis:   fragment.End.Int64(),
			ProjectionFields: []qsbridge.QuantaProjectionField{{
				Index:        fragment.Index,
				Role:         fragment.Role,
				Field:        fragment.Field,
				PhysicalName: legacyDirectRelationshipFragmentFieldName(fragment.Field),
				Type:         qsbridge.DataTypeTime,
			}},
		}
	}
	if table := e.legacyDirectCachedTable(tableName); legacyDirectRelationshipTimeQuantumField(table) != "" {
		return qsbridge.QuantaMaterializationRequest{
			Index:           tableName,
			FromEpochMillis: legacyDirectRelationshipFullTimeRangeBeginMillis,
			ToEpochMillis:   legacyDirectRelationshipFullTimeRangeEndMillis,
		}
	}
	return qsbridge.QuantaMaterializationRequest{}
}

func legacyDirectRelationshipFragmentTargetsTable(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment, tableName string) bool {
	return legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, tableName, "")
}

func legacyDirectRelationshipFragmentTargetsTableRole(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment, tableName string, role string) bool {
	if fragment.Role != "" {
		if role == "" {
			return false
		}
		return strings.EqualFold(string(fragment.Role), legacyDirectRelationshipRoleKey(role, tableName))
	}
	if fragment.Index == tableName {
		return true
	}
	for _, source := range request.Sources {
		if source.Table == tableName && source.Alias == fragment.Index {
			return true
		}
	}
	return false
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipFragmentIsTimeField(tableName string, fragment qsbridge.QuantaQueryFragment) bool {
	table := e.legacyDirectCachedTable(tableName)
	if table == nil {
		return false
	}
	attr, err := table.GetAttribute(legacyDirectRelationshipFragmentFieldName(fragment.Field))
	if err != nil || attr == nil {
		return false
	}
	return legacyDataType(attr.Type) == qsbridge.DataTypeTime
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipFragmentIsShardTimeField(tableName string, fragment qsbridge.QuantaQueryFragment) bool {
	table := e.legacyDirectCachedTable(tableName)
	if table == nil || table.BasicTable == nil || table.TimeQuantumField == "" {
		return false
	}
	return strings.EqualFold(table.TimeQuantumField, legacyDirectRelationshipFragmentFieldName(fragment.Field))
}

func legacyDirectRelationshipFragmentFieldName(field string) string {
	if i := strings.LastIndex(field, "."); i >= 0 && i+1 < len(field) {
		return field[i+1:]
	}
	return field
}

func legacyDirectRelationshipProjectionDebug(request ExecutionRequest, childTable string, table *core.Table, foundSet *roaring64.Bitmap, fromTime, toTime int64, bsiByField map[string]*roaring64.BSI) string {
	keys := make([]string, 0, len(bsiByField))
	for key := range bsiByField {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fragments := make([]string, 0, len(request.Query.Fragments))
	for _, fragment := range request.Query.Fragments {
		if legacyDirectRelationshipFragmentTargetsTable(request, fragment, childTable) {
			fragments = append(fragments, fmt.Sprintf("%s.%s %s", fragment.Index, fragment.Field, fragment.BSIOp))
		}
	}
	cardinality := uint64(0)
	if foundSet != nil {
		cardinality = foundSet.GetCardinality()
	}
	tableDebug := "table_cache=missing"
	if table != nil {
		tableDebug = fmt.Sprintf("table_cache=time_field:%s time_type:%s derived_time:%s attrs:%d", table.TimeQuantumField, table.TimeQuantumType, legacyDirectRelationshipTimeQuantumField(table), len(table.Attributes))
	}
	return fmt.Sprintf("foundset=%d window=%d..%d returned_bsi=%s child_fragments=%s %s",
		cardinality, fromTime, toTime, strings.Join(keys, ","), strings.Join(fragments, ";"), tableDebug)
}

func legacyDirectRelationshipSignedIDs(rownums []qsbridge.QuantaRownum) []int64 {
	ids := make([]int64, len(rownums))
	for i, rownum := range rownums {
		ids[i] = int64(rownum)
	}
	return ids
}

func legacyDirectRelationshipRownums(bitmap *roaring64.Bitmap) []qsbridge.QuantaRownum {
	if bitmap == nil {
		return nil
	}
	values := bitmap.ToArray()
	rownums := make([]qsbridge.QuantaRownum, len(values))
	for i, value := range values {
		rownums[i] = qsbridge.QuantaRownum(value)
	}
	return rownums
}

func legacyDirectRelationshipIntersectRownums(left []qsbridge.QuantaRownum, right []qsbridge.QuantaRownum) []qsbridge.QuantaRownum {
	rightSet := make(map[qsbridge.QuantaRownum]struct{}, len(right))
	for _, rownum := range right {
		rightSet[rownum] = struct{}{}
	}
	result := make([]qsbridge.QuantaRownum, 0, len(left))
	for _, rownum := range left {
		if _, ok := rightSet[rownum]; ok {
			result = append(result, rownum)
		}
	}
	return result
}

func legacyDirectRelationshipDiagnostic(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, message),
	}
}
