package qsruntime

import (
	"context"
	"strings"
	"sync"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type queryScratchpadContextKey struct{}

// QueryScratchpad carries request-scoped execution memoization across planner
// and executor helpers. It is topology-neutral; deployment adapters decide how
// cache misses are filled.
type QueryScratchpad struct {
	ProjectionBSIs                *ProjectionBSICache
	ProjectionValues              *ProjectionValueCache
	DomainMappings                *DomainMappingCache
	RelationshipVectorProjections *RelationshipVectorProjectionCache
	Instrumentation               *ExecutionInstrumentation
}

// NewQueryScratchpad creates an empty per-query execution scratchpad.
func NewQueryScratchpad() *QueryScratchpad {
	return &QueryScratchpad{
		ProjectionBSIs:                NewProjectionBSICache(),
		ProjectionValues:              NewProjectionValueCache(),
		DomainMappings:                NewDomainMappingCache(),
		RelationshipVectorProjections: NewRelationshipVectorProjectionCache(),
		Instrumentation:               NewExecutionInstrumentation(),
	}
}

// WithQueryScratchpad installs a request-scoped execution scratchpad.
func WithQueryScratchpad(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if QueryScratchpadFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, queryScratchpadContextKey{}, NewQueryScratchpad())
}

// QueryScratchpadFromContext returns the request scratchpad, when installed.
func QueryScratchpadFromContext(ctx context.Context) *QueryScratchpad {
	if ctx == nil {
		return nil
	}
	scratchpad, _ := ctx.Value(queryScratchpadContextKey{}).(*QueryScratchpad)
	return scratchpad
}

// ProjectionBSICacheFromContext returns the request-scoped projection cache.
func ProjectionBSICacheFromContext(ctx context.Context) *ProjectionBSICache {
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		return nil
	}
	return scratchpad.ProjectionBSIs
}

// ProjectionValueCacheFromContext returns the request-scoped materialized value
// cache.
func ProjectionValueCacheFromContext(ctx context.Context) *ProjectionValueCache {
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		return nil
	}
	return scratchpad.ProjectionValues
}

// DomainMappingCacheFromContext returns the request-scoped rownum-domain
// mapping cache.
func DomainMappingCacheFromContext(ctx context.Context) *DomainMappingCache {
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		return nil
	}
	return scratchpad.DomainMappings
}

// RelationshipVectorProjectionCacheFromContext returns the request-scoped
// relationship-vector FK projection cache.
func RelationshipVectorProjectionCacheFromContext(ctx context.Context) *RelationshipVectorProjectionCache {
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		return nil
	}
	return scratchpad.RelationshipVectorProjections
}

// RecordQueryScratchpadCacheLookup records a request-scoped cache lookup in the
// active execution instrumentation.
func RecordQueryScratchpadCacheLookup(ctx context.Context, cacheName string, hit bool, mode string, detail string) {
	recordQueryScratchpadCacheLookup(ctx, cacheName, hit, mode, detail)
}

// RecordQueryScratchpadCacheStore records a request-scoped cache write in the
// active execution instrumentation.
func RecordQueryScratchpadCacheStore(ctx context.Context, cacheName string, detail string) {
	recordQueryScratchpadCacheStore(ctx, cacheName, detail)
}

func recordQueryScratchpadCacheLookup(ctx context.Context, cacheName string, hit bool, mode string, detail string) {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil || cacheName == "" {
		return
	}
	value := "miss"
	counter := cacheName + "_miss"
	if hit {
		value = "hit"
		counter = cacheName + "_hit"
	}
	detail = queryScratchpadCacheObservationDetail(mode, detail)
	recorder.ObserveEvent("query_scratchpad", cacheName+"_lookup", value, detail)
	recorder.ObserveCount("query_scratchpad", counter, 1, detail)
}

func recordQueryScratchpadCacheStore(ctx context.Context, cacheName string, detail string) {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil || cacheName == "" {
		return
	}
	recorder.ObserveEvent("query_scratchpad", cacheName+"_store", "stored", detail)
	recorder.ObserveCount("query_scratchpad", cacheName+"_store", 1, detail)
}

func queryScratchpadCacheObservationDetail(mode string, detail string) string {
	parts := []string{}
	if mode != "" {
		parts = append(parts, "mode="+mode)
	}
	if detail != "" {
		parts = append(parts, detail)
	}
	return strings.Join(parts, " ")
}

// ProjectionBSICacheKey identifies a BSI projection by logical field and time
// window. Candidate rownum sets live in cache entries so subset reuse can be
// evaluated without creating topology-specific key shapes.
type ProjectionBSICacheKey struct {
	Index           string
	Field           string
	FromTimeNanos   int64
	ToTimeNanos     int64
	FromEpochMillis int64
	ToEpochMillis   int64
}

type projectionBSICacheEntry struct {
	RownumSet *roaring64.Bitmap
	BSI       *roaring64.BSI
}

// ProjectionBSICachePartial carries cached disjoint projection slices plus the
// rownums still required to satisfy a broader request.
type ProjectionBSICachePartial struct {
	BSI              *roaring64.BSI
	CoveredRownumSet *roaring64.Bitmap
	MissingRownumSet *roaring64.Bitmap
}

// CoveredCardinality returns the number of requested rownums reused from cache.
func (p ProjectionBSICachePartial) CoveredCardinality() uint64 {
	if p.CoveredRownumSet == nil {
		return 0
	}
	return p.CoveredRownumSet.GetCardinality()
}

// MissingCardinality returns the number of rownums that must still be fetched.
func (p ProjectionBSICachePartial) MissingCardinality() uint64 {
	if p.MissingRownumSet == nil {
		return 0
	}
	return p.MissingRownumSet.GetCardinality()
}

// MissingRownums returns the rownums that must still be fetched.
func (p ProjectionBSICachePartial) MissingRownums() []qsbridge.QuantaRownum {
	return projectionBSICacheBitmapRownums(p.MissingRownumSet)
}

// MergeFetchedMissing merges a BSI fetched for MissingRownumSet into the cached
// partial projection. It assumes the fetched BSI only contains missing rownums.
func (p ProjectionBSICachePartial) MergeFetchedMissing(fetched *roaring64.BSI) *roaring64.BSI {
	if p.BSI == nil {
		return fetched
	}
	if fetched != nil {
		p.BSI.Add(fetched)
	}
	return p.BSI
}

// ProjectionBSICache deduplicates read-only BSI projections within one SQL
// execution request.
type ProjectionBSICache struct {
	mu      sync.Mutex
	entries map[ProjectionBSICacheKey][]projectionBSICacheEntry
}

// NewProjectionBSICache creates an empty per-query BSI projection cache.
func NewProjectionBSICache() *ProjectionBSICache {
	return &ProjectionBSICache{
		entries: make(map[ProjectionBSICacheKey][]projectionBSICacheEntry),
	}
}

// Get returns an exact cached projection or a retained copy from a cached
// superset. The mode string is probe-friendly: "exact" or "retained_subset" on
// hits, and "cache_absent", "key_miss", or "coverage_miss" on misses.
func (c *ProjectionBSICache) Get(key ProjectionBSICacheKey, rownumSet *roaring64.Bitmap) (*roaring64.BSI, string, bool) {
	if c == nil {
		return nil, "cache_absent", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.entries[key]
	if len(entries) == 0 {
		return nil, "key_miss", false
	}
	for _, entry := range entries {
		if projectionRownumSetsEqual(entry.RownumSet, rownumSet) {
			return entry.BSI, "exact", true
		}
	}
	for _, entry := range entries {
		if projectionRownumSetCovers(entry.RownumSet, rownumSet) {
			return entry.BSI.NewBSIRetainSet(rownumSet), "retained_subset", true
		}
	}
	return nil, "coverage_miss", false
}

// GetPartial returns cached disjoint projection slices for part of a request and
// the rownums still missing from cache.
func (c *ProjectionBSICache) GetPartial(key ProjectionBSICacheKey, rownumSet *roaring64.Bitmap) (ProjectionBSICachePartial, string, bool) {
	if c == nil {
		return ProjectionBSICachePartial{}, "cache_absent", false
	}
	if rownumSet == nil {
		return ProjectionBSICachePartial{}, "empty_request", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.entries[key]
	if len(entries) == 0 {
		return ProjectionBSICachePartial{}, "key_miss", false
	}
	missing := rownumSet.Clone()
	covered := roaring64.NewBitmap()
	merged := roaring64.NewDefaultBSI()
	for _, entry := range entries {
		if entry.RownumSet == nil || entry.BSI == nil || missing.IsEmpty() {
			continue
		}
		overlap := missing.Clone()
		overlap.And(entry.RownumSet)
		if overlap.IsEmpty() {
			continue
		}
		merged.Add(entry.BSI.NewBSIRetainSet(overlap))
		covered.Or(overlap)
		missing.AndNot(overlap)
	}
	if covered.IsEmpty() {
		return ProjectionBSICachePartial{}, "coverage_miss", false
	}
	mode := "partial_hit"
	if missing.IsEmpty() {
		mode = "merged_entries"
	}
	return ProjectionBSICachePartial{
		BSI:              merged,
		CoveredRownumSet: covered,
		MissingRownumSet: missing,
	}, mode, true
}

// Set records a projected BSI for one candidate rownum set.
func (c *ProjectionBSICache) Set(key ProjectionBSICacheKey, rownumSet *roaring64.Bitmap, bsi *roaring64.BSI) {
	if c == nil || rownumSet == nil || bsi == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = append(c.entries[key], projectionBSICacheEntry{
		RownumSet: rownumSet.Clone(),
		BSI:       bsi,
	})
}

// ProjectionBSICacheKeyFor builds the shared projection-cache key for a native
// BSI read request.
func ProjectionBSICacheKeyFor(request NativeProjectionBSIReadRequest, fromTime, toTime int64) ProjectionBSICacheKey {
	return ProjectionBSICacheKey{
		Index:           request.Index,
		Field:           request.PhysicalField,
		FromTimeNanos:   fromTime,
		ToTimeNanos:     toTime,
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
}

func projectionRownumSetsEqual(left, right *roaring64.Bitmap) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.GetCardinality() != right.GetCardinality() {
		return false
	}
	return left.Equals(right)
}

func projectionRownumSetCovers(container, subset *roaring64.Bitmap) bool {
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

func projectionBSICacheBitmapRownums(bitmap *roaring64.Bitmap) []qsbridge.QuantaRownum {
	if bitmap == nil || bitmap.IsEmpty() {
		return nil
	}
	values := bitmap.ToArray()
	rownums := make([]qsbridge.QuantaRownum, 0, len(values))
	for _, value := range values {
		rownums = append(rownums, qsbridge.QuantaRownum(value))
	}
	return rownums
}

// ProjectionValueCacheKey identifies visible materialized cells by physical
// field and materialization window. Aliases deliberately do not participate in
// the key so repeated self-join aliases can share the same physical vector.
type ProjectionValueCacheKey struct {
	Index           string
	Field           string
	FromEpochMillis int64
	ToEpochMillis   int64
}

// ProjectionValueCacheLookup carries ordered cached values plus any rownums
// still missing from cache.
type ProjectionValueCacheLookup struct {
	Values           []qsbridge.ResultCell
	MissingRownums   []qsbridge.QuantaRownum
	MissingPositions []int
	CoveredRows      int
}

// MissingCount reports how many requested rows still need to be materialized.
func (l ProjectionValueCacheLookup) MissingCount() int {
	return len(l.MissingRownums)
}

// ProjectionValueCache deduplicates already visible projection cells within one
// SQL execution request.
type ProjectionValueCache struct {
	mu      sync.Mutex
	entries map[ProjectionValueCacheKey]map[qsbridge.QuantaRownum]qsbridge.ResultCell
}

// NewProjectionValueCache creates an empty per-query projection value cache.
func NewProjectionValueCache() *ProjectionValueCache {
	return &ProjectionValueCache{
		entries: make(map[ProjectionValueCacheKey]map[qsbridge.QuantaRownum]qsbridge.ResultCell),
	}
}

// Get returns cached visible cells in the requested rownum order. A partial hit
// includes the rownums and positions still requiring a storage read.
func (c *ProjectionValueCache) Get(key ProjectionValueCacheKey, rownums []qsbridge.QuantaRownum) (ProjectionValueCacheLookup, string, bool) {
	if c == nil {
		return ProjectionValueCacheLookup{}, "cache_absent", false
	}
	if len(rownums) == 0 {
		return ProjectionValueCacheLookup{Values: []qsbridge.ResultCell{}}, "complete_hit", true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	valuesByRow := c.entries[key]
	if len(valuesByRow) == 0 {
		return ProjectionValueCacheLookup{}, "key_miss", false
	}
	lookup := ProjectionValueCacheLookup{
		Values: make([]qsbridge.ResultCell, len(rownums)),
	}
	for i, rownum := range rownums {
		value, ok := valuesByRow[rownum]
		if !ok {
			lookup.MissingRownums = append(lookup.MissingRownums, rownum)
			lookup.MissingPositions = append(lookup.MissingPositions, i)
			continue
		}
		lookup.Values[i] = cloneProjectionValueCell(value)
		lookup.CoveredRows++
	}
	if lookup.CoveredRows == len(rownums) {
		return lookup, "complete_hit", true
	}
	if lookup.CoveredRows > 0 {
		return lookup, "partial_hit", true
	}
	return ProjectionValueCacheLookup{}, "coverage_miss", false
}

// Set records visible materialized values for their matching rownums.
func (c *ProjectionValueCache) Set(key ProjectionValueCacheKey, rownums []qsbridge.QuantaRownum, values []qsbridge.ResultCell) {
	if c == nil || len(rownums) == 0 || len(rownums) != len(values) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	valuesByRow := c.entries[key]
	if valuesByRow == nil {
		valuesByRow = make(map[qsbridge.QuantaRownum]qsbridge.ResultCell, len(rownums))
		c.entries[key] = valuesByRow
	}
	for i, rownum := range rownums {
		valuesByRow[rownum] = cloneProjectionValueCell(values[i])
	}
}

// ProjectionValueCacheKeyFor builds the shared value-cache key for a materialized
// field request.
func ProjectionValueCacheKeyFor(index string, field qsbridge.QuantaProjectionField, fromEpochMillis, toEpochMillis int64) ProjectionValueCacheKey {
	fieldName := strings.TrimSpace(field.PhysicalName)
	if fieldName == "" {
		fieldName = strings.TrimSpace(field.Field)
	}
	return ProjectionValueCacheKey{
		Index:           strings.TrimSpace(index),
		Field:           fieldName,
		FromEpochMillis: fromEpochMillis,
		ToEpochMillis:   toEpochMillis,
	}
}

func cloneProjectionValueCell(value qsbridge.ResultCell) qsbridge.ResultCell {
	return value
}

// DomainMappingCacheKey identifies a relationship-vector rownum-domain
// translation. Candidate parent/child rownum sets live in cache entries so
// narrowed requests can reuse a broader mapping when both sets are covered.
type DomainMappingCacheKey struct {
	SourceDomain  string
	TargetDomain  string
	VectorIndex   string
	VectorField   string
	Direction     string
	FromTimeNanos int64
	ToTimeNanos   int64
}

type domainMappingCacheEntry struct {
	ParentSet     *roaring64.Bitmap
	ChildSet      *roaring64.Bitmap
	ParentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum
}

// DomainMappingCache deduplicates relationship-vector rownum-domain
// translations within one SQL execution request.
type DomainMappingCache struct {
	mu      sync.Mutex
	entries map[DomainMappingCacheKey][]domainMappingCacheEntry
}

// NewDomainMappingCache creates an empty per-query domain mapping cache.
func NewDomainMappingCache() *DomainMappingCache {
	return &DomainMappingCache{
		entries: make(map[DomainMappingCacheKey][]domainMappingCacheEntry),
	}
}

// Get returns a cached child->parent rownum mapping. It returns "exact" when
// both rownum sets match the cached request and "retained_subset" when a cached
// broader mapping can safely satisfy the narrower request.
func (c *DomainMappingCache) Get(key DomainMappingCacheKey, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum) (map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, string, bool) {
	if c == nil {
		return nil, "", false
	}
	parentSet := projectionBitmapFromRownums(parentRows)
	childSet := projectionBitmapFromRownums(childRows)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries[key] {
		if projectionRownumSetsEqual(entry.ParentSet, parentSet) && projectionRownumSetsEqual(entry.ChildSet, childSet) {
			return cloneDomainMapping(entry.ParentByChild), "exact", true
		}
	}
	for _, entry := range c.entries[key] {
		if projectionRownumSetCovers(entry.ParentSet, parentSet) && projectionRownumSetCovers(entry.ChildSet, childSet) {
			return retainedDomainMapping(entry.ParentByChild, parentSet, childRows), "retained_subset", true
		}
	}
	return nil, "", false
}

// Set records a child->parent rownum mapping for one relationship edge request.
func (c *DomainMappingCache) Set(key DomainMappingCacheKey, parentRows []qsbridge.QuantaRownum, childRows []qsbridge.QuantaRownum, parentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum) {
	if c == nil || parentByChild == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = append(c.entries[key], domainMappingCacheEntry{
		ParentSet:     projectionBitmapFromRownums(parentRows),
		ChildSet:      projectionBitmapFromRownums(childRows),
		ParentByChild: cloneDomainMapping(parentByChild),
	})
}

func projectionBitmapFromRownums(rownums []qsbridge.QuantaRownum) *roaring64.Bitmap {
	bitmap := roaring64.NewBitmap()
	for _, rownum := range rownums {
		bitmap.Add(uint64(rownum))
	}
	return bitmap
}

func cloneDomainMapping(parentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum) map[qsbridge.QuantaRownum]qsbridge.QuantaRownum {
	cloned := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(parentByChild))
	for child, parent := range parentByChild {
		cloned[child] = parent
	}
	return cloned
}

func retainedDomainMapping(parentByChild map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, parentSet *roaring64.Bitmap, childRows []qsbridge.QuantaRownum) map[qsbridge.QuantaRownum]qsbridge.QuantaRownum {
	retained := make(map[qsbridge.QuantaRownum]qsbridge.QuantaRownum, len(childRows))
	for _, child := range childRows {
		parent, ok := parentByChild[child]
		if !ok || !parentSet.Contains(uint64(parent)) {
			continue
		}
		retained[child] = parent
	}
	return retained
}

// RelationshipVectorProjectionCache reuses projected FK BSIs during one
// execution request.
type RelationshipVectorProjectionCache struct {
	mu      sync.Mutex
	entries map[string]*roaring64.BSI
}

// NewRelationshipVectorProjectionCache creates an empty request-scoped
// relationship-vector FK projection cache.
func NewRelationshipVectorProjectionCache() *RelationshipVectorProjectionCache {
	return &RelationshipVectorProjectionCache{entries: map[string]*roaring64.BSI{}}
}

// Get returns a previously projected relationship-vector BSI.
func (c *RelationshipVectorProjectionCache) Get(key string) (*roaring64.BSI, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	bsi, ok := c.entries[key]
	return bsi, ok
}

// Put stores a projected relationship-vector BSI for the current execution request.
func (c *RelationshipVectorProjectionCache) Put(key string, bsi *roaring64.BSI) {
	if c == nil || key == "" || bsi == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = bsi
}

// LegacyDirectRelationshipVectorProjectionCache keeps existing inabox-direct
// call sites stable while they migrate to QueryScratchpad naming.
type LegacyDirectRelationshipVectorProjectionCache = RelationshipVectorProjectionCache

// NewLegacyDirectRelationshipVectorProjectionCache creates an empty
// request-scoped relationship-vector projection cache for compatibility call
// sites that have not yet adopted NewRelationshipVectorProjectionCache.
func NewLegacyDirectRelationshipVectorProjectionCache() *LegacyDirectRelationshipVectorProjectionCache {
	return NewRelationshipVectorProjectionCache()
}

// DirectProjectionBSICache keeps existing inabox-direct call sites stable while
// they migrate to QueryScratchpad/ProjectionBSICache naming.
type DirectProjectionBSICache = ProjectionBSICache

type directProjectionBSICacheKey = ProjectionBSICacheKey

// NewDirectProjectionBSICache creates a shared projection cache for legacy
// direct-mode call sites that have not yet adopted NewProjectionBSICache.
func NewDirectProjectionBSICache() *DirectProjectionBSICache {
	return NewProjectionBSICache()
}

// WithDirectProjectionBSICache installs the shared query scratchpad for legacy
// direct-mode call sites that have not yet adopted WithQueryScratchpad.
func WithDirectProjectionBSICache(ctx context.Context) context.Context {
	return WithQueryScratchpad(ctx)
}

func directProjectionBSICacheFromContext(ctx context.Context) *DirectProjectionBSICache {
	return ProjectionBSICacheFromContext(ctx)
}

func directProjectionBSICacheKeyFor(request NativeProjectionBSIReadRequest, fromTime, toTime int64) directProjectionBSICacheKey {
	return ProjectionBSICacheKeyFor(request, fromTime, toTime)
}
