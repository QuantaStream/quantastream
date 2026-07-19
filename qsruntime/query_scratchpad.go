package qsruntime

import (
	"context"
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
	DomainMappings                *DomainMappingCache
	RelationshipVectorProjections *RelationshipVectorProjectionCache
}

// NewQueryScratchpad creates an empty per-query execution scratchpad.
func NewQueryScratchpad() *QueryScratchpad {
	return &QueryScratchpad{
		ProjectionBSIs:                NewProjectionBSICache(),
		DomainMappings:                NewDomainMappingCache(),
		RelationshipVectorProjections: NewRelationshipVectorProjectionCache(),
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
// superset. The mode string is probe-friendly: "exact", "retained_subset", or
// empty on miss.
func (c *ProjectionBSICache) Get(key ProjectionBSICacheKey, rownumSet *roaring64.Bitmap) (*roaring64.BSI, string, bool) {
	if c == nil {
		return nil, "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries[key] {
		if projectionRownumSetsEqual(entry.RownumSet, rownumSet) {
			return entry.BSI, "exact", true
		}
	}
	for _, entry := range c.entries[key] {
		if projectionRownumSetCovers(entry.RownumSet, rownumSet) {
			return entry.BSI.NewBSIRetainSet(rownumSet), "retained_subset", true
		}
	}
	return nil, "", false
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
