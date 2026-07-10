package qsbridge

import (
	"strconv"
	"strings"
)

// This file defines the planner-facing StringEnum dictionary boundary.
// Dictionary persistence, replication, ingest mutation, and distributed
// coordination intentionally belong behind DictionaryResolver adapters rather
// than inside binding or planning code.

// DictionaryRef identifies one field-level StringEnum dictionary.
type DictionaryRef struct {
	Schema string
	Table  string
	Field  string
}

// Valid reports whether the dictionary reference has table and field identity.
func (r DictionaryRef) Valid() bool {
	return r.Table != "" && r.Field != ""
}

// QualifiedName returns a stable display name for diagnostics and explain output.
func (r DictionaryRef) QualifiedName() string {
	if r.Schema == "" {
		return r.Table + "." + r.Field
	}
	return r.Schema + "." + r.Table + "." + r.Field
}

// DictionaryVersion identifies the dictionary snapshot used for planning.
type DictionaryVersion string

// StringEnumID is the encoded integer value stored in StringEnum bitmap fields.
type StringEnumID uint64

// DictionaryUpdateMode describes how labels may enter a dictionary.
type DictionaryUpdateMode string

const (
	// DictionaryUpdateUnknown means mutation behavior has not been declared.
	DictionaryUpdateUnknown DictionaryUpdateMode = ""
	// DictionaryUpdateStatic means the dictionary is treated as a fixed snapshot.
	DictionaryUpdateStatic DictionaryUpdateMode = "static"
	// DictionaryUpdateAppendOnly means ingest may append new labels without rewriting existing ids.
	DictionaryUpdateAppendOnly DictionaryUpdateMode = "append_only"
	// DictionaryUpdateAdapterOwned means a store or ingest adapter owns mutation semantics.
	DictionaryUpdateAdapterOwned DictionaryUpdateMode = "adapter_owned"
)

// DictionaryConsistencyMode describes how dictionary readers observe mutations.
type DictionaryConsistencyMode string

const (
	// DictionaryConsistencyUnknown means consistency behavior has not been declared.
	DictionaryConsistencyUnknown DictionaryConsistencyMode = ""
	// DictionaryConsistencySnapshot means planners can treat one dictionary version as immutable.
	DictionaryConsistencySnapshot DictionaryConsistencyMode = "snapshot"
	// DictionaryConsistencyVersionedDistributed means distributed readers need explicit versions and invalidation.
	DictionaryConsistencyVersionedDistributed DictionaryConsistencyMode = "versioned_distributed"
	// DictionaryConsistencyAdapterOwned means an adapter defines consistency outside qsbridge.
	DictionaryConsistencyAdapterOwned DictionaryConsistencyMode = "adapter_owned"
)

// DictionaryCapability identifies a dictionary behavior the planner can rely on.
type DictionaryCapability string

const (
	// DictionaryCapabilityStableIDs means encoded ids are stable within a version.
	DictionaryCapabilityStableIDs DictionaryCapability = "stable_ids"
	// DictionaryCapabilityPrefixMatch means prefix LIKE can be satisfied through the dictionary.
	DictionaryCapabilityPrefixMatch DictionaryCapability = "prefix_match"
	// DictionaryCapabilityContainsMatch means contains LIKE can be satisfied through the dictionary.
	DictionaryCapabilityContainsMatch DictionaryCapability = "contains_match"
	// DictionaryCapabilityMutable means new labels may be added after initial load.
	DictionaryCapabilityMutable DictionaryCapability = "mutable"
)

// DictionaryCapabilities is a set of dictionary behaviors.
type DictionaryCapabilities []DictionaryCapability

// Has reports whether the capability set contains capability.
func (c DictionaryCapabilities) Has(capability DictionaryCapability) bool {
	for _, item := range c {
		if item == capability {
			return true
		}
	}
	return false
}

// DictionaryDefinition describes one field dictionary without choosing storage.
type DictionaryDefinition struct {
	Ref          DictionaryRef
	Version      DictionaryVersion
	Cardinality  uint64
	UpdateMode   DictionaryUpdateMode
	Consistency  DictionaryConsistencyMode
	Capabilities DictionaryCapabilities
}

// Supports reports whether this dictionary advertises capability.
func (d DictionaryDefinition) Supports(capability DictionaryCapability) bool {
	return d.Capabilities.Has(capability)
}

// EffectiveUpdateMode returns a conservative update mode when metadata is omitted.
func (d DictionaryDefinition) EffectiveUpdateMode() DictionaryUpdateMode {
	if d.UpdateMode != DictionaryUpdateUnknown {
		return d.UpdateMode
	}
	if d.Supports(DictionaryCapabilityMutable) {
		return DictionaryUpdateAppendOnly
	}
	return DictionaryUpdateStatic
}

// EffectiveConsistency returns a conservative consistency model when metadata is omitted.
func (d DictionaryDefinition) EffectiveConsistency() DictionaryConsistencyMode {
	if d.Consistency != DictionaryConsistencyUnknown {
		return d.Consistency
	}
	switch d.EffectiveUpdateMode() {
	case DictionaryUpdateAppendOnly:
		return DictionaryConsistencyVersionedDistributed
	case DictionaryUpdateAdapterOwned:
		return DictionaryConsistencyAdapterOwned
	default:
		return DictionaryConsistencySnapshot
	}
}

// AllowsMutation reports whether the dictionary may accept labels after initial load.
func (d DictionaryDefinition) AllowsMutation() bool {
	switch d.EffectiveUpdateMode() {
	case DictionaryUpdateAppendOnly, DictionaryUpdateAdapterOwned:
		return true
	default:
		return false
	}
}

// RequiresInvalidation reports whether cached dictionary readers need explicit refresh hooks.
func (d DictionaryDefinition) RequiresInvalidation() bool {
	return d.EffectiveConsistency() == DictionaryConsistencyVersionedDistributed
}

// DictionaryEntry binds one label to one encoded StringEnum id.
type DictionaryEntry struct {
	Ref     DictionaryRef
	Label   string
	ID      StringEnumID
	Version DictionaryVersion
}

// DictionaryResolver maps StringEnum labels to encoded ids and back.
//
// The resolver is a planner-facing boundary. Implementations may read from an
// in-memory cache, KVStore, Consul, an embedded store, or another backend, but
// those persistence choices should not leak into query binding or planning.
type DictionaryResolver interface {
	Dictionary(ref DictionaryRef) (DictionaryDefinition, DiagnosticSet)
	LookupLabel(ref DictionaryRef, label string) (DictionaryEntry, DiagnosticSet)
	LookupID(ref DictionaryRef, id StringEnumID) (DictionaryEntry, DiagnosticSet)
}

// MemoryDictionaryResolver is a small in-memory resolver for tests and scaffolding.
type MemoryDictionaryResolver struct {
	Dictionaries []DictionaryDefinition
	Entries      []DictionaryEntry
}

// Dictionary looks up a dictionary definition by field identity.
func (r MemoryDictionaryResolver) Dictionary(ref DictionaryRef) (DictionaryDefinition, DiagnosticSet) {
	for _, dictionary := range r.Dictionaries {
		if dictionaryRefEqual(dictionary.Ref, ref) {
			return cloneDictionaryDefinition(dictionary), nil
		}
	}
	return DictionaryDefinition{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticDictionaryNotFound, PhaseBind, "dictionary not found: "+ref.QualifiedName()),
	}
}

// LookupLabel resolves a StringEnum label to its encoded id.
func (r MemoryDictionaryResolver) LookupLabel(ref DictionaryRef, label string) (DictionaryEntry, DiagnosticSet) {
	for _, entry := range r.Entries {
		if dictionaryRefEqual(entry.Ref, ref) && entry.Label == label {
			return entry, nil
		}
	}
	return DictionaryEntry{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticDictionaryLabelNotFound, PhaseBind, "dictionary label not found: "+ref.QualifiedName()),
	}
}

// LookupID resolves an encoded StringEnum id to its label.
func (r MemoryDictionaryResolver) LookupID(ref DictionaryRef, id StringEnumID) (DictionaryEntry, DiagnosticSet) {
	for _, entry := range r.Entries {
		if dictionaryRefEqual(entry.Ref, ref) && entry.ID == id {
			return entry, nil
		}
	}
	return DictionaryEntry{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticDictionaryIDNotFound, PhaseBind, "dictionary id not found: "+ref.QualifiedName()),
	}
}

// CachedDictionaryResolver wraps another resolver with explicit process-local caching.
//
// Dictionary invalidation uses a per-field generation number. Existing entries
// are not scanned out of the sharded maps, but incrementing the generation makes
// stale entries unreachable and keeps dictionary refresh cheap.
type CachedDictionaryResolver struct {
	backend      DictionaryResolver
	dictionaries *shardedValueCache
	labels       *shardedValueCache
	ids          *shardedValueCache
	generations  *shardedValueCache
}

// NewCachedDictionaryResolver creates a process-local cache in front of backend.
func NewCachedDictionaryResolver(backend DictionaryResolver) *CachedDictionaryResolver {
	return &CachedDictionaryResolver{
		backend:      backend,
		dictionaries: newShardedValueCache(),
		labels:       newShardedValueCache(),
		ids:          newShardedValueCache(),
		generations:  newShardedValueCache(),
	}
}

// Dictionary looks up a dictionary definition through the cache.
func (r *CachedDictionaryResolver) Dictionary(ref DictionaryRef) (DictionaryDefinition, DiagnosticSet) {
	key := r.dictionaryCacheKey(ref)
	if value, ok := r.dictionaries.Get(key); ok {
		cached := value.(cachedDictionaryDefinition)
		return cloneDictionaryDefinition(cached.value), cloneDiagnosticSet(cached.diagnostics)
	}

	dictionary, diagnostics := r.backend.Dictionary(ref)
	r.dictionaries.Set(key, cachedDictionaryDefinition{
		value:       cloneDictionaryDefinition(dictionary),
		diagnostics: cloneDiagnosticSet(diagnostics),
	})
	return cloneDictionaryDefinition(dictionary), cloneDiagnosticSet(diagnostics)
}

// LookupLabel resolves a StringEnum label through the cache.
func (r *CachedDictionaryResolver) LookupLabel(ref DictionaryRef, label string) (DictionaryEntry, DiagnosticSet) {
	key := r.labelCacheKey(ref, label)
	if value, ok := r.labels.Get(key); ok {
		cached := value.(cachedDictionaryEntry)
		return cached.value, cloneDiagnosticSet(cached.diagnostics)
	}

	entry, diagnostics := r.backend.LookupLabel(ref, label)
	r.labels.Set(key, cachedDictionaryEntry{value: entry, diagnostics: cloneDiagnosticSet(diagnostics)})
	return entry, cloneDiagnosticSet(diagnostics)
}

// LookupID resolves an encoded StringEnum id through the cache.
func (r *CachedDictionaryResolver) LookupID(ref DictionaryRef, id StringEnumID) (DictionaryEntry, DiagnosticSet) {
	key := r.idCacheKey(ref, id)
	if value, ok := r.ids.Get(key); ok {
		cached := value.(cachedDictionaryEntry)
		return cached.value, cloneDiagnosticSet(cached.diagnostics)
	}

	entry, diagnostics := r.backend.LookupID(ref, id)
	r.ids.Set(key, cachedDictionaryEntry{value: entry, diagnostics: cloneDiagnosticSet(diagnostics)})
	return entry, cloneDiagnosticSet(diagnostics)
}

// InvalidateDictionary makes all cached entries for ref unreachable.
func (r *CachedDictionaryResolver) InvalidateDictionary(ref DictionaryRef) {
	key := dictionaryRefCacheKey(ref)
	generation := r.dictionaryGeneration(ref)
	r.generations.Set(key, generation+1)
}

// Clear removes all cached dictionaries and entries.
func (r *CachedDictionaryResolver) Clear() {
	r.dictionaries.Clear()
	r.labels.Clear()
	r.ids.Clear()
	r.generations.Clear()
}

type cachedDictionaryDefinition struct {
	value       DictionaryDefinition
	diagnostics DiagnosticSet
}

type cachedDictionaryEntry struct {
	value       DictionaryEntry
	diagnostics DiagnosticSet
}

// dictionaryCacheKey includes the field generation so invalidation is an O(1) version bump.
func (r *CachedDictionaryResolver) dictionaryCacheKey(ref DictionaryRef) string {
	return dictionaryRefCacheKey(ref) + "\x00" + strconv.FormatUint(r.dictionaryGeneration(ref), 10)
}

// labelCacheKey identifies one label lookup within a field dictionary generation.
func (r *CachedDictionaryResolver) labelCacheKey(ref DictionaryRef, label string) string {
	return r.dictionaryCacheKey(ref) + "\x00label\x00" + label
}

// idCacheKey identifies one encoded-id lookup within a field dictionary generation.
func (r *CachedDictionaryResolver) idCacheKey(ref DictionaryRef, id StringEnumID) string {
	return r.dictionaryCacheKey(ref) + "\x00id\x00" + strconv.FormatUint(uint64(id), 10)
}

// dictionaryGeneration returns the current cache generation for one field dictionary.
func (r *CachedDictionaryResolver) dictionaryGeneration(ref DictionaryRef) uint64 {
	value, ok := r.generations.Get(dictionaryRefCacheKey(ref))
	if !ok {
		return 0
	}
	return value.(uint64)
}

// dictionaryRefEqual compares dictionary field identity with SQL case-insensitive semantics.
func dictionaryRefEqual(left DictionaryRef, right DictionaryRef) bool {
	return strings.EqualFold(left.Schema, right.Schema) &&
		strings.EqualFold(left.Table, right.Table) &&
		strings.EqualFold(left.Field, right.Field)
}

// dictionaryRefCacheKey normalizes dictionary field identity for cache lookups.
func dictionaryRefCacheKey(ref DictionaryRef) string {
	return strings.ToLower(ref.Schema) + "\x00" + strings.ToLower(ref.Table) + "\x00" + strings.ToLower(ref.Field)
}

// cloneDictionaryDefinition returns dictionary metadata with independent capability slices.
func cloneDictionaryDefinition(dictionary DictionaryDefinition) DictionaryDefinition {
	cloned := dictionary
	cloned.Capabilities = append(DictionaryCapabilities(nil), dictionary.Capabilities...)
	return cloned
}
