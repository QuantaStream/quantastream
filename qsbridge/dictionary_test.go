package qsbridge

import "testing"

func TestDictionaryRefQualifiedName(t *testing.T) {
	ref := DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}

	if !ref.Valid() {
		t.Fatalf("expected ref to be valid")
	}
	if got, want := ref.QualifiedName(), "quanta.lineitem.l_shipmode"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
}

func TestDictionaryCapabilities(t *testing.T) {
	definition := DictionaryDefinition{
		Capabilities: DictionaryCapabilities{
			DictionaryCapabilityStableIDs,
			DictionaryCapabilityPrefixMatch,
		},
	}

	if !definition.Supports(DictionaryCapabilityPrefixMatch) {
		t.Fatalf("expected prefix capability")
	}
	if definition.Supports(DictionaryCapabilityContainsMatch) {
		t.Fatalf("did not expect contains capability")
	}
}

func TestDictionaryDefinitionDefaultsStaticSnapshot(t *testing.T) {
	definition := DictionaryDefinition{}

	if definition.EffectiveUpdateMode() != DictionaryUpdateStatic {
		t.Fatalf("EffectiveUpdateMode() = %q, want static", definition.EffectiveUpdateMode())
	}
	if definition.EffectiveConsistency() != DictionaryConsistencySnapshot {
		t.Fatalf("EffectiveConsistency() = %q, want snapshot", definition.EffectiveConsistency())
	}
	if definition.AllowsMutation() {
		t.Fatalf("static dictionary should not allow mutation")
	}
	if definition.RequiresInvalidation() {
		t.Fatalf("static snapshot should not require invalidation")
	}
}

func TestDictionaryDefinitionMutableDefaultsAppendOnlyVersioned(t *testing.T) {
	definition := DictionaryDefinition{
		Capabilities: DictionaryCapabilities{DictionaryCapabilityMutable},
	}

	if definition.EffectiveUpdateMode() != DictionaryUpdateAppendOnly {
		t.Fatalf("EffectiveUpdateMode() = %q, want append_only", definition.EffectiveUpdateMode())
	}
	if definition.EffectiveConsistency() != DictionaryConsistencyVersionedDistributed {
		t.Fatalf("EffectiveConsistency() = %q, want versioned_distributed", definition.EffectiveConsistency())
	}
	if !definition.AllowsMutation() {
		t.Fatalf("mutable dictionary should allow mutation")
	}
	if !definition.RequiresInvalidation() {
		t.Fatalf("versioned distributed dictionary should require invalidation")
	}
}

func TestDictionaryDefinitionExplicitAdapterOwnedMode(t *testing.T) {
	definition := DictionaryDefinition{
		UpdateMode:  DictionaryUpdateAdapterOwned,
		Consistency: DictionaryConsistencyAdapterOwned,
	}

	if !definition.AllowsMutation() {
		t.Fatalf("adapter-owned dictionary should allow mutation")
	}
	if definition.RequiresInvalidation() {
		t.Fatalf("adapter-owned consistency should not imply qsbridge invalidation")
	}
}

func TestMemoryDictionaryResolverLookups(t *testing.T) {
	ref := DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	resolver := MemoryDictionaryResolver{
		Dictionaries: []DictionaryDefinition{{
			Ref:          ref,
			Version:      "v1",
			Cardinality:  2,
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
		}},
		Entries: []DictionaryEntry{{
			Ref:     ref,
			Label:   "AIR",
			ID:      7,
			Version: "v1",
		}},
	}

	dictionary, diagnostics := resolver.Dictionary(DictionaryRef{Schema: "QUANTA", Table: "LINEITEM", Field: "L_SHIPMODE"})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected dictionary diagnostics: %#v", diagnostics)
	}
	if dictionary.Version != "v1" {
		t.Fatalf("Version = %q, want v1", dictionary.Version)
	}

	entry, diagnostics := resolver.LookupLabel(ref, "AIR")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected label diagnostics: %#v", diagnostics)
	}
	if entry.ID != 7 {
		t.Fatalf("ID = %d, want 7", entry.ID)
	}

	entry, diagnostics = resolver.LookupID(ref, 7)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected id diagnostics: %#v", diagnostics)
	}
	if entry.Label != "AIR" {
		t.Fatalf("Label = %q, want AIR", entry.Label)
	}
}

func TestMemoryDictionaryResolverMissingLookupsReturnDiagnostics(t *testing.T) {
	ref := DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	resolver := MemoryDictionaryResolver{}

	_, diagnostics := resolver.Dictionary(ref)
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticDictionaryNotFound)

	_, diagnostics = resolver.LookupLabel(ref, "AIR")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticDictionaryLabelNotFound)

	_, diagnostics = resolver.LookupID(ref, 7)
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticDictionaryIDNotFound)
}

func TestCachedDictionaryResolverCachesLookups(t *testing.T) {
	ref := DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	backend := &countingDictionaryResolver{
		dictionary: DictionaryDefinition{
			Ref:          ref,
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
		},
		labelEntry: DictionaryEntry{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
		idEntry:    DictionaryEntry{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
	}
	resolver := NewCachedDictionaryResolver(backend)

	for i := 0; i < 2; i++ {
		if _, diagnostics := resolver.Dictionary(ref); diagnostics.BlocksNative() {
			t.Fatalf("unexpected dictionary diagnostics: %#v", diagnostics)
		}
		if _, diagnostics := resolver.LookupLabel(ref, "AIR"); diagnostics.BlocksNative() {
			t.Fatalf("unexpected label diagnostics: %#v", diagnostics)
		}
		if _, diagnostics := resolver.LookupID(ref, 7); diagnostics.BlocksNative() {
			t.Fatalf("unexpected id diagnostics: %#v", diagnostics)
		}
	}

	if backend.dictionaryCalls != 1 {
		t.Fatalf("dictionary calls = %d, want 1", backend.dictionaryCalls)
	}
	if backend.labelCalls != 1 {
		t.Fatalf("label calls = %d, want 1", backend.labelCalls)
	}
	if backend.idCalls != 1 {
		t.Fatalf("id calls = %d, want 1", backend.idCalls)
	}
}

func TestCachedDictionaryResolverCachesDiagnosticsUntilInvalidated(t *testing.T) {
	ref := DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	backend := &countingDictionaryResolver{
		labelDiagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticDictionaryLabelNotFound, PhaseBind, "missing label"),
		},
	}
	resolver := NewCachedDictionaryResolver(backend)

	for i := 0; i < 2; i++ {
		_, diagnostics := resolver.LookupLabel(ref, "AIR")
		assertSingleDiagnosticCode(t, diagnostics, DiagnosticDictionaryLabelNotFound)
	}
	if backend.labelCalls != 1 {
		t.Fatalf("label calls = %d, want 1 cached miss", backend.labelCalls)
	}

	backend.labelEntry = DictionaryEntry{Ref: ref, Label: "AIR", ID: 7, Version: "v2"}
	backend.labelDiagnostics = nil
	resolver.InvalidateDictionary(ref)
	entry, diagnostics := resolver.LookupLabel(ref, "AIR")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics after invalidation: %#v", diagnostics)
	}
	if entry.ID != 7 || entry.Version != "v2" {
		t.Fatalf("entry = %#v, want AIR id 7 at v2", entry)
	}
	if backend.labelCalls != 2 {
		t.Fatalf("label calls = %d, want refresh after invalidation", backend.labelCalls)
	}
}

func TestCachedDictionaryResolverClearRefreshesAllLookups(t *testing.T) {
	ref := DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	backend := &countingDictionaryResolver{
		dictionary: DictionaryDefinition{Ref: ref, Version: "v1"},
		labelEntry: DictionaryEntry{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
		idEntry:    DictionaryEntry{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
	}
	resolver := NewCachedDictionaryResolver(backend)

	_, _ = resolver.Dictionary(ref)
	_, _ = resolver.LookupLabel(ref, "AIR")
	_, _ = resolver.LookupID(ref, 7)
	resolver.Clear()
	_, _ = resolver.Dictionary(ref)
	_, _ = resolver.LookupLabel(ref, "AIR")
	_, _ = resolver.LookupID(ref, 7)

	if backend.dictionaryCalls != 2 || backend.labelCalls != 2 || backend.idCalls != 2 {
		t.Fatalf(
			"calls = dictionary:%d label:%d id:%d, want all refreshed",
			backend.dictionaryCalls,
			backend.labelCalls,
			backend.idCalls,
		)
	}
}

func TestCachedDictionaryResolverReturnsDefinitionCopies(t *testing.T) {
	ref := DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	backend := &countingDictionaryResolver{
		dictionary: DictionaryDefinition{
			Ref:          ref,
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
		},
	}
	resolver := NewCachedDictionaryResolver(backend)

	dictionary, _ := resolver.Dictionary(ref)
	dictionary.Capabilities[0] = DictionaryCapabilityMutable
	dictionary, _ = resolver.Dictionary(ref)
	if dictionary.Capabilities[0] != DictionaryCapabilityStableIDs {
		t.Fatalf("cached dictionary mutation leaked: %#v", dictionary.Capabilities)
	}
}

type countingDictionaryResolver struct {
	dictionary            DictionaryDefinition
	dictionaryDiagnostics DiagnosticSet
	dictionaryCalls       int
	labelEntry            DictionaryEntry
	labelDiagnostics      DiagnosticSet
	labelCalls            int
	idEntry               DictionaryEntry
	idDiagnostics         DiagnosticSet
	idCalls               int
}

func (r *countingDictionaryResolver) Dictionary(DictionaryRef) (DictionaryDefinition, DiagnosticSet) {
	r.dictionaryCalls++
	return r.dictionary, r.dictionaryDiagnostics
}

func (r *countingDictionaryResolver) LookupLabel(DictionaryRef, string) (DictionaryEntry, DiagnosticSet) {
	r.labelCalls++
	return r.labelEntry, r.labelDiagnostics
}

func (r *countingDictionaryResolver) LookupID(DictionaryRef, StringEnumID) (DictionaryEntry, DiagnosticSet) {
	r.idCalls++
	return r.idEntry, r.idDiagnostics
}
