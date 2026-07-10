package qsbridge

import "testing"

func TestEncodingProfileStringLexBSIRequiresLookupForLongValues(t *testing.T) {
	profile := EncodingProfile{
		Kind:           EncodingStringLexBSI,
		LegacyName:     "StringHashBSI",
		PrefixLength:   16,
		MaxLength:      -1,
		RemainderStore: "kv",
		Rehydration:    RehydrationProfile{Kind: RehydrationLookup, Store: "kv"},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
			PredicateCapabilityPrefix,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityLookup,
			ProjectionCapabilityOriginalValue,
		},
	}

	if !profile.Lossless() {
		t.Fatalf("expected lookup-backed string lex encoding to be lossless")
	}
	if !profile.RequiresLookup() {
		t.Fatalf("expected long string projection to require lookup")
	}
	if profile.HasBoundedStringLength() {
		t.Fatalf("expected unbounded max length to be treated as lookup-backed")
	}
	if profile.StoresFullStringInline() {
		t.Fatalf("expected unbounded string lex profile not to be fully inline")
	}
	if !profile.NeedsStringRemainderLookup() {
		t.Fatalf("expected unbounded string lex profile to need remainder lookup")
	}
	if !profile.HasStringRemainderStore() {
		t.Fatalf("expected string lex profile to name its remainder store")
	}
	if !profile.SupportsPrefix() {
		t.Fatalf("expected lex BSI profile to support prefix predicates")
	}
	if !profile.SupportsProjection(ProjectionCapabilityOriginalValue) {
		t.Fatalf("expected original value projection capability")
	}
	if profile.Searchable() {
		t.Fatalf("did not expect search behavior without explicit search profile")
	}
}

func TestEncodingProfileStringLexBSIInlineWhenPrefixCoversMaxLength(t *testing.T) {
	profile := EncodingProfile{
		Kind:         EncodingStringLexBSI,
		PrefixLength: 16,
		MaxLength:    16,
		Rehydration:  RehydrationProfile{Kind: RehydrationInline},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
			PredicateCapabilityPrefix,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityInline,
			ProjectionCapabilityOriginalValue,
		},
	}

	if !profile.HasBoundedStringLength() {
		t.Fatalf("expected finite max length")
	}
	if !profile.StoresFullStringInline() {
		t.Fatalf("expected prefix length covering max length to store full string inline")
	}
	if profile.NeedsStringRemainderLookup() {
		t.Fatalf("did not expect a remainder lookup when prefix covers max length")
	}
	if profile.HasStringRemainderStore() {
		t.Fatalf("did not expect a remainder store for fully inline strings")
	}
}

func TestNewStringLexBSIProfileChoosesInlineOrLookupRehydration(t *testing.T) {
	inline := NewStringLexBSIProfile(StringLexBSIOptions{
		PrefixLength: 12,
		MaxLength:    12,
	})
	if !inline.StoresFullStringInline() || inline.RequiresLookup() || !inline.SupportsProjection(ProjectionCapabilityInline) {
		t.Fatalf("inline profile = %#v, want fully inline string lex BSI", inline)
	}
	if !inline.SupportsPredicate(PredicateCapabilityPrefix) || !inline.SupportsPredicate(PredicateCapabilityRange) {
		t.Fatalf("inline predicate capabilities = %#v, want prefix and range", inline.PredicateCapabilities)
	}

	lookup := NewStringLexBSIProfile(StringLexBSIOptions{
		PrefixLength:   12,
		MaxLength:      64,
		RemainderStore: "kv",
		Searchable:     true,
		SearchMode:     "text",
	})
	if !lookup.NeedsStringRemainderLookup() || !lookup.RequiresLookup() || lookup.Rehydration.Store != "kv" {
		t.Fatalf("lookup profile = %#v, want KV-backed string remainder lookup", lookup)
	}
	if !lookup.Searchable() || !lookup.SupportsPredicate(PredicateCapabilityTextSearch) {
		t.Fatalf("lookup profile = %#v, want explicit text-search capability", lookup)
	}
	if lookup.SupportsPredicate(PredicateCapabilityContains) {
		t.Fatalf("string lex text-search capability must not imply contains LIKE support: %#v", lookup.PredicateCapabilities)
	}
}

func TestEncodingProfileTimeGranularityAndNumericScale(t *testing.T) {
	timeProfile := EncodingProfile{
		Kind:        EncodingTimeBSI,
		LegacyName:  "SysMillisBSI",
		Granularity: TimeGranularityMillisecond,
		Rehydration: RehydrationProfile{Kind: RehydrationInline},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityRange,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityInline,
			ProjectionCapabilityOriginalValue,
		},
	}
	if timeProfile.Granularity != TimeGranularityMillisecond {
		t.Fatalf("Granularity = %q, want %q", timeProfile.Granularity, TimeGranularityMillisecond)
	}
	if !timeProfile.Lossless() || timeProfile.RequiresLookup() {
		t.Fatalf("expected inline millisecond time rehydration")
	}
	if !timeProfile.SupportsPredicate(PredicateCapabilityRange) {
		t.Fatalf("expected time BSI range capability")
	}

	numericProfile := EncodingProfile{
		Kind:        EncodingNumericBSI,
		LegacyName:  "FloatScaleBSI",
		Scale:       4,
		Signed:      true,
		Rehydration: RehydrationProfile{Kind: RehydrationInline},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
		},
	}
	if numericProfile.Scale != 4 {
		t.Fatalf("Scale = %d, want 4", numericProfile.Scale)
	}
	if !numericProfile.SupportsPredicate(PredicateCapabilityRange) {
		t.Fatalf("expected numeric range capability")
	}
}

func TestNewTimeAndNumericBSIProfilesPreserveRepresentationOptions(t *testing.T) {
	timeProfile := NewTimeBSIProfile(TimeGranularitySecond)
	if timeProfile.Kind != EncodingTimeBSI || timeProfile.Granularity != TimeGranularitySecond {
		t.Fatalf("time profile = %#v, want second-granularity time BSI", timeProfile)
	}
	if !timeProfile.Lossless() || !timeProfile.SupportsPredicate(PredicateCapabilityRange) {
		t.Fatalf("time profile = %#v, want lossless range-capable encoding", timeProfile)
	}

	numericProfile := NewNumericBSIProfile(6, false)
	if numericProfile.Kind != EncodingNumericBSI || numericProfile.Scale != 6 || numericProfile.Signed {
		t.Fatalf("numeric profile = %#v, want unsigned scaled numeric BSI", numericProfile)
	}
	if !numericProfile.Lossless() || !numericProfile.SupportsProjection(ProjectionCapabilityOriginalValue) {
		t.Fatalf("numeric profile = %#v, want lossless original-value projection", numericProfile)
	}
}

func TestLegacyEncodingProfileMapsSysMicroBSIToTimeBSI(t *testing.T) {
	profile := LegacyEncodingProfile("SysMicroBSI", LegacyEncodingOptions{})
	if profile.Kind != EncodingTimeBSI || profile.LegacyName != "TimeStampBSI" {
		t.Fatalf("profile = %#v, want canonical TimeStampBSI time BSI", profile)
	}
	if profile.Granularity != TimeGranularityMicrosecond {
		t.Fatalf("Granularity = %q, want %q", profile.Granularity, TimeGranularityMicrosecond)
	}
	if !profile.SupportsPredicate(PredicateCapabilityRange) || !profile.Lossless() {
		t.Fatalf("profile = %#v, want range-capable lossless time BSI", profile)
	}
}

func TestEncodingProfilePredicateOnlyIsNotLossless(t *testing.T) {
	profile := EncodingProfile{
		Kind: EncodingTextSearch,
		Search: SearchProfile{
			Enabled: true,
			Mode:    "substring",
		},
		Rehydration: RehydrationProfile{Kind: RehydrationPredicateOnly},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityTextSearch,
			PredicateCapabilityContains,
		},
	}

	if profile.Lossless() {
		t.Fatalf("predicate-only encoding must not be treated as lossless")
	}
	if profile.RequiresLookup() {
		t.Fatalf("predicate-only encoding is not a projection lookup path")
	}
	if !profile.Searchable() {
		t.Fatalf("expected search profile to advertise searchable")
	}
	if !profile.SupportsPredicate(PredicateCapabilityContains) {
		t.Fatalf("expected contains capability")
	}
}

func TestEncodingProfileMultiplicityDefaultsToScalar(t *testing.T) {
	defaultProfile := EncodingProfile{Kind: EncodingBitmap}
	if defaultProfile.EffectiveMultiplicity() != MultiplicityScalar {
		t.Fatalf("default multiplicity = %q, want %q", defaultProfile.EffectiveMultiplicity(), MultiplicityScalar)
	}
	if defaultProfile.IsSetValued() {
		t.Fatalf("default bitmap encoding should be scalar-valued")
	}

	scalarProfile := EncodingProfile{
		Kind:         EncodingBitmap,
		Multiplicity: MultiplicityScalar,
	}
	if scalarProfile.EffectiveMultiplicity() != MultiplicityScalar {
		t.Fatalf("scalar multiplicity = %q, want %q", scalarProfile.EffectiveMultiplicity(), MultiplicityScalar)
	}

	setProfile := EncodingProfile{
		Kind:         EncodingBitmap,
		Multiplicity: MultiplicitySet,
	}
	if setProfile.EffectiveMultiplicity() != MultiplicitySet {
		t.Fatalf("set multiplicity = %q, want %q", setProfile.EffectiveMultiplicity(), MultiplicitySet)
	}
	if !setProfile.IsSetValued() {
		t.Fatalf("expected set-valued bitmap encoding")
	}
}

func TestRelationshipEncodingProfileCapabilities(t *testing.T) {
	profile := RelationshipEncodingProfile{
		Kind:       RelationshipEncodingVector,
		LegacyName: "ParentRelation",
		Capabilities: RelationshipCapabilities{
			RelationshipCapabilityParentLookup,
			RelationshipCapabilityJoinReduction,
			RelationshipCapabilitySemiJoin,
			RelationshipCapabilityAntiJoinDifference,
		},
	}

	if !profile.Supports(RelationshipCapabilityParentLookup) {
		t.Fatalf("expected parent lookup capability")
	}
	if !profile.SupportsAntiJoinDifference() {
		t.Fatalf("expected anti-join difference capability")
	}
	if profile.Supports(RelationshipCapabilityNullExtension) {
		t.Fatalf("did not expect outer join null-extension without explicit capability")
	}
}
