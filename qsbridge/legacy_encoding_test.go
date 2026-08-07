package qsbridge

import "testing"

func TestLegacyEncodingProfileMapsStringEnum(t *testing.T) {
	profile := LegacyEncodingProfile("StringEnum", LegacyEncodingOptions{})

	if profile.Kind != EncodingStringEnum {
		t.Fatalf("Kind = %q, want %q", profile.Kind, EncodingStringEnum)
	}
	if profile.EffectiveMultiplicity() != MultiplicityScalar {
		t.Fatalf("Multiplicity = %q, want %q", profile.EffectiveMultiplicity(), MultiplicityScalar)
	}
	if !profile.RequiresLookup() {
		t.Fatalf("expected StringEnum projection to require dictionary lookup")
	}
	if !profile.SupportsPredicate(PredicateCapabilityPrefix) {
		t.Fatalf("expected StringEnum compatibility profile to advertise prefix predicate capability")
	}
}

func TestLegacyEncodingProfileMapsNonExclusiveStringEnumToSetMultiplicity(t *testing.T) {
	profile := LegacyEncodingProfile("StringEnum", LegacyEncodingOptions{NonExclusive: true})

	if profile.Kind != EncodingStringEnum {
		t.Fatalf("Kind = %q, want %q", profile.Kind, EncodingStringEnum)
	}
	if profile.EffectiveMultiplicity() != MultiplicitySet {
		t.Fatalf("Multiplicity = %q, want %q", profile.EffectiveMultiplicity(), MultiplicitySet)
	}
	if !profile.SupportsPredicate(PredicateCapabilityMembership) {
		t.Fatalf("expected set-valued StringEnum to preserve membership predicate capability")
	}
}

func TestLegacyEncodingProfileMapsStringLexBSI(t *testing.T) {
	profile := LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{
		Searchable:     true,
		PrefixLength:   8,
		MaxLength:      -1,
		RemainderStore: "kv",
	})

	if profile.Kind != EncodingStringLexBSI || profile.LegacyName != "StringLexBSI" {
		t.Fatalf("profile = %#v, want StringLexBSI encoding", profile)
	}
	if profile.PrefixLength != 8 || !profile.NeedsStringRemainderLookup() || profile.Rehydration.Store != "kv" {
		t.Fatalf("profile = %#v, want eight-byte prefix with KV remainder", profile)
	}
	if !profile.SupportsPredicate(PredicateCapabilityRange) || !profile.SupportsPredicate(PredicateCapabilityPrefix) {
		t.Fatalf("predicate capabilities = %#v, want range and prefix", profile.PredicateCapabilities)
	}
	if !profile.Searchable() || !profile.SupportsPredicate(PredicateCapabilityTextSearch) {
		t.Fatalf("profile = %#v, want searchable lex BSI", profile)
	}
}

func TestLegacyEncodingProfileMapsNumericAndTimeBSIs(t *testing.T) {
	floatProfile := LegacyEncodingProfile("FloatScaleBSI", LegacyEncodingOptions{Scale: 4})
	if floatProfile.Kind != EncodingNumericBSI || floatProfile.Scale != 4 {
		t.Fatalf("float profile = %#v, want numeric BSI scale 4", floatProfile)
	}
	if !floatProfile.Lossless() || floatProfile.RequiresLookup() {
		t.Fatalf("expected inline lossless float scale profile")
	}

	intProfile := LegacyEncodingProfile("IntBSI", LegacyEncodingOptions{})
	if intProfile.Kind != EncodingNumericBSI || intProfile.Scale != 0 {
		t.Fatalf("int profile = %#v, want numeric BSI scale 0", intProfile)
	}

	timeProfile := LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})
	if timeProfile.Kind != EncodingTimeBSI || timeProfile.Granularity != TimeGranularityMillisecond {
		t.Fatalf("time profile = %#v, want millisecond time BSI", timeProfile)
	}
	if timeProfile.LegacyName != "TimestampBSI" {
		t.Fatalf("LegacyName = %q, want canonical TimestampBSI", timeProfile.LegacyName)
	}
	if !timeProfile.SupportsPredicate(PredicateCapabilityRange) {
		t.Fatalf("expected time range capability")
	}
}

func TestLegacyEncodingProfileConsolidatesTimestampBSIAliases(t *testing.T) {
	tests := []struct {
		name        string
		granularity TimeGranularity
	}{
		{name: "SysSecBSI", granularity: TimeGranularitySecond},
		{name: "SysMillisBSI", granularity: TimeGranularityMillisecond},
		{name: "SysMicroBSI", granularity: TimeGranularityMicrosecond},
		{name: "SysNanoBSI", granularity: TimeGranularityNanosecond},
	}
	for _, test := range tests {
		profile := LegacyEncodingProfile(test.name, LegacyEncodingOptions{})
		if profile.Kind != EncodingTimeBSI || profile.LegacyName != "TimestampBSI" || profile.Granularity != test.granularity {
			t.Fatalf("%s profile = %#v, want canonical TimestampBSI/%s", test.name, profile, test.granularity)
		}
		if !profile.SupportsPredicate(PredicateCapabilityEquality) || !profile.SupportsPredicate(PredicateCapabilityRange) {
			t.Fatalf("%s profile = %#v, want equality and range capabilities", test.name, profile)
		}
	}
}

func TestLegacyEncodingProfileMapsCanonicalTimestampBSIWithGranularity(t *testing.T) {
	profile := LegacyEncodingProfile("TimestampBSI", LegacyEncodingOptions{Granularity: TimeGranularityMicrosecond})
	if profile.Kind != EncodingTimeBSI || profile.LegacyName != "TimestampBSI" {
		t.Fatalf("profile = %#v, want canonical TimestampBSI", profile)
	}
	if profile.Granularity != TimeGranularityMicrosecond {
		t.Fatalf("Granularity = %q, want %q", profile.Granularity, TimeGranularityMicrosecond)
	}
}

func TestLegacyEncodingProfileMapsParentRelationAsBridgeEncoding(t *testing.T) {
	profile := LegacyEncodingProfile("ParentRelation", LegacyEncodingOptions{NonExclusive: true})

	if profile.Kind != EncodingRelation {
		t.Fatalf("Kind = %q, want %q", profile.Kind, EncodingRelation)
	}
	if profile.EffectiveMultiplicity() != MultiplicityScalar {
		t.Fatalf("ParentRelation should remain scalar even if old options are noisy")
	}
	if !profile.SupportsPredicate(PredicateCapabilityMembership) {
		t.Fatalf("expected relation membership capability")
	}
}

func TestLegacyEncodingProfileMapsNonExclusiveBitmapToSetMultiplicity(t *testing.T) {
	profile := LegacyEncodingProfile("unknown_bitmap", LegacyEncodingOptions{NonExclusive: true})

	if profile.Kind != EncodingUnknown {
		t.Fatalf("Kind = %q, want unknown", profile.Kind)
	}
	if !profile.IsSetValued() {
		t.Fatalf("expected non-exclusive legacy option to map to set multiplicity")
	}
}
