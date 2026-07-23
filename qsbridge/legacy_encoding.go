package qsbridge

import "strings"

const (
	legacyStringEnum     = "StringEnum"
	legacyStringHashBSI  = "StringHashBSI"
	legacyStringLexBSI   = "StringLexBSI"
	legacyFloatScaleBSI  = "FloatScaleBSI"
	legacyIntBSI         = "IntBSI"
	legacyTimeStampBSI   = "TimeStampBSI"
	legacySysSecBSI      = "SysSecBSI"
	legacySysMillisBSI   = "SysMillisBSI"
	legacySysMicroBSI    = "SysMicroBSI"
	legacySysNanoBSI     = "SysNanoBSI"
	legacyParentRelation = "ParentRelation"
)

// LegacyEncodingOptions carries schema details needed to map old storage names.
type LegacyEncodingOptions struct {
	NonExclusive   bool
	Searchable     bool
	Scale          int
	Granularity    TimeGranularity
	PrefixLength   int
	MaxLength      int
	RemainderStore string
}

// LegacyEncodingProfile maps current Quanta storage names into the qsbridge encoding model.
//
// The mapping is intentionally conservative. It records compatibility semantics
// without claiming future capabilities such as StringLexBSI prefix behavior for
// legacy StringHashBSI fields.
func LegacyEncodingProfile(legacyName string, options LegacyEncodingOptions) EncodingProfile {
	multiplicity := MultiplicityScalar
	if options.NonExclusive {
		multiplicity = MultiplicitySet
	}

	switch normalizedLegacyEncodingName(legacyName) {
	case strings.ToLower(legacyStringEnum):
		return EncodingProfile{
			Kind:         EncodingStringEnum,
			LegacyName:   legacyStringEnum,
			Multiplicity: multiplicity,
			Rehydration:  RehydrationProfile{Kind: RehydrationLookup, Store: "dictionary"},
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityEquality,
				PredicateCapabilityMembership,
				PredicateCapabilityPrefix,
			},
			ProjectionCapabilities: ProjectionCapabilities{
				ProjectionCapabilityLookup,
				ProjectionCapabilityOriginalValue,
			},
		}
	case strings.ToLower(legacyStringHashBSI):
		return EncodingProfile{
			Kind:         EncodingBackingString,
			LegacyName:   legacyStringHashBSI,
			Multiplicity: multiplicity,
			Search:       SearchProfile{Enabled: options.Searchable, Mode: legacySearchMode(options.Searchable)},
			Rehydration:  RehydrationProfile{Kind: RehydrationLookup, Store: "kv"},
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityEquality,
			},
			ProjectionCapabilities: ProjectionCapabilities{
				ProjectionCapabilityLookup,
				ProjectionCapabilityOriginalValue,
			},
		}
	case strings.ToLower(legacyStringLexBSI):
		profile := NewStringLexBSIProfile(StringLexBSIOptions{
			PrefixLength:   options.PrefixLength,
			MaxLength:      options.MaxLength,
			RemainderStore: legacyStringLexBSIRemainderStore(options.RemainderStore, options.PrefixLength),
			Searchable:     options.Searchable,
			SearchMode:     legacySearchMode(options.Searchable),
		})
		profile.LegacyName = legacyStringLexBSI
		profile.Multiplicity = multiplicity
		return profile
	case strings.ToLower(legacyFloatScaleBSI):
		profile := NewNumericBSIProfile(options.Scale, true)
		profile.LegacyName = legacyFloatScaleBSI
		profile.Multiplicity = multiplicity
		return profile
	case strings.ToLower(legacyIntBSI):
		profile := NewNumericBSIProfile(0, true)
		profile.LegacyName = legacyIntBSI
		profile.Multiplicity = multiplicity
		return profile
	case strings.ToLower(legacyTimeStampBSI):
		profile := NewTimeBSIProfile(legacyTimeGranularityOrDefault(options.Granularity))
		profile.LegacyName = legacyTimeStampBSI
		profile.Multiplicity = multiplicity
		return profile
	case strings.ToLower(legacySysSecBSI):
		return legacyTimeBSICompatibilityProfile(TimeGranularitySecond, multiplicity)
	case strings.ToLower(legacySysMillisBSI):
		return legacyTimeBSICompatibilityProfile(TimeGranularityMillisecond, multiplicity)
	case strings.ToLower(legacySysMicroBSI):
		return legacyTimeBSICompatibilityProfile(TimeGranularityMicrosecond, multiplicity)
	case strings.ToLower(legacySysNanoBSI):
		return legacyTimeBSICompatibilityProfile(TimeGranularityNanosecond, multiplicity)
	case strings.ToLower(legacyParentRelation):
		return EncodingProfile{
			Kind:         EncodingRelation,
			LegacyName:   legacyParentRelation,
			Multiplicity: MultiplicityScalar,
			Rehydration:  RehydrationProfile{Kind: RehydrationInline},
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityEquality,
				PredicateCapabilityMembership,
			},
			ProjectionCapabilities: ProjectionCapabilities{
				ProjectionCapabilityInline,
				ProjectionCapabilityOriginalValue,
			},
		}
	default:
		return EncodingProfile{
			Kind:         EncodingUnknown,
			LegacyName:   legacyName,
			Multiplicity: multiplicity,
		}
	}
}

func legacyTimeBSICompatibilityProfile(granularity TimeGranularity, multiplicity ValueMultiplicity) EncodingProfile {
	profile := NewTimeBSIProfile(granularity)
	profile.LegacyName = legacyTimeStampBSI
	profile.Multiplicity = multiplicity
	return profile
}

func legacyTimeGranularityOrDefault(granularity TimeGranularity) TimeGranularity {
	if granularity == TimeGranularityUnknown {
		return TimeGranularityMillisecond
	}
	return granularity
}

func normalizedLegacyEncodingName(legacyName string) string {
	return strings.ToLower(strings.TrimSpace(legacyName))
}

func legacySearchMode(searchable bool) string {
	if searchable {
		return "text"
	}
	return ""
}

func legacyStringLexBSIRemainderStore(store string, prefixLength int) string {
	if prefixLength <= 0 {
		return ""
	}
	if strings.TrimSpace(store) != "" {
		return store
	}
	return "kv"
}
