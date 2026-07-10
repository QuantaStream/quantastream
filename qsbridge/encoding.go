package qsbridge

// EncodingKind identifies the physical representation used for one field.
type EncodingKind string

const (
	// EncodingUnknown means the physical representation has not been classified.
	EncodingUnknown EncodingKind = ""
	// EncodingBitmap stores values as ordinary bitmaps.
	EncodingBitmap EncodingKind = "bitmap"
	// EncodingNumericBSI stores integer, decimal, or floating values as scaled BSI integers.
	EncodingNumericBSI EncodingKind = "numeric_bsi"
	// EncodingTimeBSI stores time values as BSI integers at a configured granularity.
	EncodingTimeBSI EncodingKind = "time_bsi"
	// EncodingStringEnum stores low-cardinality strings as dictionary ids.
	EncodingStringEnum EncodingKind = "string_enum"
	// EncodingBackingString stores ordinary strings that may need side lookup or residual evaluation.
	EncodingBackingString EncodingKind = "backing_string"
	// EncodingStringLexBSI stores strings using a lexical BSI prefix and optional remainder lookup.
	EncodingStringLexBSI EncodingKind = "string_lex_bsi"
	// EncodingTextSearch stores searchable text index state separate from SQL LIKE semantics.
	EncodingTextSearch EncodingKind = "text_search"
	// EncodingRelation stores parent or child relationship mappings.
	EncodingRelation EncodingKind = "relation"
)

// TimeGranularity identifies the unit preserved by a time BSI encoding.
type TimeGranularity string

const (
	// TimeGranularityUnknown means no time unit has been selected.
	TimeGranularityUnknown TimeGranularity = ""
	// TimeGranularityDay preserves day-level time values.
	TimeGranularityDay TimeGranularity = "day"
	// TimeGranularitySecond preserves second-level time values.
	TimeGranularitySecond TimeGranularity = "second"
	// TimeGranularityMillisecond preserves millisecond-level time values.
	TimeGranularityMillisecond TimeGranularity = "millisecond"
	// TimeGranularityMicrosecond preserves microsecond-level time values.
	TimeGranularityMicrosecond TimeGranularity = "microsecond"
	// TimeGranularityNanosecond preserves nanosecond-level time values.
	TimeGranularityNanosecond TimeGranularity = "nanosecond"
)

// RehydrationKind describes whether an encoded value can be projected back to SQL.
type RehydrationKind string

const (
	// RehydrationUnknown means projection behavior has not been classified.
	RehydrationUnknown RehydrationKind = ""
	// RehydrationInline means the original SQL value can be reconstructed from encoded state alone.
	RehydrationInline RehydrationKind = "inline"
	// RehydrationLookup means projection requires a dictionary, KV, or other side lookup.
	RehydrationLookup RehydrationKind = "lookup"
	// RehydrationPredicateOnly means the encoding can filter but cannot project the original value.
	RehydrationPredicateOnly RehydrationKind = "predicate_only"
	// RehydrationLossy means the encoding cannot faithfully reconstruct the original SQL value.
	RehydrationLossy RehydrationKind = "lossy"
)

// ValueMultiplicity describes whether a field stores one value or a set of states per row.
type ValueMultiplicity string

const (
	// MultiplicityUnknown means multiplicity was not specified and should default to scalar.
	MultiplicityUnknown ValueMultiplicity = ""
	// MultiplicityScalar means at most one encoded value is active for each row.
	MultiplicityScalar ValueMultiplicity = "scalar"
	// MultiplicitySet means zero or more encoded states may be active for each row.
	MultiplicitySet ValueMultiplicity = "set"
)

// Effective returns the semantic multiplicity, defaulting unspecified fields to scalar.
func (m ValueMultiplicity) Effective() ValueMultiplicity {
	if m == MultiplicityUnknown {
		return MultiplicityScalar
	}
	return m
}

// PredicateCapability identifies a predicate shape an encoding can evaluate natively.
type PredicateCapability string

const (
	// PredicateCapabilityEquality means equality predicates can be native.
	PredicateCapabilityEquality PredicateCapability = "equality"
	// PredicateCapabilityMembership means IN or NOT IN predicates can be native.
	PredicateCapabilityMembership PredicateCapability = "membership"
	// PredicateCapabilityRange means ordered range comparisons can be native.
	PredicateCapabilityRange PredicateCapability = "range"
	// PredicateCapabilityPrefix means anchored prefix matching can be native.
	PredicateCapabilityPrefix PredicateCapability = "prefix"
	// PredicateCapabilityContains means contains matching can be native.
	PredicateCapabilityContains PredicateCapability = "contains"
	// PredicateCapabilityTextSearch means text-search predicates can be native.
	PredicateCapabilityTextSearch PredicateCapability = "text_search"
)

// PredicateCapabilities is a set of native predicate capabilities.
type PredicateCapabilities []PredicateCapability

// Has reports whether the predicate capability set contains capability.
func (c PredicateCapabilities) Has(capability PredicateCapability) bool {
	for _, item := range c {
		if item == capability {
			return true
		}
	}
	return false
}

// ProjectionCapability identifies a projection shape an encoding can satisfy.
type ProjectionCapability string

const (
	// ProjectionCapabilityInline means values can be projected from encoded state alone.
	ProjectionCapabilityInline ProjectionCapability = "inline"
	// ProjectionCapabilityLookup means values can be projected after side lookup.
	ProjectionCapabilityLookup ProjectionCapability = "lookup"
	// ProjectionCapabilityOriginalValue means projection preserves the original SQL value.
	ProjectionCapabilityOriginalValue ProjectionCapability = "original_value"
)

// ProjectionCapabilities is a set of projection capabilities.
type ProjectionCapabilities []ProjectionCapability

// Has reports whether the projection capability set contains capability.
func (c ProjectionCapabilities) Has(capability ProjectionCapability) bool {
	for _, item := range c {
		if item == capability {
			return true
		}
	}
	return false
}

// RelationshipEncodingKind identifies the physical representation behind a relationship edge.
type RelationshipEncodingKind string

const (
	// RelationshipEncodingUnknown means relationship traversal storage has not been classified.
	RelationshipEncodingUnknown RelationshipEncodingKind = ""
	// RelationshipEncodingVector stores row-to-row relationship vectors such as legacy ParentRelation data.
	RelationshipEncodingVector RelationshipEncodingKind = "relation_vector"
	// RelationshipEncodingBitmap stores relationship state in ordinary bitmaps.
	RelationshipEncodingBitmap RelationshipEncodingKind = "relation_bitmap"
	// RelationshipEncodingBSI stores relationship state in a BSI-backed representation.
	RelationshipEncodingBSI RelationshipEncodingKind = "relation_bsi"
)

// RelationshipCapability identifies a native traversal or reduction supported by a relationship encoding.
type RelationshipCapability string

const (
	// RelationshipCapabilityParentLookup means child rows can resolve their parent row.
	RelationshipCapabilityParentLookup RelationshipCapability = "parent_lookup"
	// RelationshipCapabilityChildExpansion means parent rows can expand to matching child rows.
	RelationshipCapabilityChildExpansion RelationshipCapability = "child_expansion"
	// RelationshipCapabilityJoinReduction means related found sets can reduce each other.
	RelationshipCapabilityJoinReduction RelationshipCapability = "join_reduction"
	// RelationshipCapabilitySemiJoin means IN/EXISTS-style membership can be evaluated through the relationship.
	RelationshipCapabilitySemiJoin RelationshipCapability = "semi_join"
	// RelationshipCapabilityAntiJoinDifference means NOT IN/NOT EXISTS-style exclusion can use bitmap difference.
	RelationshipCapabilityAntiJoinDifference RelationshipCapability = "anti_join_difference"
	// RelationshipCapabilityNullExtension means unmatched rows can be preserved for outer-join output.
	RelationshipCapabilityNullExtension RelationshipCapability = "null_extension"
)

// RelationshipCapabilities is a set of native relationship capabilities.
type RelationshipCapabilities []RelationshipCapability

// Has reports whether the relationship capability set contains capability.
func (c RelationshipCapabilities) Has(capability RelationshipCapability) bool {
	for _, item := range c {
		if item == capability {
			return true
		}
	}
	return false
}

// RelationshipEncodingProfile describes how a logical relationship is represented physically.
type RelationshipEncodingProfile struct {
	Kind         RelationshipEncodingKind
	LegacyName   string
	Capabilities RelationshipCapabilities
}

// Supports reports whether the relationship encoding can perform capability natively.
func (r RelationshipEncodingProfile) Supports(capability RelationshipCapability) bool {
	return r.Capabilities.Has(capability)
}

// SupportsAntiJoinDifference reports whether exclusion can be implemented as bitmap difference.
func (r RelationshipEncodingProfile) SupportsAntiJoinDifference() bool {
	return r.Supports(RelationshipCapabilityAntiJoinDifference)
}

// SearchProfile describes optional text-search behavior for a field encoding.
type SearchProfile struct {
	Enabled bool
	Mode    string
}

// RehydrationProfile describes how encoded values become SQL result values.
type RehydrationProfile struct {
	Kind  RehydrationKind
	Store string
}

// Lossless reports whether rehydration preserves the original SQL value.
func (r RehydrationProfile) Lossless() bool {
	return r.Kind == RehydrationInline || r.Kind == RehydrationLookup
}

// RequiresLookup reports whether projection needs a side store.
func (r RehydrationProfile) RequiresLookup() bool {
	return r.Kind == RehydrationLookup
}

// EncodingProfile describes the physical encoding and native capabilities of one field.
type EncodingProfile struct {
	Kind                   EncodingKind
	LegacyName             string
	Multiplicity           ValueMultiplicity
	Granularity            TimeGranularity
	Scale                  int
	Signed                 bool
	PrefixLength           int
	MaxLength              int
	RemainderStore         string
	Search                 SearchProfile
	Rehydration            RehydrationProfile
	PredicateCapabilities  PredicateCapabilities
	ProjectionCapabilities ProjectionCapabilities
}

// StringLexBSIOptions configures a lexical string BSI profile.
type StringLexBSIOptions struct {
	PrefixLength   int
	MaxLength      int
	RemainderStore string
	Searchable     bool
	SearchMode     string
}

// NewStringLexBSIProfile returns the standard lexical string BSI profile.
func NewStringLexBSIProfile(options StringLexBSIOptions) EncodingProfile {
	profile := EncodingProfile{
		Kind:           EncodingStringLexBSI,
		PrefixLength:   options.PrefixLength,
		MaxLength:      options.MaxLength,
		RemainderStore: options.RemainderStore,
		Search:         SearchProfile{Enabled: options.Searchable, Mode: options.SearchMode},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
			PredicateCapabilityPrefix,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityOriginalValue,
		},
	}
	if options.Searchable {
		profile.PredicateCapabilities = append(profile.PredicateCapabilities, PredicateCapabilityTextSearch)
	}
	if profile.StoresFullStringInline() {
		profile.Rehydration = RehydrationProfile{Kind: RehydrationInline}
		profile.ProjectionCapabilities = append(profile.ProjectionCapabilities, ProjectionCapabilityInline)
		return profile
	}
	profile.Rehydration = RehydrationProfile{Kind: RehydrationLookup, Store: options.RemainderStore}
	profile.ProjectionCapabilities = append(profile.ProjectionCapabilities, ProjectionCapabilityLookup)
	return profile
}

// NewTimeBSIProfile returns a BSI time profile at the requested granularity.
func NewTimeBSIProfile(granularity TimeGranularity) EncodingProfile {
	return EncodingProfile{
		Kind:        EncodingTimeBSI,
		Granularity: granularity,
		Rehydration: RehydrationProfile{Kind: RehydrationInline},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityInline,
			ProjectionCapabilityOriginalValue,
		},
	}
}

// NewNumericBSIProfile returns a scaled numeric BSI profile.
func NewNumericBSIProfile(scale int, signed bool) EncodingProfile {
	return EncodingProfile{
		Kind:        EncodingNumericBSI,
		Scale:       scale,
		Signed:      signed,
		Rehydration: RehydrationProfile{Kind: RehydrationInline},
		PredicateCapabilities: PredicateCapabilities{
			PredicateCapabilityEquality,
			PredicateCapabilityRange,
		},
		ProjectionCapabilities: ProjectionCapabilities{
			ProjectionCapabilityInline,
			ProjectionCapabilityOriginalValue,
		},
	}
}

// Lossless reports whether the encoding can faithfully project original SQL values.
func (e EncodingProfile) Lossless() bool {
	return e.Rehydration.Lossless()
}

// RequiresLookup reports whether the encoding needs dictionary, KV, or similar lookup for projection.
func (e EncodingProfile) RequiresLookup() bool {
	return e.Rehydration.RequiresLookup()
}

// EffectiveMultiplicity returns scalar unless the profile explicitly declares set semantics.
func (e EncodingProfile) EffectiveMultiplicity() ValueMultiplicity {
	return e.Multiplicity.Effective()
}

// IsSetValued reports whether rows may have multiple active values for this field.
func (e EncodingProfile) IsSetValued() bool {
	return e.EffectiveMultiplicity() == MultiplicitySet
}

// SupportsPredicate reports whether the encoding can evaluate capability natively.
func (e EncodingProfile) SupportsPredicate(capability PredicateCapability) bool {
	return e.PredicateCapabilities.Has(capability)
}

// SupportsProjection reports whether the encoding can project capability natively.
func (e EncodingProfile) SupportsProjection(capability ProjectionCapability) bool {
	return e.ProjectionCapabilities.Has(capability)
}

// SupportsPrefix reports whether the encoding can evaluate anchored prefix predicates natively.
func (e EncodingProfile) SupportsPrefix() bool {
	return e.SupportsPredicate(PredicateCapabilityPrefix)
}

// Searchable reports whether the encoding advertises text-search behavior.
func (e EncodingProfile) Searchable() bool {
	return e.Search.Enabled
}

// HasBoundedStringLength reports whether the encoding declares a finite maximum string length.
func (e EncodingProfile) HasBoundedStringLength() bool {
	return e.MaxLength > 0
}

// StoresFullStringInline reports whether a StringLexBSI profile can encode the whole value in BSI state.
func (e EncodingProfile) StoresFullStringInline() bool {
	return e.Kind == EncodingStringLexBSI && e.PrefixLength > 0 && e.HasBoundedStringLength() && e.PrefixLength >= e.MaxLength
}

// NeedsStringRemainderLookup reports whether a StringLexBSI profile needs a side store for suffix bytes.
func (e EncodingProfile) NeedsStringRemainderLookup() bool {
	if e.Kind != EncodingStringLexBSI || e.PrefixLength <= 0 {
		return false
	}
	return !e.HasBoundedStringLength() || e.MaxLength > e.PrefixLength
}

// HasStringRemainderStore reports whether a StringLexBSI profile names the side store for suffix bytes.
func (e EncodingProfile) HasStringRemainderStore() bool {
	return e.NeedsStringRemainderLookup() && e.RemainderStore != ""
}

func cloneEncodingProfile(profile EncodingProfile) EncodingProfile {
	cloned := profile
	cloned.PredicateCapabilities = append(PredicateCapabilities(nil), profile.PredicateCapabilities...)
	cloned.ProjectionCapabilities = append(ProjectionCapabilities(nil), profile.ProjectionCapabilities...)
	return cloned
}

// cloneRelationshipEncodingProfile copies capability slices so catalog callers cannot mutate shared state.
func cloneRelationshipEncodingProfile(profile RelationshipEncodingProfile) RelationshipEncodingProfile {
	cloned := profile
	cloned.Capabilities = append(RelationshipCapabilities(nil), profile.Capabilities...)
	return cloned
}
