package qsbridge

// SQLFeatureCategory groups SQL compatibility features by user-visible area.
type SQLFeatureCategory string

const (
	// SQLFeatureQuery covers SELECT query shape.
	SQLFeatureQuery SQLFeatureCategory = "query"
	// SQLFeaturePredicate covers WHERE, ON, HAVING, and membership filtering.
	SQLFeaturePredicate SQLFeatureCategory = "predicate"
	// SQLFeatureJoin covers join and relationship traversal semantics.
	SQLFeatureJoin SQLFeatureCategory = "join"
	// SQLFeatureAggregate covers aggregate and grouped query semantics.
	SQLFeatureAggregate SQLFeatureCategory = "aggregate"
	// SQLFeatureFunction covers built-in and custom SQL function compatibility.
	SQLFeatureFunction SQLFeatureCategory = "function"
	// SQLFeatureMutation covers INSERT, UPDATE, DELETE, and DDL/session statement shape.
	SQLFeatureMutation SQLFeatureCategory = "mutation"
	// SQLFeatureProtocol covers protocol-visible SQL behavior.
	SQLFeatureProtocol SQLFeatureCategory = "protocol"
)

// SQLFeature describes one scaffolded SQL compatibility feature.
type SQLFeature struct {
	Name         string
	Category     SQLFeatureCategory
	Status       CompatibilityStatus
	Capabilities []PlanCapability
	Diagnostics  []DiagnosticCode
	Description  string
}

// SQLFeatureMatrix is the scaffold's SQL compatibility matrix.
type SQLFeatureMatrix struct {
	Features    []SQLFeature
	Diagnostics DiagnosticSet
}

// DefaultSQLFeatureMatrix returns the current qsbridge SQL compatibility matrix.
func DefaultSQLFeatureMatrix() SQLFeatureMatrix {
	return SQLFeatureMatrix{Features: []SQLFeature{
		{
			Name:        "select_projection",
			Category:    SQLFeatureQuery,
			Status:      CompatibilityStatusNativePlanning,
			Description: "resolved projection expressions and result-column metadata",
		},
		{
			Name:        "order_by",
			Category:    SQLFeatureQuery,
			Status:      CompatibilityStatusNativePlanning,
			Description: "resolved sort expressions and direction metadata",
		},
		{
			Name:     "predicate_pushdown",
			Category: SQLFeaturePredicate,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilityBitmapPushdown,
				CapabilityBSIPushdown,
				CapabilityEncodingEquality,
				CapabilityEncodingRange,
				CapabilityEncodingMembership,
			},
			Description: "encoding-backed equality, range, and membership predicate evidence",
		},
		{
			Name:     "string_predicates",
			Category: SQLFeaturePredicate,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilityStringEnumEquality,
				CapabilityStringEnumMembership,
				CapabilityStringEnumPrefixLike,
				CapabilityStringEnumContainsLike,
				CapabilityEncodingPrefix,
				CapabilityEncodingContains,
			},
			Description: "StringEnum and encoding-advertised string equality, membership, prefix, and contains matching",
		},
		{
			Name:     "residual_scan",
			Category: SQLFeaturePredicate,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilityResidualScan,
			},
			Description: "single-table residual expression evaluation metadata",
		},
		{
			Name:     "mixed_table_residual",
			Category: SQLFeaturePredicate,
			Status:   CompatibilityStatusDeferred,
			Capabilities: []PlanCapability{
				CapabilityUnsupportedMixedTableResidual,
			},
			Diagnostics: []DiagnosticCode{DiagnosticMixedTableResidual},
			Description: "mixed-table residual predicates are modeled as blockers until executor semantics are explicit",
		},
		{
			Name:     "inner_join",
			Category: SQLFeatureJoin,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilityRelationshipParentLookup,
				CapabilityRelationshipChildExpansion,
				CapabilityRelationshipJoinReduction,
				CapabilityParentToChildExpansion,
			},
			Description: "relationship-backed inner join traversal and found-set reduction metadata",
		},
		{
			Name:     "semi_anti_membership",
			Category: SQLFeatureJoin,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilitySemiMembership,
				CapabilityAntiMembership,
				CapabilityBitmapDifference,
				CapabilityRelationshipSemiJoin,
				CapabilityRelationshipAntiJoinDifference,
			},
			Description: "IN/EXISTS and NOT IN/NOT EXISTS relationship membership, including bitmap-difference evidence",
		},
		{
			Name:     "outer_join",
			Category: SQLFeatureJoin,
			Status:   CompatibilityStatusDeferred,
			Capabilities: []PlanCapability{
				CapabilityOuterJoin,
				CapabilityNullExtension,
			},
			Diagnostics: []DiagnosticCode{DiagnosticOuterJoin},
			Description: "outer-join vocabulary records preserved-side and null-extension requirements before executor support",
		},
		{
			Name:        "grouped_aggregate",
			Category:    SQLFeatureAggregate,
			Status:      CompatibilityStatusNativePlanning,
			Description: "group-by and aggregate shape metadata over bound expressions",
		},
		{
			Name:     "grouped_join",
			Category: SQLFeatureAggregate,
			Status:   CompatibilityStatusNativePlanning,
			Capabilities: []PlanCapability{
				CapabilityGroupedJoin,
			},
			Description: "grouped join shape is explicit in classification and plan metadata",
		},
		{
			Name:     "scalar_subquery",
			Category: SQLFeatureQuery,
			Status:   CompatibilityStatusDeferred,
			Capabilities: []PlanCapability{
				CapabilityScalarSubquery,
			},
			Diagnostics: []DiagnosticCode{DiagnosticScalarSubquery},
			Description: "scalar subqueries remain a native-planning blocker until the refactor models them fully",
		},
		{
			Name:        "mutations",
			Category:    SQLFeatureMutation,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "INSERT, UPDATE, DELETE, DDL, and session statements have metadata shape but no qsbridge execution",
		},
		{
			Name:        "custom_functions",
			Category:    SQLFeatureFunction,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "Quanta-custom SQL functions are catalog-visible with origin metadata so adapters can distinguish them from MySQL-compatible functions",
		},
		{
			Name:        "prepared_and_batch",
			Category:    SQLFeatureProtocol,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "prepared handles, parameter binding, batch value sets, and long-data metadata without execution",
		},
		{
			Name:        "explain_and_management_metadata",
			Category:    SQLFeatureProtocol,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "structured explain, profile, compatibility, readiness, and plan-cache policy metadata without execution",
		},
		{
			Name:     "cancellation_and_cursors",
			Category: SQLFeatureProtocol,
			Status:   CompatibilityStatusMetadataOnly,
			Capabilities: []PlanCapability{
				CapabilityCancellationAware,
			},
			Description: "request lifecycle, cancellation, and forward-only cursor metadata without runtime interruption or fetch storage",
		},
	}}
}

// SQLFeatureMatrix returns the service SQL compatibility matrix.
func (s PlanningService) SQLFeatureMatrix() SQLFeatureMatrix {
	_ = s
	return DefaultSQLFeatureMatrix().Clone()
}

// Clone returns a deep copy of matrix.
func (m SQLFeatureMatrix) Clone() SQLFeatureMatrix {
	m.Features = cloneSQLFeatures(m.Features)
	m.Diagnostics = cloneDiagnosticSet(m.Diagnostics)
	return m
}

func cloneSQLFeatures(features []SQLFeature) []SQLFeature {
	if len(features) == 0 {
		return nil
	}
	cloned := make([]SQLFeature, 0, len(features))
	for _, feature := range features {
		feature.Capabilities = append([]PlanCapability(nil), feature.Capabilities...)
		feature.Diagnostics = append([]DiagnosticCode(nil), feature.Diagnostics...)
		cloned = append(cloned, feature)
	}
	return cloned
}
