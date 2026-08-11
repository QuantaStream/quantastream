package qsbridge

import "strings"

// NativeClassification summarizes whether a bound query can use the native path.
type NativeClassification struct {
	Kind         QueryKind
	Supported    bool
	Capabilities []PlanCapability
	Diagnostics  DiagnosticSet
	Fields       []FieldRef
}

// ClassifyNative inspects a bound query for native capabilities and blockers.
func ClassifyNative(query QueryIR) NativeClassification {
	capabilities := newCapabilityCollector()
	collectPredicateCapabilities(capabilities, query.Predicates)
	collectPredicateCapabilities(capabilities, query.Having)
	collectJoinCapabilities(capabilities, query.Joins)
	collectMembershipCapabilities(capabilities, query.Memberships)

	if len(query.Joins) > 0 && (len(query.GroupBy) > 0 || len(query.Aggregates) > 0) {
		capabilities.add(CapabilityGroupedJoin)
	}

	diagnostics := query.Diagnostics()
	return NativeClassification{
		Kind:         query.Kind,
		Supported:    !diagnostics.BlocksNative(),
		Capabilities: capabilities.values,
		Diagnostics:  diagnostics,
		Fields:       query.RequiredFields(),
	}
}

func collectPredicateCapabilities(capabilities *capabilityCollector, predicates []Predicate) {
	for _, predicate := range predicates {
		for _, capability := range predicate.Capabilities {
			capabilities.add(capability)
		}
		switch predicate.Placement {
		case PredicatePushdown:
			for _, capability := range EncodingPredicateCapabilities(predicate) {
				capabilities.add(capability)
			}
			capability, ok := StringEnumPredicateCapability(predicate)
			capabilities.addIf(ok, capability)
		case PredicateResidualScan:
			capabilities.add(CapabilityResidualScan)
		case PredicateResidualJoin:
			if !predicate.Supported() {
				capabilities.add(CapabilityUnsupportedMixedTableResidual)
			}
		}
	}
}

// StringEnumPredicateCapability reports the native StringEnum capability for predicate.
//
// This helper only classifies simple field/literal and field/list predicates. It does not
// resolve dictionary ids, expand LIKE patterns, or choose bitmap operations.
// Those choices belong in later planner and executor layers.
func StringEnumPredicateCapability(predicate Predicate) (PlanCapability, bool) {
	binary, ok := asBinaryExpr(predicate.Expr)
	if !ok {
		return "", false
	}

	if binary.Op == BinaryOpIn || binary.Op == BinaryOpNotIn {
		field, list, ok := fieldListPair(binary)
		if ok && field.Index == IndexStringEnum && stringListPredicate(list) && field.SupportsDictionaryCapability(DictionaryCapabilityStableIDs) {
			return CapabilityStringEnumMembership, true
		}
		return "", false
	}

	field, literal, ok := fieldLiteralPair(binary)
	if !ok || field.Index != IndexStringEnum || literal.Kind != ValueString {
		return "", false
	}

	switch binary.Op {
	case BinaryOpEqual, BinaryOpNotEqual:
		if field.SupportsDictionaryCapability(DictionaryCapabilityStableIDs) {
			return CapabilityStringEnumEquality, true
		}
	case BinaryOpLike, BinaryOpNotLike:
		pattern, ok := literal.Value.(string)
		if !ok {
			return "", false
		}
		switch simpleLikePattern(pattern) {
		case likePatternExact:
			if field.SupportsDictionaryCapability(DictionaryCapabilityStableIDs) {
				return CapabilityStringEnumEquality, true
			}
		case likePatternPrefix:
			if field.SupportsDictionaryCapability(DictionaryCapabilityPrefixMatch) {
				return CapabilityStringEnumPrefixLike, true
			}
		case likePatternContains:
			if binary.Op == BinaryOpLike && field.SupportsDictionaryCapability(DictionaryCapabilityContainsMatch) {
				return CapabilityStringEnumContainsLike, true
			}
		}
	}
	return "", false
}

func stringListPredicate(list ListExpr) bool {
	if len(list.Items) == 0 {
		return false
	}
	for _, item := range list.Items {
		if literal, ok := asLiteralExpr(item); ok {
			if literal.Kind != ValueString {
				return false
			}
			continue
		}
		if _, ok := asParameterExpr(item); ok {
			continue
		}
		return false
	}
	return true
}

// EncodingPredicateCapabilities reports native predicate shapes advertised by the field encoding profile.
//
// This helper does not choose executor primitives. It only records that the
// bound field's encoding claims support for the predicate shape.
func EncodingPredicateCapabilities(predicate Predicate) []PlanCapability {
	if predicate.Placement != PredicatePushdown {
		return nil
	}

	binary, ok := asBinaryExpr(predicate.Expr)
	if !ok {
		return nil
	}

	field, ok := predicateField(binary)
	if !ok {
		return nil
	}

	switch binary.Op {
	case BinaryOpEqual, BinaryOpNotEqual:
		return encodingCapabilityIfSupported(field, PredicateCapabilityEquality, CapabilityEncodingEquality)
	case BinaryOpIn, BinaryOpNotIn:
		return encodingCapabilityIfSupported(field, PredicateCapabilityMembership, CapabilityEncodingMembership)
	case BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return encodingCapabilityIfSupported(field, PredicateCapabilityRange, CapabilityEncodingRange)
	case BinaryOpLike:
		literal, ok := predicateLiteral(binary)
		if !ok || literal.Kind != ValueString {
			return nil
		}
		pattern, ok := literal.Value.(string)
		if !ok {
			return nil
		}
		switch simpleLikePattern(pattern) {
		case likePatternExact:
			return encodingCapabilityIfSupported(field, PredicateCapabilityEquality, CapabilityEncodingEquality)
		case likePatternPrefix:
			return encodingCapabilityIfSupported(field, PredicateCapabilityPrefix, CapabilityEncodingPrefix)
		case likePatternContains:
			return encodingCapabilityIfSupported(field, PredicateCapabilityContains, CapabilityEncodingContains)
		}
	}
	return nil
}

// encodingCapabilityIfSupported returns plan capability evidence when the field advertises predicate support.
func encodingCapabilityIfSupported(field FieldRef, predicate PredicateCapability, plan PlanCapability) []PlanCapability {
	if field.SupportsPredicateCapability(predicate) {
		return []PlanCapability{plan}
	}
	return nil
}

func collectJoinCapabilities(capabilities *capabilityCollector, edges []JoinEdge) {
	for _, edge := range edges {
		if edge.Direction == JoinParentToChild {
			capabilities.add(CapabilityParentToChildExpansion)
		}
		for _, capability := range edge.Capabilities() {
			capabilities.add(capability)
		}
		for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
			capabilities.add(capability)
		}
	}
}

// RelationshipPlanCapabilities maps relation-storage capabilities into plan-level evidence.
func RelationshipPlanCapabilities(encoding RelationshipEncodingProfile) []PlanCapability {
	capabilities := make([]PlanCapability, 0, len(encoding.Capabilities))
	for _, capability := range encoding.Capabilities {
		switch capability {
		case RelationshipCapabilityParentLookup:
			capabilities = append(capabilities, CapabilityRelationshipParentLookup)
		case RelationshipCapabilityChildExpansion:
			capabilities = append(capabilities, CapabilityRelationshipChildExpansion)
		case RelationshipCapabilityJoinReduction:
			capabilities = append(capabilities, CapabilityRelationshipJoinReduction)
		case RelationshipCapabilitySemiJoin:
			capabilities = append(capabilities, CapabilityRelationshipSemiJoin)
		case RelationshipCapabilityAntiJoinDifference:
			capabilities = append(capabilities, CapabilityRelationshipAntiJoinDifference)
		case RelationshipCapabilityNullExtension:
			capabilities = append(capabilities, CapabilityNullExtension)
		}
	}
	return capabilities
}

func collectMembershipCapabilities(capabilities *capabilityCollector, edges []MembershipEdge) {
	for _, edge := range edges {
		for _, capability := range edge.Capabilities() {
			capabilities.add(capability)
		}
		for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
			capabilities.add(capability)
		}
		if edge.Direction == JoinParentToChild {
			capabilities.add(CapabilityParentToChildExpansion)
		}
	}
}

type capabilityCollector struct {
	values []PlanCapability
	seen   map[PlanCapability]struct{}
}

func newCapabilityCollector() *capabilityCollector {
	return &capabilityCollector{
		values: make([]PlanCapability, 0),
		seen:   make(map[PlanCapability]struct{}),
	}
}

func (c *capabilityCollector) add(capability PlanCapability) {
	if capability == "" {
		return
	}
	if _, ok := c.seen[capability]; ok {
		return
	}
	c.seen[capability] = struct{}{}
	c.values = append(c.values, capability)
}

// addIf conditionally records capability while preserving collector de-duplication.
func (c *capabilityCollector) addIf(ok bool, capability PlanCapability) {
	if ok {
		c.add(capability)
	}
}

// HasCapability reports whether the classification includes capability.
func (c NativeClassification) HasCapability(capability PlanCapability) bool {
	for _, current := range c.Capabilities {
		if current == capability {
			return true
		}
	}
	return false
}

// asBinaryExpr unwraps pointer and value binary expressions used by parser bridges.
func asBinaryExpr(expr Expr) (BinaryExpr, bool) {
	switch n := expr.(type) {
	case BinaryExpr:
		return n, true
	case *BinaryExpr:
		if n != nil {
			return *n, true
		}
	}
	return BinaryExpr{}, false
}

// predicateField returns the field side of a simple predicate expression.
func predicateField(binary BinaryExpr) (FieldRef, bool) {
	field, _, ok := fieldLiteralPair(binary)
	if ok {
		return field, true
	}
	left, ok := asFieldExpr(binary.Left)
	if ok {
		return left.Ref, true
	}
	right, ok := asFieldExpr(binary.Right)
	if ok && binary.Op == BinaryOpEqual {
		return right.Ref, true
	}
	return FieldRef{}, false
}

// predicateLiteral returns the literal side of a simple predicate expression.
func predicateLiteral(binary BinaryExpr) (LiteralExpr, bool) {
	_, literal, ok := fieldLiteralPair(binary)
	if ok {
		return literal, true
	}
	left, ok := asLiteralExpr(binary.Left)
	if ok {
		return left, true
	}
	right, ok := asLiteralExpr(binary.Right)
	if ok {
		return right, true
	}
	return LiteralExpr{}, false
}

// fieldLiteralPair extracts the simple field/literal shape used for indexed predicates.
// Reversed literal/field order is only valid for equality.
func fieldLiteralPair(binary BinaryExpr) (FieldRef, LiteralExpr, bool) {
	leftField, leftFieldOK := asFieldExpr(binary.Left)
	rightLiteral, rightLiteralOK := asLiteralExpr(binary.Right)
	if leftFieldOK && rightLiteralOK {
		return leftField.Ref, rightLiteral, true
	}
	if binary.Op != BinaryOpEqual {
		return FieldRef{}, LiteralExpr{}, false
	}
	rightField, rightFieldOK := asFieldExpr(binary.Right)
	leftLiteral, leftLiteralOK := asLiteralExpr(binary.Left)
	if rightFieldOK && leftLiteralOK {
		return rightField.Ref, leftLiteral, true
	}
	return FieldRef{}, LiteralExpr{}, false
}

func fieldListPair(binary BinaryExpr) (FieldRef, ListExpr, bool) {
	leftField, leftFieldOK := asFieldExpr(binary.Left)
	rightList, rightListOK := asListExpr(binary.Right)
	if leftFieldOK && rightListOK {
		return leftField.Ref, rightList, true
	}
	return FieldRef{}, ListExpr{}, false
}

// asFieldExpr unwraps pointer and value field expressions.
func asFieldExpr(expr Expr) (FieldExpr, bool) {
	switch n := expr.(type) {
	case FieldExpr:
		return n, true
	case *FieldExpr:
		if n != nil {
			return *n, true
		}
	}
	return FieldExpr{}, false
}

func asParameterExpr(expr Expr) (ParameterExpr, bool) {
	switch n := expr.(type) {
	case ParameterExpr:
		return n, true
	case *ParameterExpr:
		if n != nil {
			return *n, true
		}
	}
	return ParameterExpr{}, false
}

func asListExpr(expr Expr) (ListExpr, bool) {
	switch n := expr.(type) {
	case ListExpr:
		return n, true
	case *ListExpr:
		if n != nil {
			return *n, true
		}
	}
	return ListExpr{}, false
}

// asLiteralExpr unwraps pointer and value literal expressions.
func asLiteralExpr(expr Expr) (LiteralExpr, bool) {
	switch n := expr.(type) {
	case LiteralExpr:
		return n, true
	case *LiteralExpr:
		if n != nil {
			return *n, true
		}
	}
	return LiteralExpr{}, false
}

type likePatternClass string

const (
	likePatternUnsupported likePatternClass = ""
	likePatternExact       likePatternClass = "exact"
	likePatternPrefix      likePatternClass = "prefix"
	likePatternContains    likePatternClass = "contains"
)

// simpleLikePattern classifies only LIKE patterns that can map cleanly to dictionary scans.
// Escapes, single-character wildcards, suffix-only patterns, and mixed wildcards stay unsupported.
func simpleLikePattern(pattern string) likePatternClass {
	switch {
	case pattern == "" || strings.ContainsAny(pattern, "_"):
		return likePatternUnsupported
	case !strings.Contains(pattern, "%"):
		return likePatternExact
	case strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[:len(pattern)-1], "%"):
		return likePatternPrefix
	case len(pattern) >= 2 && strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[1:len(pattern)-1], "%"):
		return likePatternContains
	default:
		return likePatternUnsupported
	}
}
