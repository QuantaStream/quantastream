package qsbridge

import "strings"

const relationshipAlignedExpressionPrefix = "__qs_relationship_aligned_expr:"
const relationshipAlignedDiscountedRevenueExpr = "discounted_revenue"

// RelationshipAlignedDiscountedRevenueField encodes the narrow storage-side
// expression used by relationship aligned aggregate readers.
func RelationshipAlignedDiscountedRevenueField(priceField string, discountField string) string {
	return relationshipAlignedExpressionPrefix + relationshipAlignedDiscountedRevenueExpr + ":" + priceField + ":" + discountField
}

// ParseRelationshipAlignedDiscountedRevenueField decodes the storage-side
// discounted revenue expression token.
func ParseRelationshipAlignedDiscountedRevenueField(valueField string) (priceField string, discountField string, ok bool) {
	rest, ok := strings.CutPrefix(valueField, relationshipAlignedExpressionPrefix+relationshipAlignedDiscountedRevenueExpr+":")
	if !ok {
		return "", "", false
	}
	price, discount, ok := strings.Cut(rest, ":")
	if !ok || price == "" || discount == "" {
		return "", "", false
	}
	return price, discount, true
}

// RelationshipAlignedAggregateValidationField returns a real physical field
// that can be used for shard/session validation before dispatching an encoded
// relationship aggregate expression.
func RelationshipAlignedAggregateValidationField(valueField string) string {
	if priceField, _, ok := ParseRelationshipAlignedDiscountedRevenueField(valueField); ok {
		return priceField
	}
	return valueField
}
