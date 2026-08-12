package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestFilterDomainBranchesForSourceKeepsQ19PartPredicatesTogether(t *testing.T) {
	filter := q19FormalFilterTreeForTest()

	branches := filterDomainBranchesForSource(filter, "part")
	if len(branches) != 3 {
		t.Fatalf("part branches = %d, want 3: %#v", len(branches), branches)
	}
	for i, branch := range branches {
		if branch.Operation != qsbridge.QuantaFilterIntersect {
			t.Fatalf("branch[%d] operation = %q, want INTERSECT", i, branch.Operation)
		}
		if got := filterDomainExpressionLeafCount(branch); got != 4 {
			t.Fatalf("branch[%d] part leaf count = %d, want brand/container/size lower/size upper", i, got)
		}
		if !filterDomainExpressionOnlySource(branch, "part") {
			t.Fatalf("branch[%d] includes a non-part predicate: %#v", i, branch)
		}
	}

	escaped := filterDomainFragmentsForSourceExcludingBranches(filter, "part", branches)
	if len(escaped) != 0 {
		t.Fatalf("escaped part leaves = %d, want none: %#v", len(escaped), escaped)
	}
}

func q19FormalFilterTreeForTest() qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterUnion,
		Children: []qsbridge.QuantaFilterExpression{
			q19FormalFilterBranchForTest("Brand#12", "SM", 1, 5, 1, 11),
			q19FormalFilterBranchForTest("Brand#23", "MED", 1, 10, 10, 20),
			q19FormalFilterBranchForTest("Brand#34", "LG", 1, 15, 20, 30),
		},
	}
}

func q19FormalFilterBranchForTest(brand, containerPrefix string, minSize, maxSize, minQuantity, maxQuantity int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			q19PartLeafForTest("p_brand", qsbridge.QuantaBSIOpEQ, brand),
			q19PartLeafForTest("p_container", "", containerPrefix),
			q19PartSizeLeafForTest(qsbridge.QuantaBSIOpGE, minSize),
			q19LineitemQuantityLeafForTest(qsbridge.QuantaBSIOpGE, minQuantity),
			q19LineitemQuantityLeafForTest(qsbridge.QuantaBSIOpLE, maxQuantity),
			q19PartSizeLeafForTest(qsbridge.QuantaBSIOpLE, maxSize),
			q19LineitemLeafForTest("l_shipmode"),
			q19LineitemLeafForTest("l_shipinstruct"),
		},
	}
}

func q19PartLeafForTest(field string, op qsbridge.QuantaBSIOp, value string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "part",
			Role:       "p",
			Field:      field,
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueString, value),
		},
	}
}

func q19PartSizeLeafForTest(op qsbridge.QuantaBSIOp, value int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "part",
			Role:       "p",
			Field:      "p_size",
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueInt, value),
		},
	}
}

func q19LineitemQuantityLeafForTest(op qsbridge.QuantaBSIOp, value int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "lineitem",
			Role:       "l",
			Field:      "l_quantity",
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueInt, value),
		},
	}
}

func q19LineitemLeafForTest(field string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index: "lineitem",
			Role:  "l",
			Field: field,
		},
	}
}
