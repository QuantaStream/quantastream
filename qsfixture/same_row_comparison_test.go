package qsfixture

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSameRowComparisonFixtureKernelReturnsRownumsWithoutProjection(t *testing.T) {
	kernel := NewSameRowComparisonFixtureKernel(map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
		1: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(10)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
		2: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(20)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
		3: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(30)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
		4: {
			"l.l_commitdate": {Kind: qsbridge.ValueInt, Value: int64(40)},
		},
	})
	request := runtimeFixtureSameRowComparisonRequest(qsbridge.BinaryOpLess, []qsbridge.QuantaRownum{3, 1, 2, 4})

	result, err := kernel.CompareSameRowFields(context.Background(), request)
	if err != nil {
		t.Fatalf("CompareSameRowFields error: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !reflect.DeepEqual(result.Domain.Rownums, []qsbridge.QuantaRownum{1}) {
		t.Fatalf("rownums = %#v, want only row 1", result.Domain.Rownums)
	}
	if len(result.Probes) != 2 || result.Probes[0].Value != "4" || result.Probes[1].Value != "1" {
		t.Fatalf("probes = %#v, want input/output counts only", result.Probes)
	}
}

func TestSameRowComparisonFixtureKernelSupportsComparisonOperators(t *testing.T) {
	kernel := NewSameRowComparisonFixtureKernel(map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
		1: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(10)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
		2: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(20)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
		3: {
			"l.l_commitdate":  {Kind: qsbridge.ValueInt, Value: int64(30)},
			"l.l_receiptdate": {Kind: qsbridge.ValueInt, Value: int64(20)},
		},
	})
	tests := []struct {
		name string
		op   qsbridge.BinaryOp
		want []qsbridge.QuantaRownum
	}{
		{name: "equal", op: qsbridge.BinaryOpEqual, want: []qsbridge.QuantaRownum{2}},
		{name: "not equal", op: qsbridge.BinaryOpNotEqual, want: []qsbridge.QuantaRownum{1, 3}},
		{name: "less", op: qsbridge.BinaryOpLess, want: []qsbridge.QuantaRownum{1}},
		{name: "less equal", op: qsbridge.BinaryOpLessEqual, want: []qsbridge.QuantaRownum{1, 2}},
		{name: "greater", op: qsbridge.BinaryOpGreater, want: []qsbridge.QuantaRownum{3}},
		{name: "greater equal", op: qsbridge.BinaryOpGreaterEqual, want: []qsbridge.QuantaRownum{2, 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := kernel.CompareSameRowFields(context.Background(), runtimeFixtureSameRowComparisonRequest(test.op, []qsbridge.QuantaRownum{1, 2, 3}))
			if err != nil {
				t.Fatalf("CompareSameRowFields error: %v", err)
			}
			if !reflect.DeepEqual(result.Domain.Rownums, test.want) {
				t.Fatalf("rownums = %#v, want %#v", result.Domain.Rownums, test.want)
			}
		})
	}
}

func runtimeFixtureSameRowComparisonRequest(op qsbridge.BinaryOp, rownums []qsbridge.QuantaRownum) qsbridge.SameRowComparisonRequest {
	lineitem := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	return qsbridge.SameRowComparisonRequest{
		ID:          "fixture.same_row",
		ProbePrefix: "fixture_same_row_",
		Domain: qsbridge.RownumDomainSet{
			Domain:  qsbridge.RownumDomain{Table: lineitem, Role: "l"},
			Rownums: append([]qsbridge.QuantaRownum(nil), rownums...),
		},
		Left:      qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Right:     qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Operator:  op,
		Kind:      qsbridge.SameRowComparisonBSI,
		Cacheable: true,
	}
}
