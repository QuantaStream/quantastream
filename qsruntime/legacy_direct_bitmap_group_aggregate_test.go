package qsruntime

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectBitmapGroupAggregateReaderStringEnumBSI(t *testing.T) {
	tableCache := testBitmapGroupAggregateTableCache()
	quantity := roaring64.NewDefaultBSI()
	quantity.SetBigValue(101, big.NewInt(10))
	quantity.SetBigValue(102, big.NewInt(20))
	quantity.SetBigValue(201, big.NewInt(7))

	projection := &fakeBitmapGroupAggregateProjection{
		BSIs: map[string]*roaring64.BSI{
			"l_quantity": quantity,
		},
	}
	sessions := &fakeBitmapGroupAggregateSessions{
		Groups: map[string][]qsbridge.QuantaRownum{
			"1/10": {101, 102},
			"2/10": {201},
		},
	}
	reader := LegacyDirectBitmapGroupAggregateReader{
		Sessions:   sessions,
		TableCache: tableCache,
		Projection: projection,
	}

	result, diagnostics, ok, err := reader.ReadBitmapGroupAggregates(context.Background(), BitmapGroupAggregateReadRequest{
		Index: "lineitem",
		GroupFields: []qsbridge.FieldRef{
			testBitmapGroupAggregateStringEnumField("l_returnflag"),
			testBitmapGroupAggregateStringEnumField("l_linestatus"),
		},
		Aggregates: []BitmapGroupAggregateReadSpec{
			{Function: "count", Type: qsbridge.DataTypeInt},
			{Function: "sum", Field: testBitmapGroupAggregateQuantityField(), Type: qsbridge.DataTypeInt},
			{Function: "avg", Field: testBitmapGroupAggregateQuantityField(), Type: qsbridge.DataTypeFloat},
			{Function: "min", Field: testBitmapGroupAggregateQuantityField(), Type: qsbridge.DataTypeInt},
			{Function: "max", Field: testBitmapGroupAggregateQuantityField(), Type: qsbridge.DataTypeInt},
		},
		CandidateRows: []qsbridge.QuantaRownum{101, 102, 201, 999},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadBitmapGroupAggregates ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := len(projection.Requests), 1; got != want {
		t.Fatalf("projection requests = %d, want %d", got, want)
	}
	if got, want := projection.Requests[0].PhysicalField, "l_quantity"; got != want {
		t.Fatalf("projected field = %q, want %q", got, want)
	}
	if got, want := len(sessions.CandidateSets), 4; got != want {
		t.Fatalf("candidate pushdown count = %d, want %d", got, want)
	}
	assertBitmapGroupAggregateGroup(t, result.Groups, "R\x00F", []any{"R", "F", int64(2), int64(30), float64(15), int64(10), int64(20)})
	assertBitmapGroupAggregateGroup(t, result.Groups, "A\x00F", []any{"A", "F", int64(1), int64(7), float64(7), int64(7), int64(7)})
}

func TestLegacyDirectBitmapGroupAggregateReaderDeclinesNonStringEnumGroup(t *testing.T) {
	reader := LegacyDirectBitmapGroupAggregateReader{
		Sessions:   &fakeBitmapGroupAggregateSessions{},
		TableCache: testBitmapGroupAggregateTableCache(),
		Projection: &fakeBitmapGroupAggregateProjection{},
	}
	_, diagnostics, ok, err := reader.ReadBitmapGroupAggregates(context.Background(), BitmapGroupAggregateReadRequest{
		Index:       "lineitem",
		GroupFields: []qsbridge.FieldRef{{Table: qsbridge.TableInstance{Table: "lineitem"}, Name: "l_orderkey", Index: qsbridge.IndexBSI}},
		Aggregates:  []BitmapGroupAggregateReadSpec{{Function: "count", Type: qsbridge.DataTypeInt}},
	})
	if err != nil || diagnostics.BlocksNative() || ok {
		t.Fatalf("ReadBitmapGroupAggregates ok/error/diagnostics = %t/%v/%v, want false/nil/none", ok, err, diagnostics)
	}
}

func TestLegacyDirectBitmapGroupAggregateReaderDeclinesCountOnly(t *testing.T) {
	reader := LegacyDirectBitmapGroupAggregateReader{
		Sessions:   &fakeBitmapGroupAggregateSessions{},
		TableCache: testBitmapGroupAggregateTableCache(),
		Projection: &fakeBitmapGroupAggregateProjection{},
	}
	_, diagnostics, ok, err := reader.ReadBitmapGroupAggregates(context.Background(), BitmapGroupAggregateReadRequest{
		Index:       "lineitem",
		GroupFields: []qsbridge.FieldRef{testBitmapGroupAggregateStringEnumField("l_returnflag")},
		Aggregates:  []BitmapGroupAggregateReadSpec{{Function: "count", Type: qsbridge.DataTypeInt}},
	})
	if err != nil || diagnostics.BlocksNative() || ok {
		t.Fatalf("ReadBitmapGroupAggregates ok/error/diagnostics = %t/%v/%v, want false/nil/none", ok, err, diagnostics)
	}
}

func testBitmapGroupAggregateTableCache() *core.TableCacheStruct {
	table := &core.Table{
		BasicTable:       &shared.BasicTable{Name: "lineitem"},
		AttributeNameMap: make(map[string]*core.Attribute),
	}
	table.Attributes = []core.Attribute{
		testBitmapGroupAggregateAttribute(table, "l_returnflag", "StringEnum", 0, []shared.Value{{Value: "R", RowID: 1}, {Value: "A", RowID: 2}}),
		testBitmapGroupAggregateAttribute(table, "l_linestatus", "StringEnum", 0, []shared.Value{{Value: "F", RowID: 10}, {Value: "O", RowID: 20}}),
		testBitmapGroupAggregateAttribute(table, "l_quantity", "IntBSI", 0, nil),
	}
	for i := range table.Attributes {
		attr := &table.Attributes[i]
		table.AttributeNameMap[attr.FieldName] = attr
	}
	tableCache := core.NewTableCacheStruct()
	tableCache.TableCache["lineitem"] = table
	return tableCache
}

func testBitmapGroupAggregateAttribute(table *core.Table, name, mapping string, scale int, values []shared.Value) core.Attribute {
	return core.Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       name,
			Type:            "Integer",
			MappingStrategy: mapping,
			Scale:           scale,
			Values:          values,
		},
		Parent: table,
	}
}

func testBitmapGroupAggregateStringEnumField(name string) qsbridge.FieldRef {
	return qsbridge.FieldRef{
		Table:        qsbridge.TableInstance{Table: "lineitem"},
		Name:         name,
		PhysicalName: name,
		Type:         qsbridge.DataTypeString,
		Index:        qsbridge.IndexStringEnum,
		Encoding:     qsbridge.LegacyEncodingProfile("StringEnum", qsbridge.LegacyEncodingOptions{}),
	}
}

func testBitmapGroupAggregateQuantityField() qsbridge.FieldRef {
	return qsbridge.FieldRef{
		Table:        qsbridge.TableInstance{Table: "lineitem"},
		Name:         "l_quantity",
		PhysicalName: "l_quantity",
		Type:         qsbridge.DataTypeInt,
		Index:        qsbridge.IndexBSI,
		Encoding:     qsbridge.LegacyEncodingProfile("IntBSI", qsbridge.LegacyEncodingOptions{}),
	}
}

type fakeBitmapGroupAggregateProjection struct {
	BSIs     map[string]*roaring64.BSI
	Requests []NativeProjectionBSIReadRequest
}

func (p *fakeBitmapGroupAggregateProjection) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results, diagnostics, err := p.ReadProjectionBSIs(ctx, []NativeProjectionBSIReadRequest{request})
	if len(results) == 0 {
		return NativeProjectionBSIReadResult{}, diagnostics, err
	}
	return results[0], diagnostics, err
}

func (p *fakeBitmapGroupAggregateProjection) ReadProjectionBSIs(_ context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results := make([]NativeProjectionBSIReadResult, len(requests))
	for i, request := range requests {
		p.Requests = append(p.Requests, request)
		results[i] = NativeProjectionBSIReadResult{BSI: p.BSIs[request.PhysicalField]}
	}
	return results, nil, nil
}

type fakeBitmapGroupAggregateSessions struct {
	Groups        map[string][]qsbridge.QuantaRownum
	CandidateSets []qsbridge.QuantaCandidateSet
}

func (s *fakeBitmapGroupAggregateSessions) BorrowDirectSession(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	return DirectSessionHandleFunc{
		QueryFunc: func(_ context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
			return BitmapQueryResult{Rownums: append([]qsbridge.QuantaRownum(nil), s.Groups[fakeBitmapGroupAggregateKey(request)]...)}, nil, nil
		},
		CandidateQueryFunc: func(_ context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error) {
			s.CandidateSets = append(s.CandidateSets, candidates)
			return BitmapQueryResult{Rownums: append([]qsbridge.QuantaRownum(nil), s.Groups[fakeBitmapGroupAggregateKey(request)]...)}, nil, true, nil
		},
	}, nil, nil
}

func fakeBitmapGroupAggregateKey(request ExecutionRequest) string {
	parts := make([]string, 0, len(request.Query.Fragments))
	for _, fragment := range request.Query.Fragments {
		if len(fragment.Values) == 0 || fragment.Values[0] == nil {
			continue
		}
		parts = append(parts, fragment.Values[0].String())
	}
	return strings.Join(parts, "/")
}

func assertBitmapGroupAggregateGroup(t *testing.T, groups []BitmapGroupAggregateReadGroup, key string, want []any) {
	t.Helper()
	for _, group := range groups {
		if group.Key != key {
			continue
		}
		cells := append(append([]qsbridge.ResultCell{}, group.Values...), group.Aggs...)
		if len(cells) != len(want) {
			t.Fatalf("group %q cell count = %d, want %d", key, len(cells), len(want))
		}
		for i, cell := range cells {
			if cell.Value != want[i] {
				t.Fatalf("group %q cell %d = %#v, want %#v", key, i, cell.Value, want[i])
			}
		}
		return
	}
	t.Fatalf("missing group %q in %#v", key, groups)
}
