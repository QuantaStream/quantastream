package qsruntime

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectProjectionBSIFieldReaderReadsValuesInRequestedOrder(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"orders": {
			BasicTable: &shared.BasicTable{Name: "orders"},
			AttributeNameMap: map[string]*core.Attribute{
				"o_orderkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(7, int64(1001))
	bsi.SetValue(9, int64(1003))
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "orders" || request.PhysicalField != "o_orderkey" {
				t.Fatalf("read request = %#v, want orders.o_orderkey", request)
			}
			return NativeProjectionBSIReadResult{
				BSI:    bsi,
				Probes: []ExecutionProbe{{Section: "native_projection_materialization", Name: "fake_bsi_read"}},
			}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "orders",
		Field:   qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt},
		Rownums: []qsbridge.QuantaRownum{9, 8, 7},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	values := result.Values
	if len(values) != 3 {
		t.Fatalf("values = %#v, want three cells", values)
	}
	if values[0].Value != int64(1003) || values[1].Kind != qsbridge.ValueNull || values[2].Value != int64(1001) {
		t.Fatalf("values = %#v, want 1003/null/1001", values)
	}
	if len(result.Probes) != 1 || result.Probes[0].Name != "fake_bsi_read" {
		t.Fatalf("probes = %#v, want fake_bsi_read", result.Probes)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderBatchesSimpleBSIFields(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_orderkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_orderkey", Type: "Integer", MappingStrategy: "IntBSI"}},
				"l_suppkey":  {BasicAttribute: &shared.BasicAttribute{FieldName: "l_suppkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			},
		},
	}}
	orderKey := roaring64.NewDefaultBSI()
	orderKey.SetValue(1, int64(10))
	orderKey.SetValue(2, int64(20))
	suppKey := roaring64.NewDefaultBSI()
	suppKey.SetValue(1, int64(100))
	suppKey.SetValue(2, int64(200))
	bsiReader := &recordingNativeProjectionBSIBatchReader{
		values: map[string]*roaring64.BSI{
			"l_orderkey": orderKey,
			"l_suppkey":  suppKey,
		},
	}
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader:     bsiReader,
	}

	results, diagnostics, err := reader.ReadProjectionFields(context.Background(), []NativeProjectionFieldReadRequest{{
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_orderkey", Type: qsbridge.DataTypeInt},
		Rownums: []qsbridge.QuantaRownum{1, 2},
	}, {
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_suppkey", Type: qsbridge.DataTypeInt},
		Rownums: []qsbridge.QuantaRownum{1, 2},
	}})
	if err != nil {
		t.Fatalf("ReadProjectionFields error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if bsiReader.singleCalls != 0 {
		t.Fatalf("single BSI reads = %d, want 0", bsiReader.singleCalls)
	}
	if len(bsiReader.batchRequests) != 1 || len(bsiReader.batchRequests[0]) != 2 {
		t.Fatalf("batch requests = %#v, want one two-field BSI batch", bsiReader.batchRequests)
	}
	if got := results[0].Values[1].Value; got != int64(20) {
		t.Fatalf("l_orderkey row 2 = %#v, want 20", got)
	}
	if got := results[1].Values[1].Value; got != int64(200) {
		t.Fatalf("l_suppkey row 2 = %#v, want 200", got)
	}
}

type recordingNativeProjectionBSIBatchReader struct {
	singleCalls   int
	batchRequests [][]NativeProjectionBSIReadRequest
	values        map[string]*roaring64.BSI
}

func (r *recordingNativeProjectionBSIBatchReader) ReadProjectionBSI(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	r.singleCalls++
	return NativeProjectionBSIReadResult{BSI: r.values[request.PhysicalField]}, nil, nil
}

func (r *recordingNativeProjectionBSIBatchReader) ReadProjectionBSIs(_ context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	r.batchRequests = append(r.batchRequests, append([]NativeProjectionBSIReadRequest(nil), requests...))
	results := make([]NativeProjectionBSIReadResult, 0, len(requests))
	for _, request := range requests {
		results = append(results, NativeProjectionBSIReadResult{BSI: r.values[request.PhysicalField]})
	}
	return results, nil, nil
}

type typedProjectionGateReader struct {
	rawCalls   int
	int64Calls int
	bsi        *roaring64.BSI
}

func (r *typedProjectionGateReader) ReadProjectionBSI(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	r.rawCalls++
	return NativeProjectionBSIReadResult{BSI: r.bsi}, nil, nil
}

func (r *typedProjectionGateReader) ReadProjectionBSIInt64Values(context.Context, []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIInt64ValueReadResult, qsbridge.DiagnosticSet, error) {
	r.int64Calls++
	return []NativeProjectionBSIInt64ValueReadResult{{
		Values: []int64{1046866},
		Exists: []bool{true},
		Fast:   true,
	}}, nil, nil
}

func (r *typedProjectionGateReader) SupportsProjectionBSIInt64Values() bool {
	return true
}

func TestLegacyDirectProjectionBSIFieldReaderReadsRownumPseudoField(t *testing.T) {
	reader := NativeProjectionBSIFieldReader{}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index: "customers_qa",
		Field: qsbridge.QuantaProjectionField{
			Index:        "customers_qa",
			Field:        "@rownum",
			PhysicalName: "@rownum",
			Type:         qsbridge.DataTypeInt,
		},
		Rownums: []qsbridge.QuantaRownum{7, 9},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(result.Values) != 2 {
		t.Fatalf("values = %#v, want two cells", result.Values)
	}
	if result.Values[0].Value != int64(7) || result.Values[1].Value != int64(9) {
		t.Fatalf("values = %#v, want candidate rownums", result.Values)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderScalesFloats(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_extendedprice": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_extendedprice", Type: "Float", MappingStrategy: "FloatScaleBSI", Scale: 2}},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(42, int64(123456))
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			return NativeProjectionBSIReadResult{BSI: bsi}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		Rownums: []qsbridge.QuantaRownum{42},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := result.Values[0].Value; got != float64(1234.56) {
		t.Fatalf("scaled value = %#v, want 1234.56", got)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderAvoidsTypedInt64ForSignedFloatScaleBSI(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"customer": {
			BasicTable: &shared.BasicTable{Name: "customer"},
			AttributeNameMap: map[string]*core.Attribute{
				"c_acctbal": {BasicAttribute: &shared.BasicAttribute{FieldName: "c_acctbal", Type: "Float", MappingStrategy: "FloatScaleBSI", Scale: 2}},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(1193, big.NewInt(-1710))
	storage := &typedProjectionGateReader{bsi: bsi}
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader:     storage,
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "customer",
		Field:   qsbridge.QuantaProjectionField{Index: "customer", Field: "c_acctbal", Type: qsbridge.DataTypeFloat},
		Rownums: []qsbridge.QuantaRownum{1193},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if storage.int64Calls != 0 {
		t.Fatalf("typed int64 reads = %d, want 0", storage.int64Calls)
	}
	if storage.rawCalls != 1 {
		t.Fatalf("raw BSI reads = %d, want 1", storage.rawCalls)
	}
	if got := result.Values[0].Value; got != float64(-17.10) {
		t.Fatalf("scaled signed value = %#v, want -17.10", got)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderAvoidsTypedInt64ForProjectionValues(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"orders": {
			BasicTable: &shared.BasicTable{Name: "orders"},
			AttributeNameMap: map[string]*core.Attribute{
				"o_orderkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(7, big.NewInt(42))
	storage := &typedProjectionGateReader{bsi: bsi}
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader:     storage,
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "orders",
		Field:   qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt},
		Rownums: []qsbridge.QuantaRownum{7},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if storage.int64Calls != 0 {
		t.Fatalf("typed int64 reads = %d, want 0", storage.int64Calls)
	}
	if storage.rawCalls != 1 {
		t.Fatalf("raw BSI reads = %d, want 1", storage.rawCalls)
	}
	if got := result.Values[0].Value; got != int64(42) {
		t.Fatalf("projected value = %#v, want 42", got)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderDecodesTimeGranularity(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"orders_qa": {
			BasicTable: &shared.BasicTable{Name: "orders_qa"},
			AttributeNameMap: map[string]*core.Attribute{
				"order_date": {BasicAttribute: &shared.BasicAttribute{FieldName: "order_date", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(1, int64(1685592000000000))
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			return NativeProjectionBSIReadResult{BSI: bsi}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "orders_qa",
		Field:   qsbridge.QuantaProjectionField{Index: "orders_qa", Field: "order_date", Type: qsbridge.DataTypeTime},
		Rownums: []qsbridge.QuantaRownum{1},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	got, ok := result.Values[0].Value.(time.Time)
	if !ok || got.UnixNano() != int64(1685592000000000000) {
		t.Fatalf("time value = %#v, want 1685592000000000000 nanos", result.Values[0])
	}
}

func TestLegacyDirectProjectionBSIFieldReaderProjectsTimestampYearExpression(t *testing.T) {
	table := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_shipdate": {
					BasicAttribute: &shared.BasicAttribute{
						FieldName:       "l_shipdate",
						Type:            "DateTime",
						MappingStrategy: "TimestampBSI",
						MapperConfig:    map[string]string{"granularity": "millisecond"},
					},
				},
			},
		},
	}}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(7, time.Date(1998, 12, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	bsi.SetValue(9, time.Date(1995, 3, 14, 0, 0, 0, 0, time.UTC).UnixMilli())
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "lineitem" || request.PhysicalField != "l_shipdate" {
				t.Fatalf("read request = %#v, want lineitem.l_shipdate", request)
			}
			return NativeProjectionBSIReadResult{
				BSI:    bsi,
				Probes: []ExecutionProbe{{Section: "native_projection_materialization", Name: "fake_bsi_read"}},
			}, nil, nil
		}),
	}
	expression := qsbridge.QuantaProjectionExpression{
		Expr: qsbridge.FunctionCall(
			qsbridge.FunctionDefinition{Name: "year", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeInt},
			qsbridge.Field(qsbridge.FieldRef{Table: table, Name: "l_shipdate", Type: qsbridge.DataTypeTime}),
		),
		Output: qsbridge.QuantaProjectionField{Index: "lineitem", Role: "l", Field: "year_l_shipdate", Type: qsbridge.DataTypeInt},
	}

	result, diagnostics, err := reader.ReadProjectionExpression(context.Background(), NativeProjectionExpressionReadRequest{
		Index:      "lineitem",
		Expression: expression,
		Rownums:    []qsbridge.QuantaRownum{7, 8, 9},
	})
	if err != nil {
		t.Fatalf("ReadProjectionExpression error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := result.Expression.Output.Field; got != "year_l_shipdate" {
		t.Fatalf("output field = %q, want year_l_shipdate", got)
	}
	if result.Values[0].Value != int64(1998) || result.Values[1].Kind != qsbridge.ValueNull || result.Values[2].Value != int64(1995) {
		t.Fatalf("year values = %#v, want 1998/null/1995", result.Values)
	}
	if !legacyProjectionTestProbeName(result.Probes, "expression_projection_rows") ||
		!legacyProjectionTestProbeName(result.Probes, "expression_projection_function") {
		t.Fatalf("probes = %#v, want expression projection probes", result.Probes)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderReadsBoolDirect(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"customers_qa": {
			BasicTable: &shared.BasicTable{Name: "customers_qa"},
			AttributeNameMap: map[string]*core.Attribute{
				"isActive": {BasicAttribute: &shared.BasicAttribute{FieldName: "isActive", Type: "Bool", MappingStrategy: "BoolDirect"}},
			},
		},
	}}
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		DictionaryReader: NativeProjectionDictionaryIDReaderFunc(func(_ context.Context, request NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "customers_qa" || request.PhysicalField != "isActive" {
				t.Fatalf("read request = %#v, want customers_qa.isActive", request)
			}
			return NativeProjectionDictionaryIDReadResult{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(0)},
					{Kind: qsbridge.ValueNull, Value: nil},
				},
			}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "customers_qa",
		Field:   qsbridge.QuantaProjectionField{Index: "customers_qa", Field: "isActive", Type: qsbridge.DataTypeBool},
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := result.Values[0]; got.Kind != qsbridge.ValueBool || got.Value != true {
		t.Fatalf("values[0] = %#v, want true", got)
	}
	if got := result.Values[1]; got.Kind != qsbridge.ValueBool || got.Value != false {
		t.Fatalf("values[1] = %#v, want false", got)
	}
	if got := result.Values[2]; got.Kind != qsbridge.ValueNull {
		t.Fatalf("values[2] = %#v, want null", got)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderReadsStringEnumDictionaryIDs(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_shipmode": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipmode", Type: "String", MappingStrategy: "StringEnum"}},
			},
		},
	}}
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		DictionaryReader: NativeProjectionDictionaryIDReaderFunc(func(_ context.Context, request NativeProjectionDictionaryIDReadRequest) (NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "lineitem" || request.PhysicalField != "l_shipmode" {
				t.Fatalf("dictionary request = %#v, want lineitem.l_shipmode", request)
			}
			return NativeProjectionDictionaryIDReadResult{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(2)},
					{Kind: qsbridge.ValueInt, Value: int64(7)},
				},
			}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString},
		Rownums: []qsbridge.QuantaRownum{1, 2},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !result.Encoded || result.Dictionary != "lineitem.l_shipmode" {
		t.Fatalf("result = %#v, want encoded dictionary ids", result)
	}
	if result.Values[0].Value != int64(2) || result.Values[1].Value != int64(7) {
		t.Fatalf("values = %#v, want dictionary ids 2 and 7", result.Values)
	}
}

func TestNativeProjectionDictionaryIDCellsPreservesRequestedOrder(t *testing.T) {
	air := roaring64.NewBitmap()
	air.Add(7)
	air.Add(9)
	truck := roaring64.NewBitmap()
	truck.Add(11)

	values := nativeProjectionDictionaryIDCells([]qsbridge.QuantaRownum{11, 8, 7, 9}, map[uint64]*roaring64.Bitmap{
		2: air,
		7: truck,
	})

	if len(values) != 4 {
		t.Fatalf("len(values) = %d, want 4", len(values))
	}
	if values[0].Value != int64(7) {
		t.Fatalf("values[0] = %#v, want dictionary id 7", values[0])
	}
	if values[1].Kind != qsbridge.ValueNull {
		t.Fatalf("values[1] = %#v, want null for missing rownum", values[1])
	}
	if values[2].Value != int64(2) || values[3].Value != int64(2) {
		t.Fatalf("values = %#v, want requested order dictionary ids", values)
	}
}

func TestNativeProjectionDictionaryIDCellsChoosesLowestIDForMultiValueRow(t *testing.T) {
	low := roaring64.NewBitmap()
	low.Add(42)
	high := roaring64.NewBitmap()
	high.Add(42)

	values := nativeProjectionDictionaryIDCells([]qsbridge.QuantaRownum{42}, map[uint64]*roaring64.Bitmap{
		9: high,
		3: low,
	})

	if values[0].Value != int64(3) {
		t.Fatalf("values[0] = %#v, want lowest dictionary id", values[0])
	}
}

func TestLegacyDirectProjectionBSIFieldReaderReadsStringLexRemainderKeys(t *testing.T) {
	attr := &core.Attribute{BasicAttribute: &shared.BasicAttribute{
		FieldName:       "c_name",
		Type:            "String",
		MappingStrategy: "StringLexBSI",
		MapperConfig:    map[string]string{"length": "8"},
		Size:            32,
	}}
	table := &core.Table{
		BasicTable:       &shared.BasicTable{Name: "customer"},
		AttributeNameMap: map[string]*core.Attribute{"c_name": attr},
	}
	attr.Parent = table
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"customer": table,
	}}
	mapper, err := core.NewStringLexBSIMapper(map[string]string{"length": "8"})
	if err != nil {
		t.Fatalf("NewStringLexBSIMapper returned error: %v", err)
	}
	encoded, err := mapper.MapValue(attr, "Buenos Aires", nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(11, encoded)
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "customer" || request.PhysicalField != "c_name" {
				t.Fatalf("read request = %#v, want customer.c_name", request)
			}
			return NativeProjectionBSIReadResult{BSI: bsi}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "customer",
		Field:   qsbridge.QuantaProjectionField{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString},
		Rownums: []qsbridge.QuantaRownum{11},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !result.Encoded || result.LookupKind != NativeProjectionLookupBackingString || result.LookupRef != "customer.c_name" {
		t.Fatalf("result = %#v, want encoded StringLex remainder lookup keys", result)
	}
	key, ok := result.Values[0].Value.(NativeProjectionStringRemainderKey)
	if !ok || key.RowNum != 11 || key.Prefix != "Buenos A" {
		t.Fatalf("values = %#v, want rownum 11 with StringLex prefix Buenos A", result.Values)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderProjectsFullInlineStringLexBSI(t *testing.T) {
	attr := &core.Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "p_brand",
			Type:            "String",
			MappingStrategy: "StringLexBSI",
			MapperConfig:    map[string]string{"length": "10"},
			Size:            10,
		},
		Parent: &core.Table{BasicTable: &shared.BasicTable{Name: "part"}},
	}
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"part": {
			BasicTable:       &shared.BasicTable{Name: "part"},
			AttributeNameMap: map[string]*core.Attribute{"p_brand": attr},
		},
	}}
	mapper, err := core.NewStringLexBSIMapper(map[string]string{"length": "10"})
	if err != nil {
		t.Fatalf("NewStringLexBSIMapper returned error: %v", err)
	}
	encoded, err := mapper.MapValue(attr, "Brand#45", nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(7, encoded)
	reader := NativeProjectionBSIFieldReader{
		TableCache: tableCache,
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "part" || request.PhysicalField != "p_brand" {
				t.Fatalf("read request = %#v, want part.p_brand", request)
			}
			return NativeProjectionBSIReadResult{BSI: bsi}, nil, nil
		}),
	}

	result, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "part",
		Field:   qsbridge.QuantaProjectionField{Index: "part", Field: "p_brand", Type: qsbridge.DataTypeString},
		Rownums: []qsbridge.QuantaRownum{7},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(result.Values) != 1 || result.Values[0].Kind != qsbridge.ValueString || result.Values[0].Value != "Brand#45" {
		t.Fatalf("values = %#v, want Brand#45 string", result.Values)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderBlocksStringLexRemainderProjection(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_comment": {BasicAttribute: &shared.BasicAttribute{
					FieldName:       "l_comment",
					Type:            "String",
					MappingStrategy: "StringLexBSI",
					MapperConfig:    map[string]string{"length": "8"},
					Size:            256,
				}},
			},
		},
	}}
	reader := NativeProjectionBSIFieldReader{TableCache: tableCache}

	_, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_comment", Type: qsbridge.DataTypeString},
		Rownums: []qsbridge.QuantaRownum{1},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if !diagnostics.BlocksNative() || diagnostics.Codes()[0] != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostics = %#v, want unsupported StringLex remainder projection", diagnostics)
	}
}

func TestLegacyDirectProjectionBSIFieldReaderFallsBackForStringEnumWithoutDictionaryReader(t *testing.T) {
	tableCache := &core.TableCacheStruct{TableCache: map[string]*core.Table{
		"lineitem": {
			BasicTable: &shared.BasicTable{Name: "lineitem"},
			AttributeNameMap: map[string]*core.Attribute{
				"l_shipmode": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipmode", Type: "String", MappingStrategy: "StringEnum"}},
			},
		},
	}}
	reader := NativeProjectionBSIFieldReader{TableCache: tableCache}

	_, diagnostics, err := reader.ReadProjectionField(context.Background(), NativeProjectionFieldReadRequest{
		Index:   "lineitem",
		Field:   qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString},
		Rownums: []qsbridge.QuantaRownum{1},
	})
	if err != nil {
		t.Fatalf("ReadProjectionField error = %v", err)
	}
	if !diagnostics.BlocksNative() || diagnostics.Codes()[0] != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostics = %#v, want unsupported fallback", diagnostics)
	}
}

func legacyProjectionTestProbeName(probes []ExecutionProbe, name string) bool {
	for _, probe := range probes {
		if probe.Name == name {
			return true
		}
	}
	return false
}
