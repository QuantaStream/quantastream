package qsruntime

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type yearBucketBoundsTestProvider struct {
	minYear   int
	maxYear   int
	queryFunc func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error)
}

func (p yearBucketBoundsTestProvider) BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	return DirectSessionHandleFunc{
		QueryFunc:   p.queryFunc,
		ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
	}, nil, nil
}

func (p yearBucketBoundsTestProvider) TimeBucketYearBounds(ctx context.Context, request ExecutionRequest, field qsbridge.FieldRef) (int, int, bool) {
	return p.minYear, p.maxYear, true
}

type fakeBitmapGroupCountReader struct {
	Result BitmapGroupCountReadResult
	Calls  int
	Last   BitmapGroupCountReadRequest
}

func (r *fakeBitmapGroupCountReader) ReadBitmapGroupCounts(ctx context.Context, request BitmapGroupCountReadRequest) (BitmapGroupCountReadResult, qsbridge.DiagnosticSet, bool, error) {
	r.Calls++
	r.Last = request
	return r.Result, nil, true, nil
}

type fakeBitmapGroupAggregateReader struct {
	Result BitmapGroupAggregateReadResult
	Calls  int
	Last   BitmapGroupAggregateReadRequest
}

func (r *fakeBitmapGroupAggregateReader) ReadBitmapGroupAggregates(ctx context.Context, request BitmapGroupAggregateReadRequest) (BitmapGroupAggregateReadResult, qsbridge.DiagnosticSet, bool, error) {
	r.Calls++
	r.Last = request
	return r.Result, nil, true, nil
}

func TestDirectBitmapSeedMembershipOnlyRequestUsesLeftFieldExistence(t *testing.T) {
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	request := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{}, qsbridge.ExecutionRequest{})
	request.SourceIndexes = []string{"partsupp"}
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  qsbridge.FieldRef{Table: partsupp, Name: "ps_partkey", PhysicalName: "ps_partkey", Type: qsbridge.DataTypeInt},
		Right: qsbridge.FieldRef{Table: part, Name: "p_partkey", PhysicalName: "p_partkey", Type: qsbridge.DataTypeInt},
		Kind:  qsbridge.MembershipSemi,
	}}

	seeded := directBitmapSeedMembershipOnlyRequest(request)
	if len(seeded.Query.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(seeded.Query.Fragments))
	}
	fragment := seeded.Query.Fragments[0]
	if fragment.Index != "partsupp" || fragment.Field != "ps_partkey" || !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want partsupp.ps_partkey is not null", fragment)
	}
}

func TestDirectBitmapCellEqualCoercesSQLTimestampLiteral(t *testing.T) {
	cellTime := time.Date(2010, 1, 3, 0, 0, 0, 0, time.UTC)
	cell := qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: cellTime}
	literal := qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "2010-01-03 00:00:00.000Z"}

	if !directBitmapCellEqual(cell, literal) {
		t.Fatalf("timestamp cell did not equal SQL timestamp literal")
	}
}

func TestDirectBitmapMaterializedDayOfWeekUsesMySQLConvention(t *testing.T) {
	orderDate := qsbridge.FieldRef{
		Table:        qsbridge.TableInstance{Table: "orders_qa"},
		Name:         "order_date",
		PhysicalName: "order_date",
		Type:         qsbridge.DataTypeTime,
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{Index: "orders_qa", Field: "order_date"},
			Values: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueTime,
				Value: time.Date(2023, 6, 10, 3, 0, 0, 0, time.UTC),
			}},
		}},
	}

	cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(
		qsbridge.Call("dayofweek", qsbridge.Field(orderDate)),
		rowSet,
		0,
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if cell.Kind != qsbridge.ValueInt || cell.Value != int64(7) {
		t.Fatalf("dayofweek = %#v, want MySQL Saturday value 7", cell)
	}
}

func TestDirectBitmapEvaluateMaterializedDateFormatCall(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{}
	call := qsbridge.Call(
		"date_format",
		qsbridge.Literal(qsbridge.ValueString, "1995-03-15 04:05:06"),
		qsbridge.Literal(qsbridge.ValueString, "%Y-%m %d %H:%i:%s %W"),
	)

	cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(call, rowSet, 0)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if cell.Kind != qsbridge.ValueString || cell.Value != "1995-03 15 04:05:06 Wednesday" {
		t.Fatalf("date_format = %#v, want formatted timestamp", cell)
	}
}

func TestDirectBitmapEvaluateMaterializedDateHelperCalls(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{}
	tests := []struct {
		name string
		call qsbridge.CallExpr
		want qsbridge.ResultCell
	}{
		{
			name: "date",
			call: qsbridge.Call("date", qsbridge.Literal(qsbridge.ValueString, "1995-03-15 04:05:06")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "1995-03-15"},
		},
		{
			name: "dayname",
			call: qsbridge.Call("dayname", qsbridge.Literal(qsbridge.ValueString, "1995-03-15")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Wednesday"},
		},
		{
			name: "monthname",
			call: qsbridge.Call("monthname", qsbridge.Literal(qsbridge.ValueString, "1995-03-15")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "March"},
		},
		{
			name: "quarter",
			call: qsbridge.Call("quarter", qsbridge.Literal(qsbridge.ValueString, "1995-03-15")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(1)},
		},
		{
			name: "week",
			call: qsbridge.Call("week", qsbridge.Literal(qsbridge.ValueString, "1995-03-15")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(11)},
		},
		{
			name: "week_mode_zero_before_first_sunday",
			call: qsbridge.Call("week", qsbridge.Literal(qsbridge.ValueString, "1996-01-02"), qsbridge.Literal(qsbridge.ValueInt, int64(0))),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(test.call, rowSet, 0)
			if diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", diagnostics)
			}
			if cell.Kind != test.want.Kind || cell.Value != test.want.Value {
				t.Fatalf("cell = %#v, want %#v", cell, test.want)
			}
		})
	}
}

func TestDirectBitmapEvaluateMaterializedNumericScalarCalls(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{}
	tests := []struct {
		name string
		call qsbridge.CallExpr
		want qsbridge.ResultCell
	}{
		{
			name: "floor",
			call: qsbridge.Call("floor", qsbridge.Literal(qsbridge.ValueFloat, 3.8)),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 3.0},
		},
		{
			name: "ceiling",
			call: qsbridge.Call("ceiling", qsbridge.Literal(qsbridge.ValueFloat, 3.2)),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 4.0},
		},
		{
			name: "truncate",
			call: qsbridge.Call("truncate", qsbridge.Literal(qsbridge.ValueFloat, 3.14159), qsbridge.Literal(qsbridge.ValueInt, int64(2))),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 3.14},
		},
		{
			name: "mod",
			call: qsbridge.Call("mod", qsbridge.Literal(qsbridge.ValueInt, int64(17)), qsbridge.Literal(qsbridge.ValueInt, int64(5))),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 2.0},
		},
		{
			name: "power",
			call: qsbridge.Call("power", qsbridge.Literal(qsbridge.ValueInt, int64(2)), qsbridge.Literal(qsbridge.ValueInt, int64(3))),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 8.0},
		},
		{
			name: "sign",
			call: qsbridge.Call("sign", qsbridge.Literal(qsbridge.ValueFloat, -7.5)),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(-1)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(test.call, rowSet, 0)
			if diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", diagnostics)
			}
			if cell.Kind != test.want.Kind || cell.Value != test.want.Value {
				t.Fatalf("cell = %#v, want %#v", cell, test.want)
			}
		})
	}
}

func TestDirectBitmapEvaluateMaterializedStringBreadthCalls(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{}
	tests := []struct {
		name string
		call qsbridge.CallExpr
		want qsbridge.ResultCell
	}{
		{
			name: "concat_ws",
			call: qsbridge.Call("concat_ws", qsbridge.Literal(qsbridge.ValueString, "|"), qsbridge.Literal(qsbridge.ValueString, "quanta"), qsbridge.Literal(qsbridge.ValueString, "stream")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "quanta|stream"},
		},
		{
			name: "repeat",
			call: qsbridge.Call("repeat", qsbridge.Literal(qsbridge.ValueString, "ha"), qsbridge.Literal(qsbridge.ValueInt, int64(3))),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "hahaha"},
		},
		{
			name: "reverse",
			call: qsbridge.Call("reverse", qsbridge.Literal(qsbridge.ValueString, "stream")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "maerts"},
		},
		{
			name: "lpad",
			call: qsbridge.Call("lpad", qsbridge.Literal(qsbridge.ValueString, "42"), qsbridge.Literal(qsbridge.ValueInt, int64(5)), qsbridge.Literal(qsbridge.ValueString, "0")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "00042"},
		},
		{
			name: "rpad",
			call: qsbridge.Call("rpad", qsbridge.Literal(qsbridge.ValueString, "QS"), qsbridge.Literal(qsbridge.ValueInt, int64(5)), qsbridge.Literal(qsbridge.ValueString, ".")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "QS..."},
		},
		{
			name: "lpad empty pad string",
			call: qsbridge.Call("lpad", qsbridge.Literal(qsbridge.ValueString, "x"), qsbridge.Literal(qsbridge.ValueInt, int64(4)), qsbridge.Literal(qsbridge.ValueString, "")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""},
		},
		{
			name: "rpad empty pad string",
			call: qsbridge.Call("rpad", qsbridge.Literal(qsbridge.ValueString, "x"), qsbridge.Literal(qsbridge.ValueInt, int64(4)), qsbridge.Literal(qsbridge.ValueString, "")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""},
		},
		{
			name: "ascii",
			call: qsbridge.Call("ascii", qsbridge.Literal(qsbridge.ValueString, "QuantaStream")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(81)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(test.call, rowSet, 0)
			if diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", diagnostics)
			}
			if cell.Kind != test.want.Kind || cell.Value != test.want.Value {
				t.Fatalf("cell = %#v, want %#v", cell, test.want)
			}
		})
	}
}

func TestDirectBitmapEvaluateMaterializedNullControlCalls(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{}
	tests := []struct {
		name string
		call qsbridge.CallExpr
		want qsbridge.ResultCell
	}{
		{
			name: "isnull_true",
			call: qsbridge.Call("isnull", qsbridge.Literal(qsbridge.ValueNull, nil)),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(1)},
		},
		{
			name: "isnull_false",
			call: qsbridge.Call("isnull", qsbridge.Literal(qsbridge.ValueString, "value")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)},
		},
		{
			name: "if_numeric_true",
			call: qsbridge.Call("if", qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Literal(qsbridge.ValueString, "yes"), qsbridge.Literal(qsbridge.ValueString, "no")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "yes"},
		},
		{
			name: "if_numeric_false",
			call: qsbridge.Call("if", qsbridge.Literal(qsbridge.ValueInt, int64(0)), qsbridge.Literal(qsbridge.ValueString, "yes"), qsbridge.Literal(qsbridge.ValueString, "no")),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "no"},
		},
		{
			name: "if_predicate_true",
			call: qsbridge.Call(
				"if",
				qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Literal(qsbridge.ValueInt, int64(17)), qsbridge.Literal(qsbridge.ValueInt, int64(10))),
				qsbridge.Literal(qsbridge.ValueString, "big"),
				qsbridge.Literal(qsbridge.ValueString, "small"),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "big"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(test.call, rowSet, 0)
			if diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", diagnostics)
			}
			if cell.Kind != test.want.Kind || cell.Value != test.want.Value {
				t.Fatalf("cell = %#v, want %#v", cell, test.want)
			}
		})
	}
}

func TestDirectBitmapRuntimeAppliesRelationshipVectorMembership(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	customerID := qsbridge.FieldRef{Table: customers, Name: "cust_id", PhysicalName: "cust_id", Type: qsbridge.DataTypeString}
	orderCustomerID := qsbridge.FieldRef{
		Table:        orders,
		Name:         "cust_id",
		PhysicalName: "cust_id",
		Type:         qsbridge.DataTypeString,
		Encoding:     qsbridge.EncodingProfile{LegacyName: "ParentRelation"},
	}
	for _, test := range []struct {
		name string
		kind qsbridge.MembershipKind
		want uint64
	}{
		{name: "semi", kind: qsbridge.MembershipSemi, want: 2},
		{name: "anti", kind: qsbridge.MembershipAnti, want: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := DirectBitmapRuntime{
				Adapter: BitmapQueryResultAdapter{},
				Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
					return DirectSessionHandleFunc{
						QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
							index, ok := request.RootIndex()
							if !ok {
								t.Fatalf("membership request had no root index")
							}
							switch index {
							case "customers_qa":
								return BitmapQueryResult{Success: true, Count: 10, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, nil, nil
							case "orders_qa":
								return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{101, 102, 103}}, nil, nil
							default:
								t.Fatalf("unexpected bitmap request root index %q", index)
								return BitmapQueryResult{}, nil, nil
							}
						},
					}, nil, nil
				}),
				Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
					t.Fatalf("relationship-vector membership should not materialize business keys for %s", request.Index)
					return qsbridge.QuantaProjectedRowSet{}, nil, nil
				}),
				RelationshipReader: InMemoryRelationshipVectorIndex{Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
					"orders_qa.cust_id": {
						101: {1},
						102: {1},
						103: {10},
					},
				}},
			}
			request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
				Fragments: []qsbridge.QuantaQueryFragment{{
					Index:     "customers_qa",
					Field:     "cust_id",
					Operation: qsbridge.QuantaOperationIntersect,
					NullCheck: true,
					Negate:    true,
				}},
			})
			request.Memberships = []qsbridge.MembershipEdge{{
				Left:  customerID,
				Right: orderCustomerID,
				Kind:  test.kind,
				Legal: true,
			}}
			request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "customer_count", Type: qsbridge.DataTypeInt}}

			result, err := runtime.ExecuteDirect(context.Background(), request)
			if err != nil {
				t.Fatalf("execute direct: %v", err)
			}
			if result.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
			}
			if result.RowSet.CandidateCount() != 1 || len(result.RowSet.ProjectionVectors) != 1 || len(result.RowSet.ProjectionVectors[0].Values) != 1 {
				t.Fatalf("rowset = %#v, want one count aggregate cell", result.RowSet)
			}
			if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(test.want) {
				t.Fatalf("count aggregate = %#v, want %d", got, test.want)
			}
		})
	}
}

func TestDirectBitmapRuntimeRecordsScratchpadInstrumentation(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())
	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{
						Success: true,
						Count:   2,
						Rownums: []qsbridge.QuantaRownum{10, 20},
					}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "customers_qa",
			Field:     "cust_id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})

	result, err := runtime.ExecuteDirect(ctx, request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbeName(t, result.Probes, "direct_bitmap", "phase_bitmap_query_elapsed")

	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil || scratchpad.Instrumentation == nil {
		t.Fatal("scratchpad instrumentation was not installed")
	}
	snapshot := scratchpad.Instrumentation.Snapshot()
	assertExecutionTimingName(t, snapshot, "direct_bitmap", "phase_bitmap_query_elapsed")
	assertExecutionCounter(t, snapshot, "direct_bitmap", "fragment_count", 1)
	assertExecutionCounter(t, snapshot, "direct_bitmap", "bitmap_count", 2)
}

func TestDirectBitmapRuntimeFiltersRelationshipVectorMembershipResiduals(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	customerID := qsbridge.FieldRef{Table: customers, Name: "cust_id", PhysicalName: "cust_id", Type: qsbridge.DataTypeString}
	orderCustomerID := qsbridge.FieldRef{
		Table:        orders,
		Name:         "cust_id",
		PhysicalName: "cust_id",
		Type:         qsbridge.DataTypeString,
		Encoding:     qsbridge.EncodingProfile{LegacyName: "ParentRelation"},
	}
	shipVia := qsbridge.FieldRef{Table: orders, Name: "ship_via", PhysicalName: "ship_via", Type: qsbridge.DataTypeString}
	materialized := false
	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					index, ok := request.RootIndex()
					if !ok {
						t.Fatalf("membership request had no root index")
					}
					switch index {
					case "customers_qa":
						return BitmapQueryResult{Success: true, Count: 10, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, nil, nil
					case "orders_qa":
						return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{101, 102, 103}}, nil, nil
					default:
						t.Fatalf("unexpected bitmap request root index %q", index)
						return BitmapQueryResult{}, nil, nil
					}
				},
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialized = true
			if request.Index != "orders_qa" {
				t.Fatalf("materialization index = %q, want orders_qa", request.Index)
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   "orders_qa",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field: qsbridge.QuantaProjectionField{Index: "orders_qa", Field: "cust_id", PhysicalName: "cust_id"},
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueInt, Value: int64(1)},
							{Kind: qsbridge.ValueInt, Value: int64(1)},
							{Kind: qsbridge.ValueInt, Value: int64(10)},
						},
					},
					{
						Field: qsbridge.QuantaProjectionField{Index: "orders_qa", Field: "ship_via", PhysicalName: "ship_via"},
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "UPS"},
							{Kind: qsbridge.ValueString, Value: "UPS"},
							{Kind: qsbridge.ValueString, Value: "FEDEX"},
						},
					},
				},
			}, nil, nil
		}),
		RelationshipReader: InMemoryRelationshipVectorIndex{Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			"orders_qa.cust_id": {
				101: {1},
				102: {1},
				103: {10},
			},
		}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "customers_qa",
			Field:     "cust_id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  customerID,
		Right: orderCustomerID,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(shipVia), qsbridge.Literal(qsbridge.ValueString, "UPS")),
			Placement: qsbridge.PredicatePushdown,
		}},
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "customer_count", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !materialized {
		t.Fatalf("expected residual materialization")
	}
	if result.RowSet.CandidateCount() != 1 || len(result.RowSet.ProjectionVectors) != 1 || len(result.RowSet.ProjectionVectors[0].Values) != 1 {
		t.Fatalf("rowset = %#v, want one count aggregate cell", result.RowSet)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(1) {
		t.Fatalf("count aggregate = %#v, want 1", got)
	}
}

func TestDirectBitmapRuntimePrefiltersCorrelatedMembershipRightSameRowResidual(t *testing.T) {
	leftTable := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	rightTable := qsbridge.TableInstance{Table: "lineitem", Alias: "l3"}
	leftOrderKey := qsbridge.FieldRef{Table: leftTable, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	leftSuppKey := qsbridge.FieldRef{Table: leftTable, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	rightOrderKey := qsbridge.FieldRef{Table: rightTable, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	rightSuppKey := qsbridge.FieldRef{Table: rightTable, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	rightReceiptDate := qsbridge.FieldRef{Table: rightTable, Name: "l_receiptdate", PhysicalName: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	rightCommitDate := qsbridge.FieldRef{Table: rightTable, Name: "l_commitdate", PhysicalName: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	sameRowCalled := false
	rightKeyNarrowCalled := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					index, ok := request.RootIndex()
					if !ok || index != "lineitem" {
						t.Fatalf("membership right request root index = %q/%t, want lineitem", index, ok)
					}
					if len(request.Query.Fragments) == 1 && request.Query.Fragments[0].BSIOp == qsbridge.QuantaBSIOpBatchEQ {
						rightKeyNarrowCalled = true
						if got, want := len(request.Query.Fragments[0].Values), 2; got != want {
							t.Fatalf("right key lookup values = %d, want %d", got, want)
						}
						return BitmapQueryResult{Success: true, Count: 2, Rownums: []qsbridge.QuantaRownum{10, 11}}, nil, nil
					}
					return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{10, 11, 12}}, nil, nil
				},
			}, nil, nil
		}),
		SameRowComparison: SameRowComparisonKernelFunc(func(_ context.Context, comparison qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
			sameRowCalled = true
			if !sameRownumSlicesEqual(comparison.Domain.Rownums, []qsbridge.QuantaRownum{10, 11}) {
				t.Fatalf("same-row domain = %#v, want key-narrowed right-side candidates", comparison.Domain.Rownums)
			}
			return qsbridge.SameRowComparisonResult{
				ID: comparison.ID,
				Domain: qsbridge.RownumDomainSet{
					Domain:  comparison.Domain.Domain,
					Rownums: []qsbridge.QuantaRownum{10},
				},
			}, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			if request.RequestCount() != 1 {
				t.Fatalf("materialization request count = %d, want one", request.RequestCount())
			}
			materializationRequest := request.Requests[0]
			if sameRownumSlicesEqual(materializationRequest.Rownums, []qsbridge.QuantaRownum{10, 11, 12}) {
				t.Fatalf("right side was materialized before native same-row filtering")
			}
			if sameRownumSlicesEqual(materializationRequest.Rownums, []qsbridge.QuantaRownum{10, 12}) {
				t.Fatalf("right side was materialized before dynamic key narrowing")
			}
			for _, field := range materializationRequest.ProjectionFields {
				if field.Field == "l_receiptdate" || field.Field == "l_commitdate" {
					t.Fatalf("same-row date field %s should not require residual materialization", field.Field)
				}
			}
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   materializationRequest.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), materializationRequest.Rownums...),
			}
			for _, field := range materializationRequest.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(materializationRequest.Rownums))
				for _, rownum := range materializationRequest.Rownums {
					values = append(values, membershipTestIntCell(rownum, field.Field))
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{Field: field, Values: values})
			}
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: request.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      materializationRequest.DependencyID,
					Request: materializationRequest,
					RowSet:  rowSet,
				}},
			}, nil
		}),
	}
	membership := qsbridge.MembershipEdge{
		Left:  leftOrderKey,
		Right: rightOrderKey,
		Kind:  qsbridge.MembershipSemi,
		Predicates: []qsbridge.Predicate{
			{
				Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(rightOrderKey), qsbridge.Field(leftOrderKey)),
				Placement: qsbridge.PredicateResidualScan,
			},
			{
				Expr:      qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(rightSuppKey), qsbridge.Field(leftSuppKey)),
				Placement: qsbridge.PredicateResidualScan,
			},
			{
				Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Field(rightReceiptDate), qsbridge.Field(rightCommitDate)),
				Placement: qsbridge.PredicateResidualScan,
			},
		},
	}

	filtered, probes, diagnostics, err := runtime.directBitmapApplyMembership(context.Background(), ExecutionRequest{}, BitmapQueryResult{
		Success: true,
		Count:   2,
		Rownums: []qsbridge.QuantaRownum{1, 2},
	}, membership, BitmapQueryResult{})
	if err != nil {
		t.Fatalf("apply membership: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !sameRowCalled {
		t.Fatalf("same-row kernel was not called")
	}
	if !rightKeyNarrowCalled {
		t.Fatalf("right key narrowing query was not called")
	}
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_narrow_left_key_count", "2")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_narrow_mode", "folded_right_request")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_narrow_right_candidates_after", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_narrow_applied", "true")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_index_keys", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_index_rows", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_index_max_bucket_size", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_index_multirow_keys", "0")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_evaluation_bucket_hits", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_evaluation_bucket_misses", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_evaluation_matched_rows", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_evaluation_first_candidate_matches", "1")
	if !sameRownumSlicesEqual(filtered.Rownums, []qsbridge.QuantaRownum{1}) {
		t.Fatalf("filtered rownums = %#v, want row 1", filtered.Rownums)
	}
}

func membershipTestIntCell(rownum qsbridge.QuantaRownum, field string) qsbridge.ResultCell {
	valuesByField := map[string]map[qsbridge.QuantaRownum]int64{
		"l_orderkey": {
			1:  100,
			2:  200,
			10: 100,
			12: 300,
		},
		"l_suppkey": {
			1:  10,
			2:  20,
			10: 11,
			12: 30,
		},
	}
	value, ok := valuesByField[field][rownum]
	if !ok {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value}
}

func sameRownumSlicesEqual(left []qsbridge.QuantaRownum, right []qsbridge.QuantaRownum) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestDirectBitmapRuntimeExecutesBorrowedSession(t *testing.T) {
	released := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{
						Success: true,
						Count:   2,
						Rownums: []qsbridge.QuantaRownum{1, 2},
					}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet {
					released = true
					return nil
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !released {
		t.Fatalf("session was not released")
	}
	if result.Count != 2 {
		t.Fatalf("count = %d, want 2", result.Count)
	}
	if got := result.CandidateCount(); got != 2 {
		t.Fatalf("candidate count = %d, want 2", got)
	}
}

func TestDirectBitmapRuntimeMaterializesProjectionThroughKernel(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	called := false
	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			called = true
			if request.RequestCount() != 1 {
				t.Fatalf("request count = %d, want one", request.RequestCount())
			}
			materializationRequest := request.Requests[0]
			if materializationRequest.Index != "orders" || len(materializationRequest.Rownums) != 2 {
				t.Fatalf("materialization request = %#v, want orders candidate rownums", materializationRequest)
			}
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: request.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      materializationRequest.DependencyID,
					Request: materializationRequest,
					RowSet: qsbridge.QuantaProjectedRowSet{
						Index:   "orders",
						Rownums: append([]qsbridge.QuantaRownum(nil), materializationRequest.Rownums...),
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Field: field,
							Values: []qsbridge.ResultCell{
								{Kind: qsbridge.ValueInt, Value: int64(1001)},
								{Kind: qsbridge.ValueInt, Value: int64(1002)},
							},
						}},
					},
				}},
				Probes: []qsbridge.ProjectionProbe{{Section: "projection_materialization", Name: "kernel_called", Value: "true"}},
			}, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{field},
	}).WithCandidateSet(qsbridge.QuantaCandidateSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{7, 8},
	})

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !called {
		t.Fatalf("materialization kernel was not called")
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.RowSet.CandidateCount() != 2 {
		t.Fatalf("rowset = %#v, want two materialized rows", result.RowSet)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[1].Value; got != int64(1002) {
		t.Fatalf("second projection value = %#v, want 1002", got)
	}
	if len(result.Probes) == 0 {
		t.Fatalf("probes = %#v, want kernel probe forwarded", result.Probes)
	}
}

func TestDirectBitmapDistinctProjectedRowSetKeepsFirstVisibleTuple(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "customers_qa",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "customers_qa", Field: "first_name", Visible: true},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "Abe"},
					{Kind: qsbridge.ValueString, Value: "Abe"},
					{Kind: qsbridge.ValueString, Value: "Annie"},
					{Kind: qsbridge.ValueString, Value: "Abe"},
				},
			},
			{
				Field: qsbridge.QuantaProjectionField{Index: "customers_qa", Field: "cust_id", Visible: false},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "101"},
					{Kind: qsbridge.ValueString, Value: "102"},
					{Kind: qsbridge.ValueString, Value: "103"},
					{Kind: qsbridge.ValueString, Value: "104"},
				},
			},
		},
	}

	distinct := directBitmapDistinctProjectedRowSet(rowSet)
	if got := distinct.Rownums; len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("rownums = %#v, want first Abe and Annie rows", got)
	}
	values := distinct.ProjectionVectors[0].Values
	if len(values) != 2 || values[0].Value != "Abe" || values[1].Value != "Annie" {
		t.Fatalf("visible values = %#v, want Abe/Annie", values)
	}
	hidden := distinct.ProjectionVectors[1].Values
	if len(hidden) != 2 || hidden[0].Value != "101" || hidden[1].Value != "103" {
		t.Fatalf("hidden values = %#v, want rows aligned with kept visible tuples", hidden)
	}
}

func TestDirectBitmapRuntimeExecutesMutationThroughSession(t *testing.T) {
	released := false
	called := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				MutationFunc: func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
					called = true
					if request.Mutation.Kind != qsbridge.MutationUpdate {
						t.Fatalf("mutation kind = %q, want update", request.Mutation.Kind)
					}
					return qsbridge.StatementResult{AffectedRows: 2, Status: "Rows matched: 2"}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet {
					released = true
					return nil
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), ExecutionRequest{
		Mutation: qsbridge.MutationShape{Kind: qsbridge.MutationUpdate},
	})
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !called || !released {
		t.Fatalf("called=%v released=%v, want both true", called, released)
	}
	if result.Statement.AffectedRows != 2 {
		t.Fatalf("affected rows = %d, want 2", result.Statement.AffectedRows)
	}
}

func TestDirectBitmapRuntimeExecutesTruncateMutationThroughSession(t *testing.T) {
	called := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			if request.Mutation.Target.Table != "customers_qa" {
				t.Fatalf("session request target = %#v, want customers_qa", request.Mutation.Target)
			}
			return DirectSessionHandleFunc{
				MutationFunc: func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
					called = true
					if request.Mutation.Kind != qsbridge.MutationTruncate {
						t.Fatalf("mutation kind = %q, want truncate", request.Mutation.Kind)
					}
					return qsbridge.StatementResult{Status: "Table customers_qa truncated"}, nil, nil
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationTruncate,
			Target: qsbridge.TableInstance{Table: "customers_qa"},
		},
	})
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !called {
		t.Fatalf("mutation handler was not called")
	}
	if result.Statement.Status != "Table customers_qa truncated" {
		t.Fatalf("status = %q, want truncate status", result.Statement.Status)
	}
}

func TestDirectBitmapProjectedValuesUsesRoleForRepeatedAliases(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field:  qsbridge.QuantaProjectionField{Index: "nation", Role: "n1", Field: "n_name"},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "FRANCE"}},
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: "nation", Role: "n2", Field: "n_name"},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "GERMANY"}},
			},
		},
	}
	values, ok := directBitmapProjectedValues(rowSet, qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "nation", Alias: "n2"},
		Name:  "n_name",
	})
	if !ok {
		t.Fatalf("n2 values not found")
	}
	if len(values) != 1 || values[0].Value != "GERMANY" {
		t.Fatalf("n2 values = %#v, want GERMANY", values)
	}
}

func TestDirectBitmapRuntimeCountsSelfJoinUsingMultiplicity(t *testing.T) {
	left := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "nation", Alias: "n1"},
		Name:  "n_regionkey",
		Type:  qsbridge.DataTypeInt,
	}
	right := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "nation", Alias: "n2"},
		Name:  "n_regionkey",
		Type:  qsbridge.DataTypeInt,
	}
	materialized := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 5, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5}}, nil, nil
				},
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialized = true
			if request.Index != "nation" {
				t.Fatalf("materialization index = %q, want nation", request.Index)
			}
			if len(request.ProjectionFields) != 1 || request.ProjectionFields[0].Role != "n1" || request.ProjectionFields[0].Field != "n_regionkey" {
				t.Fatalf("projection fields = %#v, want n1.n_regionkey", request.ProjectionFields)
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   "nation",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueInt, Value: int64(1)},
						{Kind: qsbridge.ValueInt, Value: int64(1)},
						{Kind: qsbridge.ValueInt, Value: int64(2)},
						{Kind: qsbridge.ValueInt, Value: int64(2)},
						{Kind: qsbridge.ValueInt, Value: int64(3)},
					},
				}},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"nation"}
	request.Sources = []qsbridge.TableInstance{
		{Table: "nation", Alias: "n1"},
		{Table: "nation", Alias: "n2"},
	}
	request.Joins = []qsbridge.JoinEdge{{
		Left:      left,
		Right:     right,
		Kind:      qsbridge.JoinKindInner,
		Direction: qsbridge.JoinPeerEquality,
		Legal:     true,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "joined_rows",
		Type:     qsbridge.DataTypeInt,
	}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if !materialized {
		t.Fatalf("expected self-join key materialization")
	}
	if len(result.RowSet.ProjectionVectors) != 1 || len(result.RowSet.ProjectionVectors[0].Values) != 1 {
		t.Fatalf("row set = %#v, want single aggregate cell", result.RowSet)
	}
	cell := result.RowSet.ProjectionVectors[0].Values[0]
	if cell.Kind != qsbridge.ValueInt || cell.Value != int64(9) {
		t.Fatalf("joined_rows cell = %#v, want 9", cell)
	}
}

func TestDirectBitmapGroupExpressionIndexUsesRoleForRepeatedAliases(t *testing.T) {
	n1 := qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "nation", Alias: "n1"}, Name: "n_name"}
	n2 := qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "nation", Alias: "n2"}, Name: "n_name"}
	groups := []directBitmapGroupExpression{
		{Expr: qsbridge.Field(n1), Field: n1},
		{Expr: qsbridge.Field(n2), Field: n2},
	}
	if index := directBitmapGroupExpressionIndex(qsbridge.Field(n2), groups); index != 1 {
		t.Fatalf("n2 group index = %d, want 1", index)
	}
}

func TestDirectBitmapEvaluateMaterializedSearchedCaseExpr(t *testing.T) {
	priority := qsbridge.FieldRef{
		Table:        qsbridge.TableInstance{Table: "orders", Alias: "o"},
		Name:         "o_orderpriority",
		PhysicalName: "o_orderpriority",
		Type:         qsbridge.DataTypeString,
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{
				Index:        "orders",
				Field:        "o_orderpriority",
				PhysicalName: "o_orderpriority",
				Type:         qsbridge.DataTypeString,
				Visible:      true,
			},
			Values: []qsbridge.ResultCell{
				{Kind: qsbridge.ValueString, Value: "1-URGENT"},
				{Kind: qsbridge.ValueString, Value: "2-HIGH"},
				{Kind: qsbridge.ValueString, Value: "3-MEDIUM"},
			},
		}},
	}
	expr := qsbridge.SearchedCase(
		[]qsbridge.SearchedCaseWhen{{
			Condition: qsbridge.Binary(
				qsbridge.BinaryOpOr,
				qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(priority), qsbridge.Literal(qsbridge.ValueString, "1-URGENT")),
				qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(priority), qsbridge.Literal(qsbridge.ValueString, "2-HIGH")),
			),
			Result: qsbridge.Literal(qsbridge.ValueInt, int64(1)),
		}},
		qsbridge.Literal(qsbridge.ValueInt, int64(0)),
	)
	want := []int64{1, 1, 0}
	for i := range want {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(expr, rowSet, i)
		if diagnostics.BlocksNative() {
			t.Fatalf("row %d diagnostics = %#v", i, diagnostics)
		}
		if cell.Kind != qsbridge.ValueInt || cell.Value != want[i] {
			t.Fatalf("row %d cell = %#v, want int %d", i, cell, want[i])
		}
	}
}

func TestDirectBitmapRuntimeRejectsRelationshipVectorJoinBeforeBorrow(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "orders", Alias: "o"},
			Name:  "o_orderkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_orderkey",
		},
		Kind: qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
		Legal: true,
	}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called for relationship-vector join")
			return nil, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected relationship-vector join diagnostic")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticUnsupportedJoin {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticUnsupportedJoin)
	}
	if got := result.Diagnostics[0].Message; got != "relationship-vector join execution is not wired yet: o.o_orderkey -> l.l_orderkey" {
		t.Fatalf("diagnostic message = %q", got)
	}
}

func TestDirectBitmapRuntimeRejectsNonEquiPeerJoinBeforeBorrow(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "customer", Alias: "c"},
			Name:  "c_custkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "orders", Alias: "o"},
			Name:  "o_custkey",
		},
		Operator:  qsbridge.BinaryOpGreater,
		Kind:      qsbridge.JoinKindInner,
		Direction: qsbridge.JoinPeerEquality,
		Legal:     true,
	}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called for unsupported peer join")
			return nil, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected non-equi peer join diagnostic")
	}
	if !strings.Contains(result.Diagnostics[0].Message, "non-relationship peer joins") {
		t.Fatalf("diagnostic message = %q", result.Diagnostics[0].Message)
	}
}

func TestDirectBitmapRuntimeRejectsGroupedFilterBeforeBorrow(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment:  qsbridge.QuantaQueryFragment{Index: "orders", Field: "o_orderkey", Operation: qsbridge.QuantaOperationIntersect, BSIOp: qsbridge.QuantaBSIOpEQ},
		},
	})
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called for grouped filter")
			return nil, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected grouped filter diagnostic")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticUnsupportedSQL)
	}
}

func TestDirectBitmapFilterTreeAdapterBuildsCandidateSet(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{
					Operation: qsbridge.QuantaFilterIntersect,
					Children: []qsbridge.QuantaFilterExpression{
						{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "orders", Field: "a"}},
						{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "orders", Field: "b"}},
					},
				},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "orders", Field: "c"}},
			},
		},
	})
	adapter := DirectBitmapFilterTreeAdapter{Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
		return DirectSessionHandleFunc{
			QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
				field := request.Query.Fragments[0].Field
				switch field {
				case "a":
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
				case "b":
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{2, 3}}, nil, nil
				case "c":
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{5, 2}}, nil, nil
				default:
					t.Fatalf("unexpected filter leaf field %q", field)
					return BitmapQueryResult{}, nil, nil
				}
			},
		}, nil, nil
	})}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !adapted.HasCandidateSet {
		t.Fatalf("expected precomputed candidate set")
	}
	want := []qsbridge.QuantaRownum{2, 3, 5}
	if len(adapted.CandidateSet.Rownums) != len(want) {
		t.Fatalf("rownums = %#v, want %#v", adapted.CandidateSet.Rownums, want)
	}
	for i, rownum := range want {
		if adapted.CandidateSet.Rownums[i] != rownum {
			t.Fatalf("rownums = %#v, want %#v", adapted.CandidateSet.Rownums, want)
		}
	}
	if !adapted.Query.Filter.Empty() || len(adapted.Query.Fragments) != 0 {
		t.Fatalf("adapted query = %#v, want filter/fragments cleared", adapted.Query)
	}
}

func TestDirectBitmapFilterTreeAdapterEvaluatesIntersectLeafWithinCandidates(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterIntersect,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "first"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{
					Index:      "lineitem",
					Field:      "l_quantity",
					Operation:  qsbridge.QuantaOperationIntersect,
					BSIOp:      qsbridge.QuantaBSIOpGT,
					Literal:    qsbridge.Literal(qsbridge.ValueInt, int64(9)),
					HasLiteral: true,
				}},
			},
		},
	})
	queryCalls := 0
	materializeCalls := 0
	adapter := DirectBitmapFilterTreeAdapter{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryCalls++
					field := request.Query.Fragments[0].Field
					if field != "first" {
						t.Fatalf("unexpected bitmap leaf %q, want only first leaf to query broadly", field)
					}
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
				},
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializeCalls++
			if request.Index != "lineitem" || len(request.ProjectionFields) != 1 || request.ProjectionFields[0].Field != "l_quantity" {
				t.Fatalf("materialization request = %#v, want lineitem.l_quantity", request)
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(5)}, {Kind: qsbridge.ValueInt, Value: int64(10)}, {Kind: qsbridge.ValueInt, Value: int64(15)}},
				}},
			}, nil, nil
		}),
	}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if queryCalls != 1 {
		t.Fatalf("query calls = %d, want 1", queryCalls)
	}
	if materializeCalls != 1 {
		t.Fatalf("materialize calls = %d, want 1", materializeCalls)
	}
	want := []qsbridge.QuantaRownum{2, 3}
	if len(adapted.CandidateSet.Rownums) != len(want) {
		t.Fatalf("rownums = %#v, want %#v", adapted.CandidateSet.Rownums, want)
	}
	for i, rownum := range want {
		if adapted.CandidateSet.Rownums[i] != rownum {
			t.Fatalf("rownums = %#v, want %#v", adapted.CandidateSet.Rownums, want)
		}
	}
}

func TestDirectBitmapFilterTreeAdapterRejectsMixedRownumDomains(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{
					Operation: qsbridge.QuantaFilterIntersect,
					Children: []qsbridge.QuantaFilterExpression{
						{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
						{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
					},
				},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_shipmode"}},
			},
		},
	})
	adapter := DirectBitmapFilterTreeAdapter{Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
		t.Fatalf("session provider should not be called for mixed-domain grouped filter")
		return nil, nil, nil
	})}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticUnsupportedSQL)
	}
	for _, want := range []string{
		"sources=lineitem,part",
		"target=lineitem",
		"strategies=relationship_vector_normalization",
	} {
		if !strings.Contains(diagnostics[0].Message, want) {
			t.Fatalf("diagnostic message = %q, want %q", diagnostics[0].Message, want)
		}
	}
	if adapted.HasCandidateSet {
		t.Fatalf("mixed-domain filter should not produce a candidate set")
	}
}

func TestDirectBitmapFilterTreeAdapterRoutesMixedRownumDomainsToNormalizer(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind:     qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}}
	normalizer := &FixtureFilterDomainNormalizationExecutor{}
	adapter := DirectBitmapFilterTreeAdapter{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called before mixed-domain normalization")
			return nil, nil, nil
		}),
		Normalizer: normalizer,
	}

	_, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported normalization blocker", diagnostics)
	}
	if !normalizer.LastPlan.Required() {
		t.Fatalf("normalization plan = %#v, want required", normalizer.LastPlan)
	}
	if normalizer.LastPlan.Operation != FilterDomainNormalizeGroupedFilter {
		t.Fatalf("operation = %q, want grouped_filter", normalizer.LastPlan.Operation)
	}
	if len(normalizer.LastPlan.Requests) != 1 {
		t.Fatalf("requests = %#v, want one source-to-target request", normalizer.LastPlan.Requests)
	}
	normalizationRequest := normalizer.LastPlan.Requests[0]
	if normalizationRequest.SourceDomain != "part" || normalizationRequest.TargetDomain != "lineitem" {
		t.Fatalf("request domains = %s -> %s, want part -> lineitem", normalizationRequest.SourceDomain, normalizationRequest.TargetDomain)
	}
	if len(normalizationRequest.RelationshipPath) != 1 {
		t.Fatalf("relationship path = %#v, want one-hop path", normalizationRequest.RelationshipPath)
	}
}

func TestDirectBitmapFilterDomainTranslationTargetsRelationshipVectorChild(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "part", Field: "p_partkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	request.SourceIndexes = []string{"part", "lineitem"}
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table:    qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:     "l_partkey",
			Encoding: qsbridge.EncodingProfile{Kind: qsbridge.EncodingRelation},
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind: qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
	}}

	translation := directBitmapFilterDomainTranslation(request)
	if !translation.Required {
		t.Fatalf("translation = %#v, want required mixed-domain normalization", translation)
	}
	if translation.TargetDomain != "lineitem" {
		t.Fatalf("target = %q, want lineitem", translation.TargetDomain)
	}
}

func TestDirectBitmapFilterTreeAdapterContinuesAfterSuccessfulNormalization(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterIntersect,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind:     qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}}
	reader := InMemoryRelationshipVectorIndex{
		Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			"part.p_brand": {
				7: {2},
				8: {4},
			},
		},
	}
	normalizer := NewReaderBackedFilterDomainNormalizer(
		testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7, 8}},
		}},
		reader,
	)
	adapter := DirectBitmapFilterTreeAdapter{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					field := request.Query.Fragments[0].Field
					switch field {
					case "l_quantity":
						return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{4, 5}}, nil, nil
					default:
						t.Fatalf("unexpected filter leaf field %q", field)
						return BitmapQueryResult{}, nil, nil
					}
				},
			}, nil, nil
		}),
		Normalizer: normalizer,
	}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !adapted.HasCandidateSet {
		t.Fatalf("expected precomputed candidate set")
	}
	if len(adapted.CandidateSet.Rownums) != 1 || adapted.CandidateSet.Rownums[0] != 4 {
		t.Fatalf("rownums = %#v, want [4]", adapted.CandidateSet.Rownums)
	}
}

func TestDirectBitmapFilterTreeAdapterKeepsLegacyReaderBoundaryExplicit(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterIntersect,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind:     qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}}
	reader := &LegacyDirectRelationshipVectorReader{}
	adapter := DirectBitmapFilterTreeAdapter{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called after legacy reader boundary")
			return nil, nil, nil
		}),
		Normalizer: NewReaderBackedFilterDomainNormalizer(
			testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
				"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7, 8}},
			}},
			reader,
		),
	}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want legacy reader boundary", diagnostics)
	}
	if !strings.Contains(diagnostics[0].Message, "relationship-vector reader is not wired yet") {
		t.Fatalf("diagnostic message = %q, want legacy reader boundary", diagnostics[0].Message)
	}
	if adapted.HasCandidateSet {
		t.Fatalf("legacy reader boundary should not produce candidate set")
	}
	if reader.LastRequest.SourceDomain != "part" || reader.LastRequest.TargetDomain != "lineitem" {
		t.Fatalf("reader request domains = %s -> %s, want part -> lineitem", reader.LastRequest.SourceDomain, reader.LastRequest.TargetDomain)
	}
	if reader.LastRequest.LeafName() != "part.p_brand" {
		t.Fatalf("reader leaf = %q, want part.p_brand", reader.LastRequest.LeafName())
	}
}

func TestRelationshipVectorFilterDomainNormalizationKernelReportsMissingPath(t *testing.T) {
	kernel := RelationshipVectorFilterDomainNormalizationKernel{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7}},
		}},
		Translator: &FixtureFilterDomainRelationshipVectorTranslator{},
	}
	normalization := FilterDomainNormalizationRequest{
		Operation:    FilterDomainNormalizeGroupedFilter,
		SourceDomain: "part",
		TargetDomain: "lineitem",
		Strategy:     qsbridge.PhysicalStrategyRelationshipVectorNormalization,
	}

	_, diagnostics, err := kernel.NormalizeFilterLeaf(context.Background(), ExecutionRequest{}, normalization, qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"})
	if err != nil {
		t.Fatalf("NormalizeFilterLeaf error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if !strings.Contains(diagnostics[0].Message, "relationship path missing") {
		t.Fatalf("diagnostic message = %q, want path missing", diagnostics[0].Message)
	}
}

func TestRelationshipVectorFilterDomainNormalizationKernelReportsDirectionMismatch(t *testing.T) {
	normalization := FilterDomainNormalizationRequest{
		Operation:    FilterDomainNormalizeGroupedFilter,
		SourceDomain: "supplier",
		TargetDomain: "lineitem",
		Strategy:     qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{{
			Left: qsbridge.FieldRef{
				Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
				Name:  "l_partkey",
			},
			Right: qsbridge.FieldRef{
				Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
				Name:  "p_partkey",
			},
			ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
		}},
	}
	kernel := RelationshipVectorFilterDomainNormalizationKernel{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"s_name": {Index: "supplier", Rownums: []qsbridge.QuantaRownum{7}},
		}},
		Translator: &FixtureFilterDomainRelationshipVectorTranslator{},
	}

	_, diagnostics, err := kernel.NormalizeFilterLeaf(context.Background(), ExecutionRequest{}, normalization, qsbridge.QuantaQueryFragment{Index: "supplier", Field: "s_name"})
	if err != nil {
		t.Fatalf("NormalizeFilterLeaf error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if !strings.Contains(diagnostics[0].Message, "direction mismatch") {
		t.Fatalf("diagnostic message = %q, want direction mismatch", diagnostics[0].Message)
	}
}

func TestRelationshipVectorFilterDomainNormalizationKernelReportsUnsupportedMultiHop(t *testing.T) {
	edge := qsbridge.RelationshipJoinPlanEdge{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}
	normalization := FilterDomainNormalizationRequest{
		Operation:        FilterDomainNormalizeGroupedFilter,
		SourceDomain:     "part",
		TargetDomain:     "lineitem",
		Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{edge, edge},
	}
	kernel := RelationshipVectorFilterDomainNormalizationKernel{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7}},
		}},
		Translator: &FixtureFilterDomainRelationshipVectorTranslator{},
	}

	_, diagnostics, err := kernel.NormalizeFilterLeaf(context.Background(), ExecutionRequest{}, normalization, qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"})
	if err != nil {
		t.Fatalf("NormalizeFilterLeaf error = %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if !strings.Contains(diagnostics[0].Message, "multi-hop relationship path is not supported yet") {
		t.Fatalf("diagnostic message = %q, want multi-hop unsupported", diagnostics[0].Message)
	}
}

func TestRelationshipVectorFilterDomainNormalizationKernelAllowsEmptyTargetCandidates(t *testing.T) {
	normalization := FilterDomainNormalizationRequest{
		Operation:    FilterDomainNormalizeGroupedFilter,
		SourceDomain: "part",
		TargetDomain: "lineitem",
		Strategy:     qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{{
			Left: qsbridge.FieldRef{
				Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
				Name:  "l_partkey",
			},
			Right: qsbridge.FieldRef{
				Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
				Name:  "p_partkey",
			},
			ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
		}},
	}
	kernel := RelationshipVectorFilterDomainNormalizationKernel{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"p_brand": {Index: "part", Rownums: []qsbridge.QuantaRownum{7}},
		}},
		Translator: &FixtureFilterDomainRelationshipVectorTranslator{
			Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
				"part.p_brand": {},
			},
		},
	}

	leaf, diagnostics, err := kernel.NormalizeFilterLeaf(context.Background(), ExecutionRequest{}, normalization, qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"})
	if err != nil {
		t.Fatalf("NormalizeFilterLeaf error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if leaf.CandidateSet.Index != "lineitem" {
		t.Fatalf("candidate index = %q, want lineitem", leaf.CandidateSet.Index)
	}
	if len(leaf.CandidateSet.Rownums) != 0 {
		t.Fatalf("candidate rownums = %#v, want empty target set", leaf.CandidateSet.Rownums)
	}
}

func TestKernelFilterDomainNormalizationExecutorNormalizesSourceBranches(t *testing.T) {
	edge := qsbridge.RelationshipJoinPlanEdge{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}
	brand := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"},
	}
	container := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_container"},
	}
	quantity := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"},
	}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			{
				Operation: qsbridge.QuantaFilterIntersect,
				Children:  []qsbridge.QuantaFilterExpression{brand, container},
			},
			quantity,
		},
	}
	translator := &FixtureFilterDomainRelationshipVectorTranslator{
		Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			"part.p_brand": {
				2: {20},
			},
		},
	}
	executor := KernelFilterDomainNormalizationExecutor{
		Kernel: RelationshipVectorFilterDomainNormalizationKernel{
			Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
				"p_brand":     {Index: "part", Rownums: []qsbridge.QuantaRownum{1, 2}},
				"p_container": {Index: "part", Rownums: []qsbridge.QuantaRownum{2, 3}},
			}},
			Translator: translator,
		},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Filter: filter})
	plan := FilterDomainNormalizationPlan{
		Translation: qsbridge.QuantaFilterDomainTranslation{
			Required:      true,
			SourceDomains: []string{"lineitem", "part"},
			TargetDomain:  "lineitem",
			Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
		},
		Requests: []FilterDomainNormalizationRequest{{
			Operation:        FilterDomainNormalizeGroupedFilter,
			SourceDomain:     "part",
			TargetDomain:     "lineitem",
			RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{edge},
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		}},
	}

	rewrite, diagnostics, err := executor.NormalizeFilterDomains(context.Background(), request, plan)
	if err != nil {
		t.Fatalf("NormalizeFilterDomains error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(rewrite.Branches) != 1 {
		t.Fatalf("branches = %d, want 1", len(rewrite.Branches))
	}
	if len(rewrite.Leaves) != 0 {
		t.Fatalf("leaves = %d, want 0", len(rewrite.Leaves))
	}
	branch := rewrite.Branches[0]
	if branch.SourceCount != 1 || len(branch.CandidateSet.Rownums) != 1 || branch.CandidateSet.Rownums[0] != 20 {
		t.Fatalf("branch = %#v, want one translated target row 20 from one source row", branch)
	}
	if len(translator.Calls) != 1 {
		t.Fatalf("translator calls = %d, want 1", len(translator.Calls))
	}
	if len(translator.Calls[0].SourceCandidates.Rownums) != 1 || translator.Calls[0].SourceCandidates.Rownums[0] != 2 {
		t.Fatalf("source candidates = %#v, want [2]", translator.Calls[0].SourceCandidates.Rownums)
	}
	rewritten := rewrite.Apply(filter)
	if !rewritten.Children[0].CandidateSetLeaf() {
		t.Fatalf("rewritten source branch = %#v, want candidate-set leaf", rewritten.Children[0])
	}
	if !rewritten.Children[1].Leaf() {
		t.Fatalf("rewritten target leaf = %#v, want original lineitem leaf", rewritten.Children[1])
	}
}

func TestKernelFilterDomainNormalizationExecutorCombinesSourcePredicatesInsideMixedUnionBranches(t *testing.T) {
	edge := qsbridge.RelationshipJoinPlanEdge{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}
	partBrand := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"},
	}
	partContainer := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_container"},
	}
	partSizeLower := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_size_lower"},
	}
	partSizeUpper := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: "p_size_upper"},
	}
	lineQuantity := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"},
	}
	lineShipmode := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_shipmode"},
	}
	mixedBranch := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			{
				Operation: qsbridge.QuantaFilterIntersect,
				Children:  []qsbridge.QuantaFilterExpression{partBrand, partContainer},
			},
			lineQuantity,
			{
				Operation: qsbridge.QuantaFilterIntersect,
				Children:  []qsbridge.QuantaFilterExpression{partSizeLower, lineShipmode, partSizeUpper},
			},
		},
	}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterUnion,
		Children: []qsbridge.QuantaFilterExpression{
			mixedBranch,
			mixedBranch,
		},
	}
	translator := &FixtureFilterDomainRelationshipVectorTranslator{
		Vectors: map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			"part.p_brand": {
				2: {20},
			},
		},
	}
	executor := KernelFilterDomainNormalizationExecutor{
		Kernel: RelationshipVectorFilterDomainNormalizationKernel{
			Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
				"p_brand":      {Index: "part", Rownums: []qsbridge.QuantaRownum{1, 2, 3}},
				"p_container":  {Index: "part", Rownums: []qsbridge.QuantaRownum{2, 3}},
				"p_size_lower": {Index: "part", Rownums: []qsbridge.QuantaRownum{2, 4}},
				"p_size_upper": {Index: "part", Rownums: []qsbridge.QuantaRownum{2, 5}},
			}},
			Translator: translator,
		},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Filter: filter})
	plan := FilterDomainNormalizationPlan{
		Translation: qsbridge.QuantaFilterDomainTranslation{
			Required:      true,
			SourceDomains: []string{"lineitem", "part"},
			TargetDomain:  "lineitem",
			Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
		},
		Requests: []FilterDomainNormalizationRequest{{
			Operation:        FilterDomainNormalizeGroupedFilter,
			SourceDomain:     "part",
			TargetDomain:     "lineitem",
			RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{edge},
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		}},
	}

	rewrite, diagnostics, err := executor.NormalizeFilterDomains(context.Background(), request, plan)
	if err != nil {
		t.Fatalf("NormalizeFilterDomains error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(rewrite.Branches) != 2 {
		t.Fatalf("branches = %d, want one combined part branch per OR arm", len(rewrite.Branches))
	}
	if len(rewrite.Leaves) != 0 {
		t.Fatalf("leaves = %d, want no separately normalized part leaves", len(rewrite.Leaves))
	}
	if len(translator.Calls) != 1 && len(translator.Calls) != 2 {
		t.Fatalf("translator calls = %d, want one reusable translation or one translation per OR arm", len(translator.Calls))
	}
	for _, call := range translator.Calls {
		if got := call.SourceCandidates.Rownums; len(got) != 1 || got[0] != 2 {
			t.Fatalf("source candidates = %#v, want [2]", got)
		}
	}
	rewritten := rewrite.Apply(filter)
	if rewritten.Operation != qsbridge.QuantaFilterUnion || len(rewritten.Children) != 2 {
		t.Fatalf("rewritten root = %#v, want two-arm union", rewritten)
	}
	for _, child := range rewritten.Children {
		if child.Operation != qsbridge.QuantaFilterIntersect || len(child.Children) != 3 {
			t.Fatalf("rewritten mixed branch = %#v, want candidate plus two lineitem leaves", child)
		}
		if !child.Children[0].CandidateSetLeaf() {
			t.Fatalf("first rewritten child = %#v, want translated candidate-set leaf", child.Children[0])
		}
		if !child.Children[1].Leaf() || child.Children[1].Fragment.Index != "lineitem" {
			t.Fatalf("second rewritten child = %#v, want lineitem leaf", child.Children[1])
		}
		if !child.Children[2].Leaf() || child.Children[2].Fragment.Index != "lineitem" {
			t.Fatalf("third rewritten child = %#v, want lineitem leaf", child.Children[2])
		}
	}
}

func TestDirectBitmapFilterTreeAdapterReportsIncompleteNormalizationRewrite(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterIntersect,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	normalizer := &FixtureFilterDomainNormalizationExecutor{
		Succeed:       true,
		RewriteResult: qsbridge.FilterDomainRewriteResult{TargetDomain: "lineitem"},
	}
	adapter := DirectBitmapFilterTreeAdapter{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called when normalization leaves mixed domains")
			return nil, nil, nil
		}),
		Normalizer: normalizer,
	}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)
	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	for _, want := range []string{
		"filter-domain normalization incomplete after rewrite",
		"remaining_sources=part",
		"target=lineitem",
	} {
		if !strings.Contains(diagnostics[0].Message, want) {
			t.Fatalf("diagnostic message = %q, want %q", diagnostics[0].Message, want)
		}
	}
	if adapted.HasCandidateSet {
		t.Fatalf("incomplete normalization should not produce a candidate set")
	}
}

func TestDirectBitmapRuntimeUsesPrecomputedCandidateSet(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
	}).WithCandidateSet(qsbridge.QuantaCandidateSet{Index: "orders", Rownums: []qsbridge.QuantaRownum{8, 9}})
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called for precomputed candidate set")
			return nil, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(8)}, {Kind: qsbridge.ValueInt, Value: int64(9)}},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(result.RowSet.Rownums) != 2 || result.RowSet.Rownums[0] != 8 || result.RowSet.Rownums[1] != 9 {
		t.Fatalf("rownums = %#v, want [8 9]", result.RowSet.Rownums)
	}
}

func TestDirectBitmapRuntimeUsesFilterAdapterBeforeBorrow(t *testing.T) {
	filterAdapted := false
	queryExecuted := false
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment:  qsbridge.QuantaQueryFragment{Index: "orders", Field: "o_orderkey", Operation: qsbridge.QuantaOperationIntersect, BSIOp: qsbridge.QuantaBSIOpEQ},
		},
	})
	runtime := DirectBitmapRuntime{
		FilterAdapter: DirectBitmapFilterAdapterFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error) {
			filterAdapted = true
			got.Query.Filter = qsbridge.QuantaFilterExpression{}
			got.Query.Fragments = []qsbridge.QuantaQueryFragment{{Index: "orders", Field: "o_orderkey"}}
			return got, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			if !request.Query.Filter.Empty() {
				t.Fatalf("filter was not adapted away: %#v", request.Query.Filter)
			}
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryExecuted = true
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{1}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1)}},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !filterAdapted || !queryExecuted {
		t.Fatalf("filterAdapted/queryExecuted = %t/%t, want true/true", filterAdapted, queryExecuted)
	}
}

func TestDirectBitmapRuntimeUsesRelationshipVectorJoinExecutor(t *testing.T) {
	executor := &recordingRelationshipVectorJoinExecutor{}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "orders", Alias: "o"},
			Name:  "o_orderkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_orderkey",
		},
		Kind:     qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
		Legal:    true,
	}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			t.Fatalf("session provider should not be called for relationship-vector join")
			return nil, nil, nil
		}),
		RelationshipJoins: executor,
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !executor.called {
		t.Fatalf("relationship executor was not called")
	}
	if result.Statement.Status != "relationship executor called" {
		t.Fatalf("status = %q, want relationship executor called", result.Statement.Status)
	}
}

type recordingRelationshipVectorJoinExecutor struct {
	called bool
}

func (e *recordingRelationshipVectorJoinExecutor) ExecuteRelationshipVectorJoin(_ context.Context, _ ExecutionRequest, request RelationshipVectorJoinRequest) (ExecutionResult, error) {
	e.called = true
	if request.EdgeCount() != 1 {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"relationship request edge count mismatch",
			)},
		}, nil
	}
	return ExecutionResult{
		Statement: qsbridge.StatementResult{Status: "relationship executor called"},
	}, nil
}

func TestDirectBitmapRuntimeMaterializesProjectionFields(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "orders",
			Field:   "o_orderkey",
			Type:    qsbridge.DataTypeInt,
			Visible: true,
		}},
	})
	materialized := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{
						Success: true,
						Count:   2,
						Rownums: []qsbridge.QuantaRownum{8, 9},
					}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialized = true
			if request.Index != "orders" {
				t.Fatalf("materialization index = %q, want orders", request.Index)
			}
			if request.CandidateCount() != 2 {
				t.Fatalf("candidate count = %d, want 2", request.CandidateCount())
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(8)}, {Kind: qsbridge.ValueInt, Value: int64(9)}},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !materialized {
		t.Fatalf("materializer was not called")
	}
	if result.RowSet.ProjectionCount() != 1 {
		t.Fatalf("projection count = %d, want 1", result.RowSet.ProjectionCount())
	}
}

func TestDirectBitmapRuntimeExecutesInsertMutation(t *testing.T) {
	released := false
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Mutation = qsbridge.MutationShape{
		Kind:   qsbridge.MutationInsert,
		Target: qsbridge.TableInstance{Schema: "quanta", Table: "customers_qa"},
		Columns: []qsbridge.FieldRef{
			{Name: "cust_id"},
		},
		Rows: []qsbridge.MutationRow{{
			Values: []qsbridge.Expr{qsbridge.Literal(qsbridge.ValueString, "9001")},
		}},
	}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				InsertFunc: func(ctx context.Context, got ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
					if got.Mutation.Kind != qsbridge.MutationInsert {
						t.Fatalf("mutation = %q, want insert", got.Mutation.Kind)
					}
					return qsbridge.StatementResult{AffectedRows: 1, LastInsertID: 9001}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet {
					released = true
					return nil
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !released {
		t.Fatalf("session was not released")
	}
	if result.Statement.AffectedRows != 1 || result.Statement.LastInsertID != 9001 {
		t.Fatalf("statement = %#v, want affected=1 lastInsertID=9001", result.Statement)
	}
}

func TestDirectBitmapRuntimeSynthesizesCountAggregateRows(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "part",
			Field:     "p_partkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
		}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "part_count",
		Type:     qsbridge.DataTypeInt,
	}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 2000}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != int64(2000) {
		t.Fatalf("chunk rows = %#v, want one count cell", chunk.Rows)
	}
}

func TestDirectBitmapRuntimeEvaluatesGlobalAggregateRatioProjection(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem"}
	numerator := qsbridge.FieldRef{Table: table, Name: "promo_revenue", Type: qsbridge.DataTypeFloat}
	denominator := qsbridge.FieldRef{Table: table, Name: "total_revenue", Type: qsbridge.DataTypeFloat}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_shipdate",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpRange,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitem", Field: "promo_revenue", Type: qsbridge.DataTypeFloat},
			{Index: "lineitem", Field: "total_revenue", Type: qsbridge.DataTypeFloat},
		},
	})
	request.SourceIndexes = []string{"lineitem"}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "sum", Input: qsbridge.Field(numerator), Alias: "promo_revenue", Type: qsbridge.DataTypeFloat},
		{Function: "sum", Input: qsbridge.Field(denominator), Alias: "total_revenue", Type: qsbridge.DataTypeFloat},
	}
	request.Projection = []qsbridge.ProjectionColumn{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpDivide,
			qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Literal(qsbridge.ValueFloat, float64(100)), qsbridge.AggregateRef("promo_revenue", 0)),
			qsbridge.AggregateRef("total_revenue", 1),
		),
		Alias: "promo_revenue",
		Type:  qsbridge.DataTypeFloat,
	}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 2, Rownums: []qsbridge.QuantaRownum{1, 2}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   "lineitem",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{Field: request.ProjectionFields[0], Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: float64(5)}, {Kind: qsbridge.ValueFloat, Value: float64(7)}}},
					{Field: request.ProjectionFields[1], Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: float64(50)}, {Kind: qsbridge.ValueFloat, Value: float64(70)}}},
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(result.RowSet.ProjectionVectors) != 1 || result.RowSet.ProjectionVectors[0].Field.Field != "promo_revenue" {
		t.Fatalf("projection vectors = %#v, want promo_revenue", result.RowSet.ProjectionVectors)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != float64(10) {
		t.Fatalf("rows = %#v, want one ratio cell 10", chunk.Rows)
	}
}

func TestDirectBitmapRuntimeMaterializesGroupedCountAggregate(t *testing.T) {
	table := qsbridge.TableInstance{ID: "orders", Table: "orders"}
	priority := qsbridge.FieldRef{Table: table, Name: "o_orderpriority", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderdate",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderpriority", Type: qsbridge.DataTypeString}},
	})
	request.SourceIndexes = []string{"orders"}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(priority)}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "order_count",
		Type:     qsbridge.DataTypeInt,
	}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(priority), Direction: qsbridge.SortAscending}}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(priority), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	materialized := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 5, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialized = true
			if len(request.Rownums) != 5 {
				t.Fatalf("materialization rownums = %#v, want 5 candidates", request.Rownums)
			}
			if len(request.ProjectionFields) != 1 || request.ProjectionFields[0].Field != "o_orderpriority" {
				t.Fatalf("projection fields = %#v, want o_orderpriority", request.ProjectionFields)
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   "orders",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "1-URGENT"},
						{Kind: qsbridge.ValueString, Value: "2-HIGH"},
						{Kind: qsbridge.ValueString, Value: "1-URGENT"},
						{Kind: qsbridge.ValueString, Value: "3-MEDIUM"},
						{Kind: qsbridge.ValueString, Value: "2-HIGH"},
					},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !materialized {
		t.Fatalf("grouped count aggregate used bitmap count fast path; want materialization")
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	want := [][]any{{"1-URGENT", int64(2)}, {"2-HIGH", int64(2)}, {"3-MEDIUM", int64(1)}}
	if len(chunk.Rows) != len(want) {
		t.Fatalf("rows = %#v, want %d grouped rows", chunk.Rows, len(want))
	}
	for i := range want {
		if chunk.Rows[i][0].Value != want[i][0] || chunk.Rows[i][1].Value != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, chunk.Rows[i], want[i])
		}
	}
}

func TestDirectBitmapRuntimeUsesYearBucketRangeCountsForGroupedCount(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem"}
	shipdate := qsbridge.FieldRef{
		Table:        table,
		Name:         "l_shipdate",
		PhysicalName: "l_shipdate",
		Type:         qsbridge.DataTypeTime,
		Encoding:     qsbridge.NewTimeBSIProfile(qsbridge.TimeGranularityMillisecond),
	}
	lower, ok := directBitmapEncodeTimeValue(shipdate.Encoding.Granularity, time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("failed to encode lower bound")
	}
	upper, ok := directBitmapEncodeTimeValue(shipdate.Encoding.Granularity, time.Date(1995, 1, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("failed to encode upper bound")
	}
	groupExpr := qsbridge.Call("year", qsbridge.Field(shipdate))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{
			{Index: "lineitem", Field: "l_shipdate", Operation: qsbridge.QuantaOperationIntersect, BSIOp: qsbridge.QuantaBSIOpGE, Value: big.NewInt(lower)},
			{Index: "lineitem", Field: "l_shipdate", Operation: qsbridge.QuantaOperationIntersect, BSIOp: qsbridge.QuantaBSIOpLT, Value: big.NewInt(upper)},
		},
	})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{groupExpr}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "line_count",
		Type:     qsbridge.DataTypeInt,
	}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: groupExpr, Direction: qsbridge.SortAscending}}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: groupExpr, Alias: "l_year", Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.AggregateRef("line_count", 0), Alias: "line_count", Type: qsbridge.DataTypeInt},
	}
	testRownums := func(count uint64) []qsbridge.QuantaRownum {
		rownums := make([]qsbridge.QuantaRownum, int(count))
		for i := range rownums {
			rownums[i] = qsbridge.QuantaRownum(i + 1)
		}
		return rownums
	}
	bucketCounts := map[int]uint64{1992: 2, 1993: 3, 1994: 1}
	queryCount := 0
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryCount++
					for _, fragment := range request.Query.Fragments {
						if fragment.Index != "lineitem" || fragment.Field != "l_shipdate" || fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil {
							continue
						}
						begin, ok := directBitmapDecodeTimeValue(shipdate.Encoding.Granularity, fragment.Begin.Int64())
						if !ok {
							t.Fatalf("could not decode bucket begin %v", fragment.Begin)
						}
						count := bucketCounts[begin.Year()]
						return BitmapQueryResult{Success: true, Count: count, Rownums: testRownums(count)}, nil, nil
					}
					return BitmapQueryResult{Success: true, Count: 6, Rownums: testRownums(6)}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("year bucket grouped count should use BSI range counts instead of materialization")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if queryCount != 4 {
		t.Fatalf("bitmap queries = %d, want initial plus 3 year buckets", queryCount)
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_mode", "timestamp_bsi_range")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_bound_mode", "predicate_bounds")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_queries", "3")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	want := [][]any{{int64(1992), int64(2)}, {int64(1993), int64(3)}, {int64(1994), int64(1)}}
	if len(chunk.Rows) != len(want) {
		t.Fatalf("rows = %#v, want %d grouped rows", chunk.Rows, len(want))
	}
	for i := range want {
		if chunk.Rows[i][0].Value != want[i][0] || chunk.Rows[i][1].Value != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, chunk.Rows[i], want[i])
		}
	}
}

func TestDirectBitmapRuntimeUsesObservedYearBucketBounds(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem"}
	shipdate := qsbridge.FieldRef{
		Table:        table,
		Name:         "l_shipdate",
		PhysicalName: "l_shipdate",
		Type:         qsbridge.DataTypeTime,
		Encoding:     qsbridge.NewTimeBSIProfile(qsbridge.TimeGranularityMillisecond),
	}
	lower, ok := directBitmapEncodeTimeValue(shipdate.Encoding.Granularity, time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("failed to encode lower bound")
	}
	groupExpr := qsbridge.Call("year", qsbridge.Field(shipdate))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{
			{Index: "lineitem", Field: "l_shipdate", Operation: qsbridge.QuantaOperationIntersect, BSIOp: qsbridge.QuantaBSIOpGE, Value: big.NewInt(lower)},
		},
	})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{groupExpr}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "line_count",
		Type:     qsbridge.DataTypeInt,
	}}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: groupExpr, Alias: "l_year", Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.AggregateRef("line_count", 0), Alias: "line_count", Type: qsbridge.DataTypeInt},
	}
	queryCount := 0
	runtime := DirectBitmapRuntime{
		Sessions: yearBucketBoundsTestProvider{
			minYear: 1990,
			maxYear: 1994,
			queryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
				queryCount++
				for _, fragment := range request.Query.Fragments {
					if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil {
						continue
					}
					begin, ok := directBitmapDecodeTimeValue(shipdate.Encoding.Granularity, fragment.Begin.Int64())
					if !ok {
						t.Fatalf("could not decode bucket begin %v", fragment.Begin)
					}
					if begin.Year() < 1992 || begin.Year() > 1994 {
						t.Fatalf("unexpected year bucket %d from observed bounds", begin.Year())
					}
					return BitmapQueryResult{Success: true, Count: 1, Rownums: []qsbridge.QuantaRownum{1}}, nil, nil
				}
				return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
			},
		},
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("year bucket grouped count should use observed BSI shard bounds instead of materialization")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if queryCount != 4 {
		t.Fatalf("bitmap queries = %d, want initial plus 3 observed year buckets", queryCount)
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_bound_mode", "observed_shards")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_queries", "3")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "year_bucket_range", "1992-1994")
}

func TestDirectBitmapRuntimeAppliesSingleTableMembershipBeforeCountAggregate(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	customers := qsbridge.TableInstance{Table: "customers", Alias: "c"}
	orderCustomerID := qsbridge.FieldRef{Table: orders, Name: "customer_id", Type: qsbridge.DataTypeInt}
	customerID := qsbridge.FieldRef{Table: customers, Name: "cust_id", Type: qsbridge.DataTypeInt}

	for _, tc := range []struct {
		name string
		kind qsbridge.MembershipKind
		want int64
	}{
		{name: "semi", kind: qsbridge.MembershipSemi, want: 2},
		{name: "anti", kind: qsbridge.MembershipAnti, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
				Fragments: []qsbridge.QuantaQueryFragment{{
					Index:     "orders",
					Field:     "order_id",
					Operation: qsbridge.QuantaOperationIntersect,
					BSIOp:     qsbridge.QuantaBSIOpGE,
				}},
			})
			request.Memberships = []qsbridge.MembershipEdge{{
				Left:  orderCustomerID,
				Right: customerID,
				Kind:  tc.kind,
				Legal: true,
			}}
			request.SQLAggregates = []qsbridge.Aggregate{{
				Function: "count",
				Alias:    "order_count",
				Type:     qsbridge.DataTypeInt,
			}}

			runtime := DirectBitmapRuntime{
				Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
					return DirectSessionHandleFunc{
						QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
							index, _ := request.RootIndex()
							switch index {
							case "orders":
								return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
							case "customers":
								return BitmapQueryResult{Success: true, Count: 2, Rownums: []qsbridge.QuantaRownum{101, 102}}, nil, nil
							default:
								t.Fatalf("unexpected query index %q", index)
								return BitmapQueryResult{}, nil, nil
							}
						},
						ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
					}, nil, nil
				}),
				Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
					values := []qsbridge.ResultCell(nil)
					switch request.Index {
					case "orders":
						values = []qsbridge.ResultCell{
							{Kind: qsbridge.ValueInt, Value: int64(10)},
							{Kind: qsbridge.ValueInt, Value: int64(20)},
							{Kind: qsbridge.ValueInt, Value: int64(30)},
						}
					case "customers":
						values = []qsbridge.ResultCell{
							{Kind: qsbridge.ValueInt, Value: int64(20)},
							{Kind: qsbridge.ValueInt, Value: int64(30)},
						}
					default:
						t.Fatalf("unexpected materialization index %q", request.Index)
					}
					return qsbridge.QuantaProjectedRowSet{
						Index:   request.Index,
						Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Field:  request.ProjectionFields[0],
							Values: values,
						}},
					}, nil, nil
				}),
			}

			result, err := runtime.ExecuteDirect(context.Background(), request)
			if err != nil {
				t.Fatalf("execute direct: %v", err)
			}
			if result.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
			}
			chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
			if diagnostics.BlocksNative() {
				t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
			}
			if got := chunk.Rows[0][0].Value; got != tc.want {
				t.Fatalf("count value = %#v, want %d", got, tc.want)
			}
		})
	}
}

func TestDirectBitmapRuntimeAppliesCorrelatedSiblingMembershipBeforeCountAggregate(t *testing.T) {
	leftTable := qsbridge.TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l1"}
	rightTable := qsbridge.TableInstance{ID: "lineitem_membership", Table: "lineitem", Alias: "l2"}
	leftOrderKey := qsbridge.FieldRef{Table: leftTable, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt}
	rightOrderKey := qsbridge.FieldRef{Table: rightTable, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt}
	leftSuppKey := qsbridge.FieldRef{Table: leftTable, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt}
	rightSuppKey := qsbridge.FieldRef{Table: rightTable, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  leftOrderKey,
		Right: rightOrderKey,
		Kind:  qsbridge.MembershipSemi,
		Predicates: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(rightSuppKey), qsbridge.Field(leftSuppKey)),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		}},
		Legal: true,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "same_order_other_supplier_count",
		Type:     qsbridge.DataTypeInt,
	}}

	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 4, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_orderkey":
						if rownum <= 2 {
							values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(10)})
						} else {
							values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(20)})
						}
					case "l_suppkey":
						switch rownum {
						case 1, 2:
							values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(1)})
						case 3:
							values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(2)})
						case 4:
							values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(3)})
						default:
							t.Fatalf("unexpected rownum %d", rownum)
						}
					default:
						t.Fatalf("unexpected projection field %#v", field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
					Field:  field,
					Values: values,
				})
			}
			return rowSet, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != int64(2) {
		t.Fatalf("count value = %#v, want 2", got)
	}
}

func TestDirectBitmapRuntimeMaterializesResidualPredicatesBeforeCount(t *testing.T) {
	table := qsbridge.TableInstance{ID: "part", Table: "part"}
	partType := qsbridge.FieldRef{Table: table, Name: "p_type", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "part",
			Field:     "p_size",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "part",
			Field: "p_type",
			Type:  qsbridge.DataTypeString,
		}},
	})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpNotLike, qsbridge.Field(partType), qsbridge.Literal(qsbridge.ValueString, "MEDIUM POLISHED%")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Alias:    "part_count",
		Type:     qsbridge.DataTypeInt,
	}}
	materialized := false
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialized = true
			return qsbridge.QuantaProjectedRowSet{
				Index:   "part",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "SMALL POLISHED COPPER"},
						{Kind: qsbridge.ValueString, Value: "MEDIUM POLISHED BRASS"},
						{Kind: qsbridge.ValueString, Value: "LARGE BURNISHED TIN"},
					},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !materialized {
		t.Fatalf("materializer was not called")
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != int64(2) {
		t.Fatalf("count = %#v, want 2", got)
	}
}

func TestDirectBitmapEvaluateResidualRegexpPredicate(t *testing.T) {
	table := qsbridge.TableInstance{ID: "region", Table: "region"}
	field := qsbridge.FieldRef{Table: table, Name: "r_name", PhysicalName: "r_name", Type: qsbridge.DataTypeString}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "region",
		Rownums: []qsbridge.QuantaRownum{1, 2},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field:  qsbridge.QuantaProjectionField{Index: "region", Field: "r_name", Type: qsbridge.DataTypeString},
			Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "AFRICA"}, {Kind: qsbridge.ValueString, Value: "EUROPE"}},
		}},
	}

	expr := qsbridge.Binary(qsbridge.BinaryOpRegexp, qsbridge.Field(field), qsbridge.Literal(qsbridge.ValueString, "^A"))
	first, diagnostics := directBitmapEvaluateResidualBoolExpr(expr, rowSet, 0)
	if diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v", diagnostics)
	}
	second, diagnostics := directBitmapEvaluateResidualBoolExpr(expr, rowSet, 1)
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v", diagnostics)
	}
	if !first || second {
		t.Fatalf("regexp matches = %t/%t, want true/false", first, second)
	}
}

func TestDirectBitmapRuntimeRejectsUnsupportedAggregates(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "part"}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Type: qsbridge.DataTypeFloat}}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 2000}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected unsupported aggregate diagnostics")
	}
}

func TestDirectBitmapRuntimeMaterializesNumericAggregates(t *testing.T) {
	table := qsbridge.TableInstance{ID: "part", Table: "part"}
	field := qsbridge.FieldRef{Table: table, Name: "p_size", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "part"}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "part",
			Field: "p_size",
			Type:  qsbridge.DataTypeInt,
		}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "sum", Input: qsbridge.Field(field), Alias: "sum_size", Type: qsbridge.DataTypeFloat},
		{Function: "min", Input: qsbridge.Field(field), Alias: "min_size", Type: qsbridge.DataTypeFloat},
		{Function: "max", Input: qsbridge.Field(field), Alias: "max_size", Type: qsbridge.DataTypeFloat},
		{Function: "avg", Input: qsbridge.Field(field), Alias: "avg_size", Type: qsbridge.DataTypeFloat},
	}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   "part",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(3)}, {Kind: qsbridge.ValueInt, Value: int64(9)}, {Kind: qsbridge.ValueInt, Value: int64(14)}},
				}},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	got := chunk.Rows[0]
	want := []float64{26, 3, 14, 26.0 / 3.0}
	for i, wantValue := range want {
		if gotValue, ok := got[i].Value.(float64); !ok || gotValue != wantValue {
			t.Fatalf("row[%d] = %#v, want %v", i, got[i].Value, wantValue)
		}
	}
}

func TestDirectBitmapRuntimeMaterializesGroupedArithmeticAggregate(t *testing.T) {
	table := qsbridge.TableInstance{ID: "partsupp", Table: "partsupp"}
	partKey := qsbridge.FieldRef{Table: table, Name: "ps_partkey", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: table, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	availQty := qsbridge.FieldRef{Table: table, Name: "ps_availqty", Type: qsbridge.DataTypeInt}
	aggregate := qsbridge.Aggregate{
		Function: "sum",
		Input:    qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(supplyCost), qsbridge.Field(availQty)),
		Alias:    "part_value",
		Type:     qsbridge.DataTypeFloat,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "partsupp", Field: "ps_partkey", Type: qsbridge.DataTypeInt},
			{Index: "partsupp", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
			{Index: "partsupp", Field: "ps_availqty", Type: qsbridge.DataTypeInt},
		},
	})
	request.SourceIndexes = []string{"partsupp"}
	request.SQLAggregates = []qsbridge.Aggregate{aggregate}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(partKey)}
	request.Having = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.AggregateRef("part_value", 0), qsbridge.Literal(qsbridge.ValueFloat, float64(1200))),
	}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("part_value", 0), Direction: qsbridge.SortDescending}}
	request.Result = qsbridge.ResultShape{Limit: 2}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(partKey), Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.AggregateRef("part_value", 0), Alias: "part_value", Type: qsbridge.DataTypeFloat},
	}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 6, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5, 6}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   "partsupp",
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{Field: request.ProjectionFields[0], Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueInt, Value: int64(1)},
						{Kind: qsbridge.ValueInt, Value: int64(1)},
						{Kind: qsbridge.ValueInt, Value: int64(2)},
						{Kind: qsbridge.ValueInt, Value: int64(3)},
						{Kind: qsbridge.ValueInt, Value: int64(4)},
						{Kind: qsbridge.ValueInt, Value: int64(5)},
					}},
					{Field: request.ProjectionFields[1], Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueFloat, Value: float64(10)},
						{Kind: qsbridge.ValueFloat, Value: float64(10.5)},
						{Kind: qsbridge.ValueFloat, Value: float64(20)},
						{Kind: qsbridge.ValueFloat, Value: float64(1)},
						{Kind: qsbridge.ValueFloat, Value: float64(15)},
						{Kind: qsbridge.ValueFloat, Value: float64(14)},
					}},
					{Field: request.ProjectionFields[2], Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueInt, Value: int64(100)},
						{Kind: qsbridge.ValueInt, Value: int64(100)},
						{Kind: qsbridge.ValueInt, Value: int64(100)},
						{Kind: qsbridge.ValueInt, Value: int64(100)},
						{Kind: qsbridge.ValueInt, Value: int64(100)},
						{Kind: qsbridge.ValueInt, Value: int64(100)},
					}},
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != int64(1) || chunk.Rows[0][1].Value != float64(2050) {
		t.Fatalf("first row = %#v, want part 1 value 2050", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != int64(2) || chunk.Rows[1][1].Value != float64(2000) {
		t.Fatalf("second row = %#v, want part 2 value 2000", chunk.Rows[1])
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "candidate_rows", "6")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_count", "1")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_computed_count", "0")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_shapes", "field:partsupp.ps_partkey")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_fields", "partsupp.ps_partkey")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "groups", "5")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "post_having_groups", "4")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "sort_input_groups", "4")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "limit", "2")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "final_rows", "2")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "topn_candidate", "true")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "order_strategy", "heap_topn")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_strategy", "index_replay")
	assertExecutionProbeName(t, result.Probes, "direct_bitmap", "phase_bitmap_query_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_materialization_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_residual_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_group_values_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_aggregate_inputs_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_group_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_having_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_order_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_output_elapsed")
	assertExecutionProbeName(t, result.Probes, "grouped_aggregate", "phase_limit_elapsed")
}

func TestDirectBitmapRuntimeUsesBitmapGroupCountReader(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem"}
	returnFlag := qsbridge.FieldRef{Table: table, Name: "l_returnflag", PhysicalName: "l_returnflag", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum}
	lineStatus := qsbridge.FieldRef{Table: table, Name: "l_linestatus", PhysicalName: "l_linestatus", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum}
	reader := &fakeBitmapGroupCountReader{
		Result: BitmapGroupCountReadResult{
			Mode:          "test_bitmap_group_count",
			CandidateRows: 6,
			FieldCount:    2,
			ValueCount:    4,
			Groups: []BitmapGroupCountReadGroup{
				{Key: "A\x00F", Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "A"}, {Kind: qsbridge.ValueString, Value: "F"}}, Count: 3},
				{Key: "N\x00O", Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "N"}, {Kind: qsbridge.ValueString, Value: "O"}}, Count: 2},
			},
		},
	}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 6, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5, 6}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		BitmapGroupCounts: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("bitmap group count path should not materialize %d rows", len(request.Rownums))
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(returnFlag), qsbridge.Field(lineStatus)}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "count_order", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if reader.Calls != 1 {
		t.Fatalf("bitmap group count calls = %d, want 1", reader.Calls)
	}
	if len(reader.Last.CandidateRows) != 6 || len(reader.Last.Fields) != 2 {
		t.Fatalf("read request = %#v, want 6 rows and 2 fields", reader.Last)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "A" || chunk.Rows[0][1].Value != "F" || chunk.Rows[0][2].Value != int64(3) {
		t.Fatalf("first row = %#v, want A/F/3", chunk.Rows[0])
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_strategy", "bitmap_group_count")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "bitmap_group_count_mode", "test_bitmap_group_count")
}

func TestDirectBitmapBitmapGroupCountRejectsComputedGroupExpressions(t *testing.T) {
	table := qsbridge.TableInstance{ID: "customer", Table: "customer"}
	segment := qsbridge.FieldRef{Table: table, Name: "c_mktsegment", PhysicalName: "c_mktsegment", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum}
	prefixExpr := qsbridge.FunctionCall(
		qsbridge.FunctionDefinition{Name: "substr", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeString},
		qsbridge.Field(segment),
		qsbridge.Literal(qsbridge.ValueInt, int64(1)),
		qsbridge.Literal(qsbridge.ValueInt, int64(1)),
	)
	groupExpressions := []directBitmapGroupExpression{{
		Expr:  prefixExpr,
		Field: segment,
	}}
	if directBitmapBitmapGroupCountExpressions(groupExpressions) {
		t.Fatalf("bitmap group count accepted computed group expression")
	}
}

func TestDirectBitmapRuntimeUsesBitmapGroupAggregateReader(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem"}
	returnFlag := qsbridge.FieldRef{Table: table, Name: "l_returnflag", PhysicalName: "l_returnflag", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum}
	quantity := qsbridge.FieldRef{Table: table, Name: "l_quantity", PhysicalName: "l_quantity", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	reader := &fakeBitmapGroupAggregateReader{
		Result: BitmapGroupAggregateReadResult{
			Mode:           "test_bitmap_group_aggregate",
			CandidateRows:  5,
			FieldCount:     1,
			ValueCount:     2,
			AggregateCount: 2,
			Groups: []BitmapGroupAggregateReadGroup{
				{Key: "A", Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "A"}}, Aggs: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(3)}, {Kind: qsbridge.ValueFloat, Value: float64(70)}}},
				{Key: "R", Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "R"}}, Aggs: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(2)}, {Kind: qsbridge.ValueFloat, Value: float64(80)}}},
			},
		},
	}
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 5, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		BitmapGroupAggregates: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("bitmap group aggregate path should not materialize %d rows", len(request.Rownums))
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(returnFlag)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Alias: "count_order", Type: qsbridge.DataTypeInt},
		{Function: "sum", Input: qsbridge.Field(quantity), Alias: "sum_qty", Type: qsbridge.DataTypeFloat},
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if reader.Calls != 1 {
		t.Fatalf("bitmap group aggregate calls = %d, want 1", reader.Calls)
	}
	if len(reader.Last.CandidateRows) != 5 || len(reader.Last.GroupFields) != 1 || len(reader.Last.Aggregates) != 2 {
		t.Fatalf("read request = %#v, want 5 rows, 1 group field, 2 aggregates", reader.Last)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "A" || chunk.Rows[0][1].Value != int64(3) || chunk.Rows[0][2].Value != float64(70) {
		t.Fatalf("first row = %#v, want A/3/70", chunk.Rows[0])
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_strategy", "bitmap_group_aggregate")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "bitmap_group_aggregate_mode", "test_bitmap_group_aggregate")
}

func TestDirectBitmapGroupedAggregateStreamingCandidateRequiresLargeSimpleShape(t *testing.T) {
	table := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	orderKey := qsbridge.FieldRef{Table: table, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	extendedPrice := qsbridge.FieldRef{Table: table, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	request := ExecutionRequest{
		GroupBy: []qsbridge.Expr{qsbridge.Field(orderKey)},
		SQLAggregates: []qsbridge.Aggregate{{
			Function: "sum",
			Input:    qsbridge.Field(extendedPrice),
			Alias:    "revenue",
			Type:     qsbridge.DataTypeFloat,
		}},
	}
	largeGroupValues := [][]qsbridge.ResultCell{make([]qsbridge.ResultCell, directBitmapStreamingGroupedAggregateMinRows)}
	largeAggregateInputs := [][]qsbridge.ResultCell{make([]qsbridge.ResultCell, directBitmapStreamingGroupedAggregateMinRows)}
	if !directBitmapStreamingGroupedAggregateCandidate(request, largeGroupValues, largeAggregateInputs) {
		t.Fatalf("streaming candidate = false, want true for large simple grouped sum")
	}
	smallGroupValues := [][]qsbridge.ResultCell{make([]qsbridge.ResultCell, directBitmapStreamingGroupedAggregateMinRows-1)}
	if directBitmapStreamingGroupedAggregateCandidate(request, smallGroupValues, largeAggregateInputs) {
		t.Fatalf("streaming candidate = true, want false below row threshold")
	}
	distinct := request
	distinct.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Mode:     qsbridge.AggregateDistinct,
		Input:    qsbridge.Field(extendedPrice),
		Alias:    "distinct_revenue",
		Type:     qsbridge.DataTypeInt,
	}}
	if directBitmapStreamingGroupedAggregateCandidate(distinct, largeGroupValues, largeAggregateInputs) {
		t.Fatalf("streaming candidate = true, want false for distinct aggregate")
	}
}

func assertExecutionProbe(t *testing.T, probes []ExecutionProbe, section string, name string, value string) {
	t.Helper()
	for _, probe := range probes {
		if probe.Section == section && probe.Name == name && probe.Value == value {
			return
		}
	}
	t.Fatalf("probe %s/%s=%s not found in %#v", section, name, value, probes)
}

func assertExecutionProbeName(t *testing.T, probes []ExecutionProbe, section string, name string) {
	t.Helper()
	for _, probe := range probes {
		if probe.Section == section && probe.Name == name && probe.Value != "" {
			return
		}
	}
	t.Fatalf("probe %s/%s not found in %#v", section, name, probes)
}

func assertExecutionTimingName(t *testing.T, snapshot ExecutionInstrumentationSnapshot, section string, name string) {
	t.Helper()
	for _, timing := range snapshot.Timings {
		if timing.Section == section && timing.Name == name && timing.Duration > 0 {
			return
		}
	}
	t.Fatalf("timing %s/%s not found in %#v", section, name, snapshot.Timings)
}

func assertExecutionCounter(t *testing.T, snapshot ExecutionInstrumentationSnapshot, section string, name string, value uint64) {
	t.Helper()
	for _, counter := range snapshot.Counters {
		if counter.Section == section && counter.Name == name && counter.Value == value {
			return
		}
	}
	t.Fatalf("counter %s/%s=%d not found in %#v", section, name, value, snapshot.Counters)
}

func TestDirectBitmapNumericCellValueParsesRationalStrings(t *testing.T) {
	value, ok := directBitmapNumericCellValue(qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "123/100"})
	if !ok || value != 1.23 {
		t.Fatalf("numeric value = %v/%t, want 1.23/true", value, ok)
	}
}

func TestDirectBitmapEvaluateMaterializedSubstringCall(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "customer",
		Rownums: []qsbridge.QuantaRownum{1},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field:  qsbridge.QuantaProjectionField{Index: "customer", Field: "c_phone", Visible: true},
			Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "13-750-942-6364"}},
		}},
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(
		qsbridge.Call("substr", qsbridge.Field(qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "customer"}, Name: "c_phone"}), qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Literal(qsbridge.ValueInt, int64(2))),
		rowSet,
		0,
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if cell.Kind != qsbridge.ValueString || cell.Value != "13" {
		t.Fatalf("cell = %#v, want string 13", cell)
	}
}

func TestDirectBitmapEvaluateMaterializedMySQLTrimLocateAndComparisonCalls(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{Rownums: []qsbridge.QuantaRownum{1}}
	for _, test := range []struct {
		name string
		call qsbridge.CallExpr
		want qsbridge.ResultCell
	}{
		{
			name: "trim leading custom string",
			call: qsbridge.Call("trim",
				qsbridge.Literal(qsbridge.ValueString, "leading"),
				qsbridge.Literal(qsbridge.ValueString, "x"),
				qsbridge.Literal(qsbridge.ValueString, "xxxstream"),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "stream"},
		},
		{
			name: "locate",
			call: qsbridge.Call("locate",
				qsbridge.Literal(qsbridge.ValueString, "stream"),
				qsbridge.Literal(qsbridge.ValueString, "quantastream"),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(7)},
		},
		{
			name: "instr",
			call: qsbridge.Call("instr",
				qsbridge.Literal(qsbridge.ValueString, "quantastream"),
				qsbridge.Literal(qsbridge.ValueString, "stream"),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(7)},
		},
		{
			name: "greatest",
			call: qsbridge.Call("greatest",
				qsbridge.Literal(qsbridge.ValueInt, int64(3)),
				qsbridge.Literal(qsbridge.ValueInt, int64(7)),
				qsbridge.Literal(qsbridge.ValueInt, int64(4)),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(7)},
		},
		{
			name: "least",
			call: qsbridge.Call("least",
				qsbridge.Literal(qsbridge.ValueInt, int64(3)),
				qsbridge.Literal(qsbridge.ValueInt, int64(7)),
				qsbridge.Literal(qsbridge.ValueInt, int64(4)),
			),
			want: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(3)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cell, diagnostics := directBitmapEvaluateMaterializedCallExpr(test.call, rowSet, 0)
			if diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", diagnostics)
			}
			if cell.Kind != test.want.Kind || cell.Value != test.want.Value {
				t.Fatalf("cell = %#v, want %#v", cell, test.want)
			}
		})
	}
}

func TestDirectBitmapEvaluateMaterializedBinaryExprPropagatesNull(t *testing.T) {
	field := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "lineitem"},
		Name:  "l_discount",
		Type:  qsbridge.DataTypeFloat,
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{1},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{
				Index: "lineitem",
				Field: "l_discount",
				Type:  qsbridge.DataTypeFloat,
			},
			Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueNull, Value: nil}},
		}},
	}
	cell, diagnostics := directBitmapEvaluateMaterializedBinaryExpr(qsbridge.BinaryExpr{
		Left:  qsbridge.Literal(qsbridge.ValueInt, int64(1)),
		Op:    qsbridge.BinaryOpSubtract,
		Right: qsbridge.FieldExpr{Ref: field},
	}, rowSet, 0)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if cell.Kind != qsbridge.ValueNull || cell.Value != nil {
		t.Fatalf("cell = %#v, want NULL", cell)
	}
}

func TestDirectBitmapRuntimeRequiresMaterializerForProjection(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
	})
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{1}}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected missing materializer diagnostics")
	}
}

func TestDirectBitmapRuntimeReportsMissingSessionProvider(t *testing.T) {
	result, err := DirectBitmapRuntime{}.ExecuteDirect(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected missing provider diagnostics")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestDirectBitmapRuntimeReportsNilSession(t *testing.T) {
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return nil, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected nil session diagnostics")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestDirectBitmapRuntimeAppendsQueryAndReleaseDiagnostics(t *testing.T) {
	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true}, qsbridge.DiagnosticSet{
						qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "query diagnostic"),
					}, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet {
					return qsbridge.DiagnosticSet{
						qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "release diagnostic"),
					}
				},
			}, nil, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if len(result.Diagnostics) != 2 {
		t.Fatalf("diagnostic count = %d, want 2", len(result.Diagnostics))
	}
}

func TestDirectBitmapRuntimeLimitsProjectedRowSet(t *testing.T) {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "part",
		Rownums: []qsbridge.QuantaRownum{10, 20, 30},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{Index: "part", Field: "p_size", Type: qsbridge.DataTypeInt, Visible: true},
			Values: []qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(1)},
				{Kind: qsbridge.ValueInt, Value: int64(2)},
				{Kind: qsbridge.ValueInt, Value: int64(3)},
			},
		}},
	}

	limited := directBitmapLimitProjectedRowSet(rowSet, 1, 1, true)
	if got := limited.CandidateCount(); got != 1 {
		t.Fatalf("candidate count = %d, want 1", got)
	}
	if got := limited.Rownums[0]; got != qsbridge.QuantaRownum(20) {
		t.Fatalf("rownum = %d, want 20", got)
	}
	if got := limited.ProjectionVectors[0].Values[0].Value; got != int64(2) {
		t.Fatalf("projected value = %v, want 2", got)
	}
	if got := rowSet.CandidateCount(); got != 3 {
		t.Fatalf("source row set was mutated; candidate count = %d, want 3", got)
	}

	empty := directBitmapLimitProjectedRowSet(rowSet, 0, 0, true)
	if got := empty.CandidateCount(); got != 0 {
		t.Fatalf("limit zero candidate count = %d, want 0", got)
	}
	if got := rowSet.CandidateCount(); got != 3 {
		t.Fatalf("source row set was mutated after limit zero; candidate count = %d, want 3", got)
	}
}

func TestDirectBitmapRuntimeOrdersVisibleProjectedRowSetByResultColumns(t *testing.T) {
	table := qsbridge.TableInstance{ID: "part", Table: "part"}
	size := qsbridge.FieldRef{Table: table, Name: "p_size", Type: qsbridge.DataTypeInt}
	typ := qsbridge.FieldRef{Table: table, Name: "p_type", Type: qsbridge.DataTypeString}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "part",
		Rownums: []qsbridge.QuantaRownum{10},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field:  qsbridge.QuantaProjectionField{Index: "part", Field: "p_type", Type: qsbridge.DataTypeString, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "PROMO BURNISHED COPPER"}},
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: "part", Field: "p_size", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(45)}},
			},
		},
	}

	ordered := directBitmapOrderVisibleProjectedRowSet(rowSet, []qsbridge.FieldRef{size, typ})
	if got := ordered.ProjectionVectors[0].Field.Field; got != "p_size" {
		t.Fatalf("first vector field = %q, want p_size", got)
	}
	if got := ordered.ProjectionVectors[1].Field.Field; got != "p_type" {
		t.Fatalf("second vector field = %q, want p_type", got)
	}
}

func TestDirectBitmapMaterializedGroupedAggregateSupportsComputedYearGroup(t *testing.T) {
	table := qsbridge.TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderDate := qsbridge.FieldRef{Table: table, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	totalPrice := qsbridge.FieldRef{Table: table, Name: "o_totalprice", Type: qsbridge.DataTypeFloat}
	yearExpr := qsbridge.FunctionCall(
		qsbridge.FunctionDefinition{Name: "year", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeInt},
		qsbridge.Field(orderDate),
	)
	request := ExecutionRequest{
		SourceIndexes: []string{"orders"},
		GroupBy:       []qsbridge.Expr{yearExpr},
		Projection: []qsbridge.ProjectionColumn{
			{Expr: yearExpr, Alias: "o_year", Type: qsbridge.DataTypeInt},
			{Expr: qsbridge.AggregateRef("total_revenue", 0), Alias: "total_revenue", Type: qsbridge.DataTypeFloat},
			{
				Expr: qsbridge.Binary(
					qsbridge.BinaryOpDivide,
					qsbridge.AggregateRef("total_revenue", 0),
					qsbridge.AggregateRef("order_count", 1),
				),
				Alias: "avg_revenue",
				Type:  qsbridge.DataTypeFloat,
			},
		},
		SQLAggregates: []qsbridge.Aggregate{
			{
				Function: "sum",
				Input:    qsbridge.Field(totalPrice),
				Alias:    "total_revenue",
				Type:     qsbridge.DataTypeFloat,
			},
			{
				Function: "count",
				Alias:    "order_count",
				Type:     qsbridge.DataTypeInt,
			},
		},
	}
	materialized := qsbridge.QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueTime, Value: time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC)},
					{Kind: qsbridge.ValueTime, Value: time.Date(1996, 4, 10, 0, 0, 0, 0, time.UTC)},
					{Kind: qsbridge.ValueTime, Value: time.Date(1996, 7, 20, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				Field: qsbridge.QuantaProjectionField{Index: "orders", Role: "o", Field: "o_totalprice", Type: qsbridge.DataTypeFloat},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueFloat, Value: float64(10)},
					{Kind: qsbridge.ValueFloat, Value: float64(20)},
					{Kind: qsbridge.ValueFloat, Value: float64(30)},
				},
			},
		},
	}

	result := directBitmapMaterializedGroupedAggregateResult(request, materialized, ExecutionResult{})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != int64(1995) {
		t.Fatalf("first year = %v, want 1995", got)
	}
	if got := chunk.Rows[0][1].Value; got != float64(10) {
		t.Fatalf("first total = %v, want 10", got)
	}
	if got := chunk.Rows[0][2].Value; got != float64(10) {
		t.Fatalf("first average = %v, want 10", got)
	}
	if got := chunk.Rows[1][0].Value; got != int64(1996) {
		t.Fatalf("second year = %v, want 1996", got)
	}
	if got := chunk.Rows[1][1].Value; got != float64(50) {
		t.Fatalf("second total = %v, want 50", got)
	}
	if got := chunk.Rows[1][2].Value; got != float64(25) {
		t.Fatalf("second average = %v, want 25", got)
	}
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_count", "1")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_computed_count", "1")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_shapes", "call:year(field:o.o_orderdate)")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_expression_fields", "o.o_orderdate")
}

func TestDirectBitmapMaterializedGroupedAggregateUsesDerivedYearProjection(t *testing.T) {
	table := qsbridge.TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderDate := qsbridge.FieldRef{Table: table, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	yearExpr := qsbridge.FunctionCall(
		qsbridge.FunctionDefinition{Name: "year", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeInt},
		qsbridge.Field(orderDate),
	)
	request := ExecutionRequest{
		SourceIndexes: []string{"orders"},
		GroupBy:       []qsbridge.Expr{yearExpr},
		Projection: []qsbridge.ProjectionColumn{
			{Expr: yearExpr, Alias: "o_year", Type: qsbridge.DataTypeInt},
			{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
		},
		SQLAggregates: []qsbridge.Aggregate{{
			Function: "count",
			Alias:    "order_count",
			Type:     qsbridge.DataTypeInt,
		}},
	}
	materialized := qsbridge.QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{Index: "orders", Role: "o", Field: "year_o_orderdate", Type: qsbridge.DataTypeInt},
			Values: []qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(1995)},
				{Kind: qsbridge.ValueInt, Value: int64(1996)},
				{Kind: qsbridge.ValueInt, Value: int64(1996)},
			},
		}},
	}

	result := directBitmapMaterializedGroupedAggregateResult(request, materialized, ExecutionResult{})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != int64(1995) {
		t.Fatalf("first year = %v, want 1995", got)
	}
	if got := chunk.Rows[0][1].Value; got != int64(1) {
		t.Fatalf("first count = %v, want 1", got)
	}
	if got := chunk.Rows[1][0].Value; got != int64(1996) {
		t.Fatalf("second year = %v, want 1996", got)
	}
	if got := chunk.Rows[1][1].Value; got != int64(2) {
		t.Fatalf("second count = %v, want 2", got)
	}
}

func TestDirectBitmapMaterializedAggregateCountFieldIgnoresNullInputs(t *testing.T) {
	table := qsbridge.TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := qsbridge.FieldRef{Table: table, Name: "o_orderkey", Type: qsbridge.DataTypeInt}
	aggregate := qsbridge.Aggregate{
		Function: "count",
		Input:    qsbridge.Field(orderKey),
		Alias:    "order_count",
		Type:     qsbridge.DataTypeInt,
	}
	materialized := qsbridge.QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{Index: "orders", Role: "o", Field: "o_orderkey", Type: qsbridge.DataTypeInt},
			Values: []qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(1001)},
				{Kind: qsbridge.ValueNull, Value: nil},
				{Kind: qsbridge.ValueInt, Value: int64(1003)},
			},
		}},
	}

	cell, diagnostics := directBitmapMaterializedAggregateCell(aggregate, materialized)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if cell.Value != int64(2) {
		t.Fatalf("count = %v, want 2", cell.Value)
	}
}

func TestDirectBitmapMaterializedGroupedAggregateCountFieldIgnoresNullInputs(t *testing.T) {
	customer := qsbridge.TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orders := qsbridge.TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customerKey := qsbridge.FieldRef{Table: customer, Name: "c_custkey", Type: qsbridge.DataTypeInt}
	orderKey := qsbridge.FieldRef{Table: orders, Name: "o_orderkey", Type: qsbridge.DataTypeInt}
	request := ExecutionRequest{
		SourceIndexes: []string{"customer"},
		GroupBy:       []qsbridge.Expr{qsbridge.Field(customerKey)},
		Projection: []qsbridge.ProjectionColumn{
			{Expr: qsbridge.Field(customerKey), Alias: "customer_key", Type: qsbridge.DataTypeInt},
			{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
		},
		SQLAggregates: []qsbridge.Aggregate{{
			Function: "count",
			Input:    qsbridge.Field(orderKey),
			Alias:    "order_count",
			Type:     qsbridge.DataTypeInt,
		}},
	}
	materialized := qsbridge.QuantaProjectedRowSet{
		Index:   "customer",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "customer", Role: "c", Field: "c_custkey", Type: qsbridge.DataTypeInt},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(3)},
					{Kind: qsbridge.ValueInt, Value: int64(6)},
				},
			},
			{
				Field: qsbridge.QuantaProjectionField{Index: "orders", Role: "o", Field: "o_orderkey", Type: qsbridge.DataTypeInt},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1001)},
					{Kind: qsbridge.ValueNull, Value: nil},
					{Kind: qsbridge.ValueNull, Value: nil},
				},
			},
		},
	}

	result := directBitmapMaterializedGroupedAggregateResult(request, materialized, ExecutionResult{})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	wantCounts := []int64{1, 0, 0}
	if len(chunk.Rows) != len(wantCounts) {
		t.Fatalf("rows = %#v, want %d rows", chunk.Rows, len(wantCounts))
	}
	for i, want := range wantCounts {
		if got := chunk.Rows[i][1].Value; got != want {
			t.Fatalf("row %d count = %v, want %d; rows=%#v", i, got, want, chunk.Rows)
		}
	}
}

func TestDirectBitmapMaterializedGroupedAggregateSupportsComputedSubstringGroup(t *testing.T) {
	table := qsbridge.TableInstance{ID: "part", Table: "part", Alias: "p"}
	partType := qsbridge.FieldRef{Table: table, Name: "p_type", Type: qsbridge.DataTypeString}
	prefixExpr := qsbridge.FunctionCall(
		qsbridge.FunctionDefinition{Name: "substr", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeString},
		qsbridge.Field(partType),
		qsbridge.Literal(qsbridge.ValueInt, int64(1)),
		qsbridge.Literal(qsbridge.ValueInt, int64(5)),
	)
	request := ExecutionRequest{
		SourceIndexes: []string{"part"},
		GroupBy:       []qsbridge.Expr{prefixExpr},
		Projection: []qsbridge.ProjectionColumn{
			{Expr: prefixExpr, Alias: "prefix", Type: qsbridge.DataTypeString},
			{Expr: qsbridge.AggregateRef("part_count", 0), Alias: "part_count", Type: qsbridge.DataTypeInt},
		},
		SQLAggregates: []qsbridge.Aggregate{
			{
				Function: "count",
				Alias:    "part_count",
				Type:     qsbridge.DataTypeInt,
			},
		},
	}
	materialized := qsbridge.QuantaProjectedRowSet{
		Index:   "part",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "part", Role: "p", Field: "p_type", Type: qsbridge.DataTypeString},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "ECONOMY ANODIZED"},
					{Kind: qsbridge.ValueString, Value: "ECONOMY BRUSHED"},
					{Kind: qsbridge.ValueString, Value: "PROMO POLISHED"},
				},
			},
		},
	}

	result := directBitmapMaterializedGroupedAggregateResult(request, materialized, ExecutionResult{})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != "ECONO" {
		t.Fatalf("first prefix = %v, want ECONO", got)
	}
	if got := chunk.Rows[0][1].Value; got != int64(2) {
		t.Fatalf("first count = %v, want 2", got)
	}
	if got := chunk.Rows[1][0].Value; got != "PROMO" {
		t.Fatalf("second prefix = %v, want PROMO", got)
	}
	if got := chunk.Rows[1][1].Value; got != int64(1) {
		t.Fatalf("second count = %v, want 1", got)
	}
}
