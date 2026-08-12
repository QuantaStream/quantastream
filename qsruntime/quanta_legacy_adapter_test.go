package qsruntime

import (
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLegacyBitmapQueryTimeWindowValueNormalizesEpochGranularity(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  string
	}{
		{name: "millis", value: 1685577600000, want: "2023-06-01T00"},
		{name: "micros", value: 1685577600000000, want: "2023-06-01T00"},
		{name: "nanos", value: 1685577600000000000, want: "2023-06-01T00"},
		{name: "seconds", value: 1685577600, want: "2023-06-01T00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyBitmapQueryTimeWindowValue(tt.value); got != tt.want {
				t.Fatalf("time window = %q, want %q", got, tt.want)
			}
		})
	}
}
func TestLegacyBitmapQueryAdapterConvertsBSIFragmentToProto(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(8),
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(query)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Index != "orders" {
		t.Fatalf("index = %q, want orders", fragment.Index)
	}
	if fragment.Field != "o_orderkey" {
		t.Fatalf("field = %q, want o_orderkey", fragment.Field)
	}
	if fragment.Operation != pb.QueryFragment_INTERSECT {
		t.Fatalf("operation = %v, want INTERSECT", fragment.Operation)
	}
	if fragment.BsiOp != pb.QueryFragment_GE {
		t.Fatalf("bsi op = %v, want GE", fragment.BsiOp)
	}
	if got := new(big.Int).SetBytes(fragment.Value).Int64(); got != 8 {
		t.Fatalf("value = %d, want 8", got)
	}
}

func TestLegacyBitmapQueryAdapterConvertsStandardDictionaryBitmapFragmentToProto(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_shipmode",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpNone,
			Values:    []*big.Int{big.NewInt(7)},
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(query)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Field != "l_shipmode" || fragment.RowID != 7 {
		t.Fatalf("fragment = %#v, want l_shipmode row ID 7", fragment)
	}
	if fragment.BsiOp != pb.QueryFragment_NA {
		t.Fatalf("bsi op = %v, want NA", fragment.BsiOp)
	}
	if len(fragment.Values) != 0 {
		t.Fatalf("values = %#v, want none for single dictionary bitmap", fragment.Values)
	}
}

func TestLegacyBitmapQueryAdapterConvertsSeedToExistenceFragment(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Seeds: []qsbridge.QuantaSeed{{
			Index: "lineitem",
			Field: "l_shipdate",
			Kind:  qsbridge.QuantaSeedTableExistence,
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(query)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Index != "lineitem" || fragment.Field != "l_shipdate" {
		t.Fatalf("fragment = %#v, want lineitem.l_shipdate", fragment)
	}
	if fragment.Operation != pb.QueryFragment_UNION || !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want UNION not-null existence fragment", fragment)
	}
}

func TestLegacyBitmapQueryAdapterAppliesSeedShardWindow(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Seeds: []qsbridge.QuantaSeed{{
			Index:       "lineitem",
			Field:       "l_shipdate",
			Kind:        qsbridge.QuantaSeedTableExistence,
			Begin:       big.NewInt(1685577600000),
			End:         big.NewInt(1685664000000),
			ShardWindow: true,
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(NewExecutionRequest(query))
	if got, want := time.Unix(0, proto.FromTime).UTC().Format(shared.YMDHTimeFmt), "2023-06-01T00"; got != want {
		t.Fatalf("from time = %q, want %q", got, want)
	}
	if got, want := time.Unix(0, proto.ToTime).UTC().Format(shared.YMDHTimeFmt), "2023-06-02T00"; got != want {
		t.Fatalf("to time = %q, want %q", got, want)
	}
}

func TestLegacyBitmapQueryAdapterKeepsShardWindowSeedOutOfBSICompare(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Seeds: []qsbridge.QuantaSeed{{
			Index:       "lineitem",
			Field:       "l_shipdate",
			Kind:        qsbridge.QuantaSeedTableExistence,
			Begin:       big.NewInt(1685577600000),
			End:         big.NewInt(1685664000000),
			ShardWindow: true,
		}},
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_shipmode",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpNone,
			Values:    []*big.Int{big.NewInt(7)},
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(NewExecutionRequest(query))
	if len(proto.Query) != 2 {
		t.Fatalf("fragments = %d, want shard seed plus dictionary bitmap", len(proto.Query))
	}
	var seed, leaf *pb.QueryFragment
	for _, fragment := range proto.Query {
		switch fragment.Field {
		case "l_shipdate":
			seed = fragment
		case "l_shipmode":
			leaf = fragment
		}
	}
	if seed == nil || leaf == nil {
		t.Fatalf("fragments = %#v, want shard seed and dictionary bitmap", proto.Query)
	}
	if seed.Field != "l_shipdate" || seed.Operation != pb.QueryFragment_UNION || !seed.NullCheck || !seed.Negate || seed.BsiOp != pb.QueryFragment_NA {
		t.Fatalf("seed fragment = %#v, want non-BSI shard-window existence", seed)
	}
	if leaf.Field != "l_shipmode" || leaf.RowID != 7 || leaf.BsiOp != pb.QueryFragment_NA || len(leaf.Values) != 0 {
		t.Fatalf("leaf fragment = %#v, want non-BSI dictionary bitmap row ID", leaf)
	}
}

func TestLegacyBitmapQueryAdapterPassesThroughDifferenceFragment(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "part",
			Field:     "p_brand",
			Operation: qsbridge.QuantaOperationDifference,
			BSIOp:     qsbridge.QuantaBSIOpEQ,
			Value:     big.NewInt(1954561617),
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(query)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Operation != pb.QueryFragment_DIFFERENCE {
		t.Fatalf("operation = %v, want DIFFERENCE", fragment.Operation)
	}
	if fragment.Negate {
		t.Fatalf("negate = true, want false for explicit difference fragment")
	}
}

func TestLegacyBitmapQueryAdapterConvertsLoweredExecutionRequest(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey >= ?"},
		qsbridge.ExecutionOptions{},
		qsbridge.IndexedParameterValue(1, qsbridge.ValueInt, int64(8)),
	)
	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(intermediate)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Index != "orders" {
		t.Fatalf("index = %q, want orders", fragment.Index)
	}
	if fragment.Field != "o_orderkey" {
		t.Fatalf("field = %q, want o_orderkey", fragment.Field)
	}
	if fragment.Operation != pb.QueryFragment_INTERSECT {
		t.Fatalf("operation = %v, want INTERSECT", fragment.Operation)
	}
	if fragment.BsiOp != pb.QueryFragment_GE {
		t.Fatalf("bsi op = %v, want GE", fragment.BsiOp)
	}
	if got := new(big.Int).SetBytes(fragment.Value).Int64(); got != 8 {
		t.Fatalf("value = %d, want 8", got)
	}
}

func TestLegacyBitmapQueryAdapterConvertsLoweredRangeExecutionRequest(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey >= ? and o.o_orderkey <= ?"},
		qsbridge.ExecutionOptions{},
		qsbridge.IndexedParameterValue(1, qsbridge.ValueInt, int64(8)),
		qsbridge.IndexedParameterValue(2, qsbridge.ValueInt, int64(12)),
	)
	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(intermediate)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.BsiOp != pb.QueryFragment_RANGE {
		t.Fatalf("bsi op = %v, want RANGE", fragment.BsiOp)
	}
	if len(fragment.Value) != 0 {
		t.Fatalf("value bytes = %v, want empty for range", fragment.Value)
	}
	if got := new(big.Int).SetBytes(fragment.Begin).Int64(); got != 8 {
		t.Fatalf("begin = %d, want 8", got)
	}
	if got := new(big.Int).SetBytes(fragment.End).Int64(); got != 12 {
		t.Fatalf("end = %d, want 12", got)
	}
}

func TestLegacyBitmapQueryAdapterKeepsLogicalRangeInclusiveBeforeRuntimeAdaptation(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey >= ? and o.o_orderkey <= ?"},
		qsbridge.ExecutionOptions{},
		qsbridge.IndexedParameterValue(1, qsbridge.ValueInt, int64(8)),
		qsbridge.IndexedParameterValue(2, qsbridge.ValueInt, int64(12)),
	)
	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}

	fragment := intermediate.Fragments[0]
	if got := fragment.End.Int64(); got != 12 {
		t.Fatalf("logical range end = %d, want inclusive SQL end 12", got)
	}
}

func TestLegacyBitmapQueryAdapterDefaultsRequestWindowToUnixZero(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "customers_qa",
		Field:     "rownum",
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}}

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(NewExecutionRequest(query))
	if proto.FromTime != 0 {
		t.Fatalf("from time = %d, want unix-zero shard", proto.FromTime)
	}
	if proto.ToTime != 0 {
		t.Fatalf("to time = %d, want unix-zero shard", proto.ToTime)
	}
}
func TestLegacyBitmapQueryAdapterAddsLegacyTimeWindowForDatetimeRange(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select l.l_suppkey from lineitem as l where l.l_shipdate >= '1996-01-01' and l.l_shipdate < '1996-04-01'"},
		qsbridge.ExecutionOptions{},
	)
	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	for i := range intermediate.Fragments {
		if intermediate.Fragments[i].Field == "l_shipdate" {
			intermediate.Fragments[i].ShardWindow = true
		}
	}

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(NewSQLExecutionRequest(intermediate, request))
	if got, want := proto.FromTime, time.Date(1996, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(); got != want {
		t.Fatalf("from time = %d, want %d", got, want)
	}
	if got, want := proto.ToTime, time.Date(1996, 3, 31, 23, 0, 0, 0, time.UTC).UnixNano(); got != want {
		t.Fatalf("to time = %d, want %d", got, want)
	}
}

func TestLegacyBitmapQueryAdapterDoesNotUseProjectedNonShardTimestampAsWindow(t *testing.T) {
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "customers_qa",
			Field:     "timestamp_micro",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpRange,
			Begin:     big.NewInt(time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
			End:       big.NewInt(time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:        "customers_qa",
			Field:        "timestamp_micro",
			PhysicalName: "timestamp_micro",
			Type:         qsbridge.DataTypeTime,
		}},
	}

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(NewExecutionRequest(query))
	if proto.FromTime != 0 {
		t.Fatalf("from time = %d, want unix-zero shard for non-physical timestamp predicate", proto.FromTime)
	}
	if proto.ToTime != 0 {
		t.Fatalf("to time = %d, want unix-zero shard for non-physical timestamp predicate", proto.ToTime)
	}
}

func TestLegacyBitmapQueryAdapterConvertsLoweredInExecutionRequest(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey in (?, ?)"},
		qsbridge.ExecutionOptions{},
		qsbridge.IndexedParameterValue(1, qsbridge.ValueInt, int64(7)),
		qsbridge.IndexedParameterValue(2, qsbridge.ValueInt, int64(9)),
	)
	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(intermediate)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.BsiOp != pb.QueryFragment_BATCH_EQ {
		t.Fatalf("bsi op = %v, want BATCH_EQ", fragment.BsiOp)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if got := new(big.Int).SetBytes(fragment.Values[0]).Int64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
	if got := new(big.Int).SetBytes(fragment.Values[1]).Int64(); got != 9 {
		t.Fatalf("values[1] = %d, want 9", got)
	}
}

func TestLegacyBitmapQueryAdapterConvertsLoweredStringEnumExecutionRequest(t *testing.T) {
	service := qsbridge.NewPlanningService(qsbridge.Planner{
		Parser:        qsbridge.SimpleParserBridge{},
		Catalog:       runtimeAdapterCatalog(),
		DefaultSchema: "quanta",
		Scope:         qsbridge.PhysicalScope{Placement: qsbridge.PlacementLocal},
	}, nil)

	_, request := service.PrepareExecutionRequest(
		qsbridge.PlanRequest{SQL: "select l.l_shipmode as shipmode from lineitem as l where l.l_shipmode in ('AIR', 'MAIL')"},
		qsbridge.ExecutionOptions{},
	)
	intermediate, diagnostics := (qsbridge.QuantaIntermediateLowerer{Dictionaries: runtimeAdapterDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}

	proto := LegacyBitmapQueryAdapter{}.ToProto(intermediate)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.BsiOp != pb.QueryFragment_NA {
		t.Fatalf("bsi op = %v, want NA for StringEnum bitmap", fragment.BsiOp)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if got := new(big.Int).SetBytes(fragment.Values[0]).Uint64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
	if got := new(big.Int).SetBytes(fragment.Values[1]).Uint64(); got != 8 {
		t.Fatalf("values[1] = %d, want 8", got)
	}
}

func TestLegacyDirectExecutionWithShardWindowAddsSeedForCandidateStringEnumPredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipmode", Type: "String", MappingStrategy: "StringEnum"}},
		},
	}
	table.TimeQuantumType = "YMD"
	table.TimeQuantumField = "l_shipdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "lineitem",
		Field:     "l_shipmode",
		Operation: qsbridge.QuantaOperationIntersect,
		Values:    []*big.Int{big.NewInt(7), big.NewInt(8)},
	}}})
	request.SourceIndexes = []string{"lineitem"}
	request.HasCandidateSet = true

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Seeds) != 1 {
		t.Fatalf("seeds = %#v, want synthetic shard-window seed", adapted.Query.Seeds)
	}
	shard := adapted.Query.Seeds[0]
	if shard.Index != "lineitem" || shard.Field != "l_shipdate" || shard.Kind != qsbridge.QuantaSeedTableExistence || !shard.ShardWindow {
		t.Fatalf("synthetic seed = %#v, want lineitem.l_shipdate shard-window existence", shard)
	}
	if shard.Begin.Int64() != legacyDirectRelationshipFullTimeRangeBeginMillis || shard.End.Int64() != legacyDirectRelationshipFullTimeRangeEndMillis {
		t.Fatalf("synthetic range = %d..%d, want full shard window", shard.Begin.Int64(), shard.End.Int64())
	}
	if len(adapted.Query.Fragments) != 1 || adapted.Query.Fragments[0].Field != "l_shipmode" {
		t.Fatalf("fragments = %#v, want original l_shipmode predicate preserved without synthetic BSI range", adapted.Query.Fragments)
	}
	if len(adapted.Query.ProjectionFields) == 0 || adapted.Query.ProjectionFields[0].Field != "l_shipdate" || adapted.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want shard time metadata", adapted.Query.ProjectionFields)
	}
}

func TestLegacyDirectExecutionWithShardWindowAddsSeedForPlainPredicateOnPhysicalTimeShard(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipmode", Type: "String", MappingStrategy: "StringEnum"}},
		},
	}
	table.TimeQuantumType = "YMD"
	table.TimeQuantumField = "l_shipdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "lineitem",
		Field:     "l_shipmode",
		Operation: qsbridge.QuantaOperationIntersect,
		Values:    []*big.Int{big.NewInt(7), big.NewInt(8)},
	}}})
	request.SourceIndexes = []string{"lineitem"}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Seeds) != 1 {
		t.Fatalf("seeds = %#v, want synthetic shard-window seed", adapted.Query.Seeds)
	}
	shard := adapted.Query.Seeds[0]
	if shard.Index != "lineitem" || shard.Field != "l_shipdate" || shard.Kind != qsbridge.QuantaSeedTableExistence || !shard.ShardWindow {
		t.Fatalf("synthetic seed = %#v, want lineitem.l_shipdate shard-window existence", shard)
	}
	if len(adapted.Query.Fragments) != 1 || adapted.Query.Fragments[0].Field != "l_shipmode" {
		t.Fatalf("fragments = %#v, want original l_shipmode predicate preserved without synthetic BSI range", adapted.Query.Fragments)
	}
	if len(adapted.Query.ProjectionFields) == 0 || adapted.Query.ProjectionFields[0].Field != "l_shipdate" || adapted.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want shard time metadata", adapted.Query.ProjectionFields)
	}
}

func TestLegacyDirectExecutionWithShardWindowSkipsBareFullTableScan(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum", Type: "Int", MappingStrategy: "IntBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "birthdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}
	table.TimeQuantumField = "birthdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"customers_qa"}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Fragments) != 0 {
		t.Fatalf("fragments = %#v, want no synthetic shard range for bare full-table scan", adapted.Query.Fragments)
	}
}

func TestLegacyDirectExecutionWithShardWindowSkipsNonPhysicalTimestampPredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "orders_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "order_date", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}
	table.TimeQuantumField = "order_date"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "orders_qa",
		Field:     "order_date",
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpGE,
		Value:     big.NewInt(1685664000000),
	}}})
	request.SourceIndexes = []string{"orders_qa"}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want only original non-physical timestamp predicate", adapted.Query.Fragments)
	}
	if adapted.Query.Fragments[0].Field != "order_date" || adapted.Query.Fragments[0].BSIOp != qsbridge.QuantaBSIOpGE {
		t.Fatalf("fragment = %#v, want original order_date predicate", adapted.Query.Fragments[0])
	}
}

func TestLegacyDirectExecutionWithShardWindowSkipsPlainSingleTablePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "birthdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "age", Type: "Int", MappingStrategy: "IntBSI"}},
		},
	}
	table.TimeQuantumField = "birthdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "customers_qa",
		Field:     "age",
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpEQ,
		Value:     big.NewInt(42),
	}}})
	request.SourceIndexes = []string{"customers_qa"}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want only original predicate", adapted.Query.Fragments)
	}
	if adapted.Query.Fragments[0].Field != "age" {
		t.Fatalf("fragment = %#v, want age predicate", adapted.Query.Fragments[0])
	}
}
func TestLegacyDirectExecutionWithShardWindowAddsSeedForNonShardTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_receiptdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}
	table.TimeQuantumType = "YMD"
	table.TimeQuantumField = "l_shipdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "lineitem",
		Field:     "l_receiptdate",
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpRange,
		Begin:     big.NewInt(757382400000),
		End:       big.NewInt(788918400000),
	}}})
	request.SourceIndexes = []string{"lineitem"}
	request.Query.ProjectionFields = []qsbridge.QuantaProjectionField{{
		Index:        "lineitem",
		Field:        "l_receiptdate",
		PhysicalName: "l_receiptdate",
		Type:         qsbridge.DataTypeTime,
	}}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Seeds) != 1 {
		t.Fatalf("seeds = %#v, want synthetic shard-window seed", adapted.Query.Seeds)
	}
	if adapted.Query.Seeds[0].Index != "lineitem" || adapted.Query.Seeds[0].Field != "l_shipdate" || !adapted.Query.Seeds[0].ShardWindow {
		t.Fatalf("synthetic seed = %#v, want lineitem.l_shipdate shard-window seed", adapted.Query.Seeds[0])
	}
	if len(adapted.Query.Fragments) != 1 || adapted.Query.Fragments[0].Field != "l_receiptdate" {
		t.Fatalf("fragments = %#v, want original receiptdate predicate preserved without synthetic BSI range", adapted.Query.Fragments)
	}
	if len(adapted.Query.ProjectionFields) < 2 || adapted.Query.ProjectionFields[0].Field != "l_shipdate" || adapted.Query.ProjectionFields[1].Field != "l_receiptdate" {
		t.Fatalf("projection fields = %#v, want shard metadata before receiptdate metadata", adapted.Query.ProjectionFields)
	}
}

func TestLegacyDirectExecutionWithShardWindowMarksOneSidedShardTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}
	table.TimeQuantumType = "YMD"
	table.TimeQuantumField = "l_shipdate"
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     "lineitem",
		Field:     "l_shipdate",
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpGT,
		Value:     big.NewInt(795398400000),
	}}})
	request.SourceIndexes = []string{"lineitem"}

	adapted := legacyDirectExecutionWithShardWindow(request, table)

	if len(adapted.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want original one-sided shipdate predicate", adapted.Query.Fragments)
	}
	predicate := adapted.Query.Fragments[0]
	if predicate.Field != "l_shipdate" || predicate.BSIOp != qsbridge.QuantaBSIOpGT || predicate.Value.Int64() != 795398400000 {
		t.Fatalf("predicate fragment = %#v, want original one-sided shipdate predicate preserved", predicate)
	}
	if !predicate.ShardWindow {
		t.Fatalf("predicate fragment = %#v, want shard window marker", predicate)
	}
	if len(adapted.Query.ProjectionFields) == 0 || adapted.Query.ProjectionFields[0].Field != "l_shipdate" || adapted.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want shard time metadata", adapted.Query.ProjectionFields)
	}
}

func TestLegacyBitmapQueryAdapterClonesMutableValues(t *testing.T) {
	value := big.NewInt(8)
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     value,
		}},
	}

	bitmap := LegacyBitmapQueryAdapter{}.ToBitmapQuery(query)
	value.SetInt64(9)
	proto := bitmap.ToProto()
	if got := new(big.Int).SetBytes(proto.Query[0].Value).Int64(); got != 8 {
		t.Fatalf("value = %d after source mutation, want 8", got)
	}
}

func TestLegacyBitmapQueryAdapterValidatesMissingFragmentShape(t *testing.T) {
	diagnostics := LegacyBitmapQueryAdapter{}.ValidateIntermediateQuery(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
		}},
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected validation diagnostics")
	}
	if len(diagnostics) != 3 {
		t.Fatalf("diagnostics = %d, want 3", len(diagnostics))
	}
}

func TestLegacyBitmapQueryAdapterValidatesRangeOperands(t *testing.T) {
	diagnostics := LegacyBitmapQueryAdapter{}.ValidateIntermediateQuery(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpRange,
			Begin:     big.NewInt(8),
		}},
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing range operand diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInvalidExecutionOption)
	}
}

func runtimeAdapterCatalog() qsbridge.MemoryCatalog {
	return qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []qsbridge.FieldDefinition{
					{Name: "o_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
				},
			},
			{
				Schema: "quanta",
				Name:   "lineitem",
				Fields: []qsbridge.FieldDefinition{
					{
						Name:  "l_shipmode",
						Type:  qsbridge.DataTypeString,
						Index: qsbridge.IndexStringEnum,
						Dictionary: qsbridge.DictionaryDefinition{
							Ref:          qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"},
							Version:      "v1",
							Capabilities: qsbridge.DictionaryCapabilities{qsbridge.DictionaryCapabilityStableIDs},
						},
					},
					{Name: "l_shipdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime, Encoding: qsbridge.LegacyEncodingProfile("SysMillisBSI", qsbridge.LegacyEncodingOptions{})},
					{Name: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
				},
			},
		},
	}
}

func runtimeAdapterDictionaries() qsbridge.MemoryDictionaryResolver {
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	return qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{
			Ref:          ref,
			Version:      "v1",
			Capabilities: qsbridge.DictionaryCapabilities{qsbridge.DictionaryCapabilityStableIDs},
		}},
		Entries: []qsbridge.DictionaryEntry{
			{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
			{Ref: ref, Label: "MAIL", ID: 8, Version: "v1"},
		},
	}
}
