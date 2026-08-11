package qsruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestNativeProjectionMaterializationKernelReadsSimpleFields(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true}
	kernel := NativeProjectionMaterializationKernel{
		Reader: NativeProjectionFieldReaderFunc(func(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
			if request.Index != "orders" || request.Field.Field != "o_orderkey" {
				t.Fatalf("read request = %#v, want orders.o_orderkey", request)
			}
			return NativeProjectionFieldReadResult{
				Field: request.Field,
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1001)},
					{Kind: qsbridge.ValueInt, Value: int64(1002)},
				},
				Probes: []ExecutionProbe{{Section: "native_projection_materialization", Name: "field_read", Value: "o_orderkey"}},
			}, nil, nil
		}),
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "orders",
			Rownums:          []qsbridge.QuantaRownum{7, 8},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
			DependencyID:     "orders_fields",
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(result.Results) != 1 {
		t.Fatalf("results = %#v, want one materialized batch", result.Results)
	}
	rowSet := result.Results[0].RowSet
	if rowSet.CandidateCount() != 2 || rowSet.ProjectionCount() != 1 {
		t.Fatalf("rowSet = %#v, want two candidates and one vector", rowSet)
	}
	if got := rowSet.ProjectionVectors[0].Values[1].Value; got != int64(1002) {
		t.Fatalf("second value = %#v, want 1002", got)
	}
	if !nativeProjectionMaterializationTestProbeName(result.Probes, "projection_materialization_request_count") ||
		!nativeProjectionMaterializationTestProbeName(result.Probes, "field_read") ||
		!nativeProjectionMaterializationTestProbeName(result.Probes, "projection_materialization_elapsed") {
		t.Fatalf("probes = %#v, want request-count and field-read probes", result.Probes)
	}
}

func TestNativeProjectionMaterializationKernelReadsProjectionExpressions(t *testing.T) {
	table := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	shipDate := qsbridge.FieldRef{Table: table, Name: "l_shipdate", Type: qsbridge.DataTypeTime}
	expression := qsbridge.QuantaProjectionExpression{
		Expr: qsbridge.FunctionCall(
			qsbridge.FunctionDefinition{Name: "year", Kind: qsbridge.FunctionScalar, ReturnType: qsbridge.DataTypeInt},
			qsbridge.Field(shipDate),
		),
		Output: qsbridge.QuantaProjectionField{Index: "lineitem", Role: "l", Field: "year_l_shipdate", Type: qsbridge.DataTypeInt},
	}
	reader := &recordingNativeProjectionExpressionReader{}
	kernel := NativeProjectionMaterializationKernel{Reader: reader}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:                 "lineitem",
			Rownums:               []qsbridge.QuantaRownum{7, 8},
			ProjectionExpressions: []qsbridge.QuantaProjectionExpression{expression},
			DependencyID:          "lineitem_expr",
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if reader.fieldCalls != 0 {
		t.Fatalf("field reader calls = %d, want 0", reader.fieldCalls)
	}
	if len(reader.expressionRequests) != 1 {
		t.Fatalf("expression requests = %#v, want one", reader.expressionRequests)
	}
	rowSet := result.Results[0].RowSet
	if rowSet.CandidateCount() != 2 || rowSet.ProjectionCount() != 1 {
		t.Fatalf("rowSet = %#v, want two candidates and one derived vector", rowSet)
	}
	vector := rowSet.ProjectionVectors[0]
	if vector.Field.Field != "year_l_shipdate" {
		t.Fatalf("derived field = %#v, want year_l_shipdate", vector.Field)
	}
	if vector.Values[0].Value != int64(1998) || vector.Values[1].Value != int64(1999) {
		t.Fatalf("derived values = %#v, want 1998/1999", vector.Values)
	}
	if !nativeProjectionMaterializationTestProbeName(result.Probes, "expression_read_elapsed") {
		t.Fatalf("probes = %#v, want expression_read_elapsed", result.Probes)
	}
}

func TestNativeProjectionMaterializationKernelReusesCachedProjectionValues(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())
	field := qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true}
	readRownums := [][]qsbridge.QuantaRownum{}
	kernel := NativeProjectionMaterializationKernel{
		Reader: NativeProjectionFieldReaderFunc(func(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
			readRownums = append(readRownums, append([]qsbridge.QuantaRownum(nil), request.Rownums...))
			values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
			for _, rownum := range request.Rownums {
				values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum) * 10})
			}
			return NativeProjectionFieldReadResult{
				Field:  request.Field,
				Values: values,
			}, nil, nil
		}),
	}

	first, err := kernel.MaterializeProjectionBatches(ctx, ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("first MaterializeProjectionBatches error = %v", err)
	}
	if first.Diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v, want none", first.Diagnostics)
	}
	second, err := kernel.MaterializeProjectionBatches(ctx, ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2, 3},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("second MaterializeProjectionBatches error = %v", err)
	}
	if second.Diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", second.Diagnostics)
	}
	if len(readRownums) != 2 {
		t.Fatalf("reader calls = %#v, want two calls", readRownums)
	}
	if !sameRownumSlicesEqual(readRownums[0], []qsbridge.QuantaRownum{1, 2}) {
		t.Fatalf("first read rownums = %#v, want 1/2", readRownums[0])
	}
	if !sameRownumSlicesEqual(readRownums[1], []qsbridge.QuantaRownum{3}) {
		t.Fatalf("second read rownums = %#v, want only missing row 3", readRownums[1])
	}
	values := second.Results[0].RowSet.ProjectionVectors[0].Values
	if values[0].Value != int64(10) || values[1].Value != int64(20) || values[2].Value != int64(30) {
		t.Fatalf("second values = %#v, want cached 10/20 plus fetched 30", values)
	}
	if !nativeProjectionMaterializationTestProbeName(second.Probes, "projection_value_cache_hit") ||
		!nativeProjectionMaterializationTestProbeName(second.Probes, "projection_value_cache_missing_rows") {
		t.Fatalf("second probes = %#v, want projection value cache probes", second.Probes)
	}
	snapshot := ExecutionInstrumentationSnapshotFromContext(ctx)
	assertExecutionCounter(t, snapshot, "query_scratchpad", "projection_value_cache_hit", 1)
	if got := executionCounterObservationCount(snapshot, "query_scratchpad", "projection_value_cache_store"); got != 2 {
		t.Fatalf("projection value cache store observations = %d, want 2", got)
	}
}

func TestNativeProjectionMaterializationKernelUsesBatchFieldReader(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Field: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "lineitem", Field: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Visible: true},
	}
	reader := &recordingNativeProjectionBatchFieldReader{}
	kernel := NativeProjectionMaterializationKernel{Reader: reader}

	result, err := kernel.MaterializeProjectionBatches(ctx, ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: fields,
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if reader.singleCalls != 0 {
		t.Fatalf("single reader calls = %d, want 0", reader.singleCalls)
	}
	if len(reader.batchRequests) != 1 || len(reader.batchRequests[0]) != 2 {
		t.Fatalf("batch requests = %#v, want one two-field batch", reader.batchRequests)
	}
	rowSet := result.Results[0].RowSet
	if got := rowSet.ProjectionVectors[0].Values[1].Value; got != int64(20) {
		t.Fatalf("l_orderkey row 2 = %#v, want 20", got)
	}
	if got := rowSet.ProjectionVectors[1].Values[1].Value; got != int64(200) {
		t.Fatalf("l_suppkey row 2 = %#v, want 200", got)
	}
}

type recordingNativeProjectionBatchFieldReader struct {
	singleCalls   int
	batchRequests [][]NativeProjectionFieldReadRequest
}

func (r *recordingNativeProjectionBatchFieldReader) ReadProjectionField(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	r.singleCalls++
	return NativeProjectionFieldReadResult{Field: request.Field}, nil, nil
}

func (r *recordingNativeProjectionBatchFieldReader) ReadProjectionFields(_ context.Context, requests []NativeProjectionFieldReadRequest) ([]NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	r.batchRequests = append(r.batchRequests, append([]NativeProjectionFieldReadRequest(nil), requests...))
	results := make([]NativeProjectionFieldReadResult, 0, len(requests))
	for _, request := range requests {
		multiplier := int64(10)
		if strings.EqualFold(request.Field.Field, "l_suppkey") {
			multiplier = 100
		}
		values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
		for _, rownum := range request.Rownums {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum) * multiplier})
		}
		results = append(results, NativeProjectionFieldReadResult{Field: request.Field, Values: values})
	}
	return results, nil, nil
}

type recordingNativeProjectionExpressionReader struct {
	fieldCalls         int
	expressionRequests []NativeProjectionExpressionReadRequest
}

func (r *recordingNativeProjectionExpressionReader) ReadProjectionField(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
	r.fieldCalls++
	return NativeProjectionFieldReadResult{Field: request.Field}, nil, nil
}

func (r *recordingNativeProjectionExpressionReader) ReadProjectionExpression(_ context.Context, request NativeProjectionExpressionReadRequest) (NativeProjectionExpressionReadResult, qsbridge.DiagnosticSet, error) {
	r.expressionRequests = append(r.expressionRequests, request)
	return NativeProjectionExpressionReadResult{
		Expression: request.Expression,
		Values: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(1998)},
			{Kind: qsbridge.ValueInt, Value: int64(1999)},
		},
		Probes: []ExecutionProbe{{Section: "native_projection_materialization", Name: "fake_expression_read", Value: "year"}},
	}, nil, nil
}

func nativeProjectionMaterializationTestProbeName(probes []ExecutionProbe, name string) bool {
	for _, probe := range probes {
		if probe.Name == name {
			return true
		}
	}
	return false
}

func executionCounterObservationCount(snapshot ExecutionInstrumentationSnapshot, section string, name string) int {
	count := 0
	for _, counter := range snapshot.Counters {
		if counter.Section == section && counter.Name == name {
			count++
		}
	}
	return count
}

func TestNativeProjectionMaterializationKernelRehydratesEncodedStrings(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString, Visible: true}
	kernel := NativeProjectionMaterializationKernel{
		Reader: NativeProjectionFieldReaderFunc(func(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
			return NativeProjectionFieldReadResult{
				Field:      request.Field,
				Encoded:    true,
				Dictionary: "lineitem.l_shipmode",
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(2)},
				},
			}, nil, nil
		}),
		Rehydrator: NativeProjectionValueRehydratorFunc(func(_ context.Context, request NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
			if request.Dictionary != "lineitem.l_shipmode" {
				t.Fatalf("dictionary = %q, want lineitem.l_shipmode", request.Dictionary)
			}
			return NativeProjectionValueRehydrationResult{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "AIR"},
					{Kind: qsbridge.ValueString, Value: "TRUCK"},
				},
			}, nil, nil
		}),
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	values := result.Results[0].RowSet.ProjectionVectors[0].Values
	if values[0].Value != "AIR" || values[1].Value != "TRUCK" {
		t.Fatalf("rehydrated values = %#v, want AIR/TRUCK", values)
	}
}

func TestNativeProjectionMaterializationKernelRehydratesStringEnumIDsWithoutFallback(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString, Visible: true}
	kernel := NativeProjectionMaterializationKernel{
		Reader: NativeProjectionFieldReaderFunc(func(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
			return NativeProjectionFieldReadResult{
				Field:      request.Field,
				Encoded:    true,
				Dictionary: "lineitem.l_shipmode",
				LookupKind: NativeProjectionLookupDictionary,
				LookupRef:  "lineitem.l_shipmode",
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(2)},
				},
			}, nil, nil
		}),
		Rehydrator: NativeProjectionDictionaryLabelRehydrator{
			Labels: map[string]map[int64]string{
				"lineitem.l_shipmode": {
					1: "AIR",
					2: "TRUCK",
				},
			},
		},
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	values := result.Results[0].RowSet.ProjectionVectors[0].Values
	if values[0].Value != "AIR" || values[1].Value != "TRUCK" {
		t.Fatalf("values = %#v, want AIR/TRUCK", values)
	}
	for _, probe := range result.Probes {
		if probe.Name == "projection_materialization_fallback_to_compat" {
			t.Fatalf("probes = %#v, did not expect compat fallback", result.Probes)
		}
	}
}

func TestNativeProjectionMaterializationKernelRehydratesStringEnumIDsFromCatalogResolver(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString, Visible: true}
	dictionaryRef := qsbridge.DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "lineitem",
		Fields: []qsbridge.FieldDefinition{{
			Name: "l_shipmode",
			Type: qsbridge.DataTypeString,
			Dictionary: qsbridge.DictionaryDefinition{
				Ref: dictionaryRef,
			},
		}},
	}}, nil, nil)
	resolver := qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{Ref: dictionaryRef}},
		Entries: []qsbridge.DictionaryEntry{
			{Ref: dictionaryRef, ID: 1, Label: "AIR"},
			{Ref: dictionaryRef, ID: 2, Label: "TRUCK"},
		},
	}
	kernel := NativeProjectionMaterializationKernel{
		Reader: NativeProjectionFieldReaderFunc(func(_ context.Context, request NativeProjectionFieldReadRequest) (NativeProjectionFieldReadResult, qsbridge.DiagnosticSet, error) {
			return NativeProjectionFieldReadResult{
				Field:      request.Field,
				Encoded:    true,
				LookupKind: NativeProjectionLookupDictionary,
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(1)},
					{Kind: qsbridge.ValueInt, Value: int64(2)},
				},
			}, nil, nil
		}),
		Rehydrator: NativeProjectionDictionaryLabelRehydrator{
			Catalog:  catalog,
			Resolver: resolver,
		},
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "lineitem",
			Rownums:          []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	values := result.Results[0].RowSet.ProjectionVectors[0].Values
	if values[0].Value != "AIR" || values[1].Value != "TRUCK" {
		t.Fatalf("values = %#v, want AIR/TRUCK", values)
	}
	if !nativeProjectionMaterializationTestProbeName(result.Probes, "dictionary_resolver_rehydration_values") {
		t.Fatalf("probes = %#v, want dictionary resolver rehydration probe", result.Probes)
	}
}

func TestNewNativeProjectionDictionaryLabelRehydratorCachesResolverLookups(t *testing.T) {
	ref := qsbridge.DictionaryRef{Table: "lineitem", Field: "l_shipmode"}
	backend := &countingDictionaryResolver{
		dictionary: qsbridge.DictionaryDefinition{Ref: ref},
		entries: map[qsbridge.StringEnumID]string{
			1: "AIR",
			2: "TRUCK",
		},
	}
	rehydrator := NewNativeProjectionDictionaryLabelRehydrator(qsbridge.QueryCatalogView{}, backend)

	result, diagnostics, err := rehydrator.RehydrateProjectionValues(context.Background(), NativeProjectionValueRehydrationRequest{
		Index:      "lineitem",
		Field:      qsbridge.QuantaProjectionField{Index: "lineitem", Field: "l_shipmode", Type: qsbridge.DataTypeString},
		LookupKind: NativeProjectionLookupDictionary,
		LookupRef:  "lineitem.l_shipmode",
		Values: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(1)},
			{Kind: qsbridge.ValueInt, Value: int64(1)},
			{Kind: qsbridge.ValueInt, Value: int64(2)},
			{Kind: qsbridge.ValueInt, Value: int64(2)},
		},
	})
	if err != nil {
		t.Fatalf("RehydrateProjectionValues error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := backend.lookupIDCalls; got != 2 {
		t.Fatalf("LookupID calls = %d, want one backend lookup per distinct id", got)
	}
	if result.Values[0].Value != "AIR" || result.Values[1].Value != "AIR" || result.Values[2].Value != "TRUCK" || result.Values[3].Value != "TRUCK" {
		t.Fatalf("values = %#v, want cached AIR/AIR/TRUCK/TRUCK", result.Values)
	}
}
func TestNativeProjectionCompositeRehydratorRoutesBackingStringLookups(t *testing.T) {
	calledBackingStrings := false
	rehydrator := NativeProjectionCompositeRehydrator{
		Dictionary: NativeProjectionValueRehydratorFunc(func(context.Context, NativeProjectionValueRehydrationRequest) (NativeProjectionValueRehydrationResult, qsbridge.DiagnosticSet, error) {
			t.Fatalf("dictionary rehydrator should not be called for backing strings")
			return NativeProjectionValueRehydrationResult{}, nil, nil
		}),
		BackingStrings: NativeProjectionBackingStringLookupReaderFunc(func(_ context.Context, request NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error) {
			calledBackingStrings = true
			if request.LookupRef != "customer.c_name" || len(request.Values) != 2 {
				t.Fatalf("backing-string request = %#v, want customer.c_name with two values", request)
			}
			return NativeProjectionBackingStringLookupResult{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "Customer#1"},
					{Kind: qsbridge.ValueString, Value: "Customer#2"},
				},
				Probes: []ExecutionProbe{{Section: "native_projection_materialization", Name: "fake_backing_string_lookup"}},
			}, nil, nil
		}),
	}

	result, diagnostics, err := rehydrator.RehydrateProjectionValues(context.Background(), NativeProjectionValueRehydrationRequest{
		Index:      "customer",
		Field:      qsbridge.QuantaProjectionField{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString},
		LookupKind: NativeProjectionLookupBackingString,
		LookupRef:  "customer.c_name",
		Values: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: uint64(1)},
			{Kind: qsbridge.ValueInt, Value: uint64(2)},
		},
	})
	if err != nil {
		t.Fatalf("RehydrateProjectionValues error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !calledBackingStrings {
		t.Fatalf("backing-string lookup reader was not called")
	}
	if result.Values[0].Value != "Customer#1" || result.Values[1].Value != "Customer#2" {
		t.Fatalf("values = %#v, want Customer#1/Customer#2", result.Values)
	}
	if len(result.Probes) != 1 || result.Probes[0].Name != "fake_backing_string_lookup" {
		t.Fatalf("probes = %#v, want fake backing-string probe", result.Probes)
	}
}

func TestNativeProjectionCompositeRehydratorJoinsStringLexPrefixAndSuffix(t *testing.T) {
	rehydrator := NativeProjectionCompositeRehydrator{
		BackingStrings: NativeProjectionBackingStringLookupReaderFunc(func(_ context.Context, request NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error) {
			if len(request.Values) != 2 {
				t.Fatalf("request values = %#v, want two StringLex remainder keys", request.Values)
			}
			return NativeProjectionBackingStringLookupResult{
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "ires"},
					{Kind: qsbridge.ValueString, Value: ""},
				},
			}, nil, nil
		}),
	}

	result, diagnostics, err := rehydrator.RehydrateProjectionValues(context.Background(), NativeProjectionValueRehydrationRequest{
		Index:      "customer",
		Field:      qsbridge.QuantaProjectionField{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString},
		LookupKind: NativeProjectionLookupBackingString,
		LookupRef:  "customer.c_name",
		Values: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: NativeProjectionStringRemainderKey{RowNum: 11, Prefix: "Buenos A"}},
			{Kind: qsbridge.ValueInt, Value: NativeProjectionStringRemainderKey{RowNum: 12, Prefix: "Seattle"}},
		},
	})
	if err != nil {
		t.Fatalf("RehydrateProjectionValues error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if result.Values[0].Value != "Buenos Aires" || result.Values[1].Value != "Seattle" {
		t.Fatalf("values = %#v, want full StringLex values", result.Values)
	}
}

func TestFallbackProjectionMaterializationKernelUsesFallbackForUnsupportedNative(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	calledFallback := false
	kernel := FallbackProjectionMaterializationKernel{
		Preferred: NativeProjectionMaterializationKernel{},
		Fallback: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			calledFallback = true
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: request.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      "fallback",
					Request: request.Requests[0],
					RowSet: qsbridge.QuantaProjectedRowSet{
						Index:   "orders",
						Rownums: []qsbridge.QuantaRownum{7},
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Field:  field,
							Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1007)}},
						}},
					},
				}},
			}, nil
		}),
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "orders",
			Rownums:          []qsbridge.QuantaRownum{7},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if !calledFallback {
		t.Fatalf("fallback kernel was not called")
	}
	if got := result.Results[0].RowSet.ProjectionVectors[0].Values[0].Value; got != int64(1007) {
		t.Fatalf("fallback value = %#v, want 1007", got)
	}
	if len(result.Probes) < 2 || result.Probes[0].Name != "fallback_to_compat" || result.Probes[0].Value != "true" {
		t.Fatalf("fallback probes = %#v, want fallback_to_compat marker", result.Probes)
	}
}

func TestFallbackProjectionMaterializationKernelDoesNotFallbackForExpressions(t *testing.T) {
	calledFallback := false
	kernel := FallbackProjectionMaterializationKernel{
		Preferred: NativeProjectionMaterializationKernel{},
		Fallback: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			calledFallback = true
			return qsbridge.ProjectionMaterializationKernelResult{ID: request.ID}, nil
		}),
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:   "lineitem",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionExpressions: []qsbridge.QuantaProjectionExpression{{
				Expr: qsbridge.Call("year", qsbridge.Field(qsbridge.FieldRef{
					Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
					Name:  "l_shipdate",
					Type:  qsbridge.DataTypeTime,
				})),
				Output: qsbridge.QuantaProjectionField{Index: "lineitem", Field: "year_l_shipdate", Type: qsbridge.DataTypeInt},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if calledFallback {
		t.Fatalf("fallback kernel was called for expression projection")
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want native unsupported diagnostic", result.Diagnostics)
	}
}

func TestFallbackProjectionMaterializationKernelPreservesNativeDiagnosticsWithoutFallback(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	kernel := FallbackProjectionMaterializationKernel{
		Preferred: NativeProjectionMaterializationKernel{},
	}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "orders",
			Rownums:          []qsbridge.QuantaRownum{7},
			ProjectionFields: []qsbridge.QuantaProjectionField{field},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want native unsupported diagnostic", result.Diagnostics)
	}
	if got := result.Diagnostics[0].Message; !strings.Contains(got, "native projection materialization has no field reader") {
		t.Fatalf("diagnostic message = %q, want native field-reader detail", got)
	}
}

type countingDictionaryResolver struct {
	dictionary    qsbridge.DictionaryDefinition
	entries       map[qsbridge.StringEnumID]string
	lookupIDCalls int
}

func (r *countingDictionaryResolver) Dictionary(ref qsbridge.DictionaryRef) (qsbridge.DictionaryDefinition, qsbridge.DiagnosticSet) {
	if !strings.EqualFold(ref.Table, r.dictionary.Ref.Table) || !strings.EqualFold(ref.Field, r.dictionary.Ref.Field) {
		return qsbridge.DictionaryDefinition{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseExecute, "dictionary not found: "+ref.QualifiedName()),
		}
	}
	return r.dictionary, nil
}

func (r *countingDictionaryResolver) LookupLabel(ref qsbridge.DictionaryRef, label string) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	for id, entryLabel := range r.entries {
		if entryLabel == label {
			return qsbridge.DictionaryEntry{Ref: ref, Label: label, ID: id}, nil
		}
	}
	return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryLabelNotFound, qsbridge.PhaseExecute, "dictionary label not found: "+ref.QualifiedName()),
	}
}

func (r *countingDictionaryResolver) LookupID(ref qsbridge.DictionaryRef, id qsbridge.StringEnumID) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	r.lookupIDCalls++
	label, ok := r.entries[id]
	if !ok {
		return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryIDNotFound, qsbridge.PhaseExecute, "dictionary id not found: "+ref.QualifiedName()),
		}
	}
	return qsbridge.DictionaryEntry{Ref: ref, Label: label, ID: id}, nil
}

func (r *countingDictionaryResolver) LookupPrefix(ref qsbridge.DictionaryRef, prefix string) ([]qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	entries := make([]qsbridge.DictionaryEntry, 0)
	for id, label := range r.entries {
		if strings.HasPrefix(label, prefix) {
			entries = append(entries, qsbridge.DictionaryEntry{Ref: ref, Label: label, ID: id})
		}
	}
	return entries, nil
}
