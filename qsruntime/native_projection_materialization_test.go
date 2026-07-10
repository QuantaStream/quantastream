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
	if len(result.Probes) != 2 || result.Probes[1].Name != "field_read" {
		t.Fatalf("probes = %#v, want request-count and field-read probes", result.Probes)
	}
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
	if len(result.Probes) < 2 || result.Probes[1].Name != "dictionary_resolver_rehydration_values" {
		t.Fatalf("probes = %#v, want dictionary resolver rehydration probe", result.Probes)
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
