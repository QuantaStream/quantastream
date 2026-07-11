package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestProjectionMaterializationCapabilityReportClassifiesFieldTypes(t *testing.T) {
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index: "orders",
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Field: "o_orderkey", Type: qsbridge.DataTypeInt},
				{Field: "o_orderpriority", Type: qsbridge.DataTypeString},
			},
		}},
	}

	report := ProjectionMaterializationCapabilityReportForRequest(request, qsbridge.ProjectionMaterializationKernelResult{})

	if report.NativeFieldCount != 1 || report.CompatFallbackFieldCount != 1 {
		t.Fatalf("report = %#v, want one native and one compat fallback field", report)
	}
	if !report.LegacyMaterializerReachable || report.LegacyMaterializerUsed {
		t.Fatalf("legacy materializer visibility = reachable %v used %v, want reachable without observed use", report.LegacyMaterializerReachable, report.LegacyMaterializerUsed)
	}
	if report.Fields[0].Status != ProjectionMaterializationCapabilityNative ||
		report.Fields[1].Status != ProjectionMaterializationCapabilityCompatFallback {
		t.Fatalf("fields = %#v, want native int and compat string", report.Fields)
	}
	if report.Fields[0].Source != "native_bsi" || report.Fields[0].ReasonCode != ProjectionMaterializationReasonNativeInlineBSI {
		t.Fatalf("int capability = %#v, want native BSI reason", report.Fields[0])
	}
	if report.Fields[1].LookupKind != NativeProjectionLookupBackingString {
		t.Fatalf("string lookup kind = %q, want backing string boundary", report.Fields[1].LookupKind)
	}
	if report.Fields[1].Source != "kvstore_needed" || report.Fields[1].ReasonCode != ProjectionMaterializationReasonBackingStringKV {
		t.Fatalf("string capability = %#v, want KVStore backing-string fallback", report.Fields[1])
	}
	if report.Fields[1].Reason != "backing-string lookup requires KVStore/cache-backed rehydration" {
		t.Fatalf("string reason = %q, want backing-string boundary reason", report.Fields[1].Reason)
	}
	if report.RuntimeFallbackObserved {
		t.Fatalf("runtime fallback observed = true, want false for static report")
	}
}

func TestProjectionMaterializationCapabilityReportUsesCatalogDictionaryForStringEnum(t *testing.T) {
	dictionaryRef := qsbridge.DictionaryRef{Table: "orders", Field: "o_orderpriority"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "orders",
		Fields: []qsbridge.FieldDefinition{{
			Name: "o_orderpriority",
			Type: qsbridge.DataTypeString,
			Dictionary: qsbridge.DictionaryDefinition{
				Ref: dictionaryRef,
			},
		}},
	}}, nil, nil)
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index: "orders",
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "orders", Field: "o_orderpriority", Type: qsbridge.DataTypeString},
			},
		}},
	}

	report := ProjectionMaterializationCapabilityReportForRequestWithCatalog(request, qsbridge.ProjectionMaterializationKernelResult{}, catalog)

	if report.NativeFieldCount != 1 || report.CompatFallbackFieldCount != 0 {
		t.Fatalf("report = %#v, want dictionary string to be native", report)
	}
	if report.LegacyMaterializerReachable || report.LegacyMaterializerUsed {
		t.Fatalf("legacy materializer visibility = reachable %v used %v, want no legacy materializer path for native dictionary", report.LegacyMaterializerReachable, report.LegacyMaterializerUsed)
	}
	if report.Fields[0].LookupKind != NativeProjectionLookupDictionary {
		t.Fatalf("lookup kind = %q, want dictionary", report.Fields[0].LookupKind)
	}
	if report.Fields[0].Encoding != qsbridge.EncodingStringEnum ||
		report.Fields[0].Source != "dictionary_resolver" ||
		report.Fields[0].ReasonCode != ProjectionMaterializationReasonNativeDictionary {
		t.Fatalf("dictionary capability = %#v, want native StringEnum dictionary path", report.Fields[0])
	}
}

func TestProjectionMaterializationCapabilityReportUsesCatalogBackingStringForNativeKVRehydration(t *testing.T) {
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "customer",
		Fields: []qsbridge.FieldDefinition{{
			Name: "c_name",
			Type: qsbridge.DataTypeString,
			Encoding: qsbridge.EncodingProfile{
				Kind: qsbridge.EncodingBackingString,
			},
		}},
	}}, nil, nil)
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index: "customer",
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "customer", Field: "c_name", Type: qsbridge.DataTypeString},
			},
		}},
	}

	report := ProjectionMaterializationCapabilityReportForRequestWithCatalog(request, qsbridge.ProjectionMaterializationKernelResult{}, catalog)

	if report.NativeFieldCount != 1 || report.CompatFallbackFieldCount != 0 {
		t.Fatalf("report = %#v, want backing string to be native", report)
	}
	if report.LegacyMaterializerReachable || report.LegacyMaterializerUsed {
		t.Fatalf("legacy materializer visibility = reachable %v used %v, want no compatibility materializer path", report.LegacyMaterializerReachable, report.LegacyMaterializerUsed)
	}
	field := report.Fields[0]
	if field.Encoding != qsbridge.EncodingBackingString || field.LookupKind != NativeProjectionLookupBackingString {
		t.Fatalf("field capability = %#v, want backing-string lookup", field)
	}
	if field.Source != "backing_string_lookup_reader" || field.ReasonCode != ProjectionMaterializationReasonNativeBackingString {
		t.Fatalf("field capability = %#v, want native backing-string KV/cache reason", field)
	}
}
func TestProjectionMaterializationSelectionPlanSplitsNativeAndCompatFields(t *testing.T) {
	dictionaryRef := qsbridge.DictionaryRef{Table: "orders", Field: "o_orderpriority"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "orders",
		Fields: []qsbridge.FieldDefinition{{
			Name: "o_orderpriority",
			Type: qsbridge.DataTypeString,
			Dictionary: qsbridge.DictionaryDefinition{
				Ref: dictionaryRef,
			},
		}},
	}}, nil, nil)
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{1, 2},
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt},
				{Index: "orders", Field: "o_orderpriority", Type: qsbridge.DataTypeString},
				{Index: "orders", Field: "o_comment", Type: qsbridge.DataTypeString},
			},
		}},
	}

	plan := ProjectionMaterializationSelectionPlanForRequest(request, catalog)

	if !plan.SplitRequired() {
		t.Fatalf("plan = %#v, want split native/compat work", plan)
	}
	if plan.Native.RequestCount() != 1 || plan.Compat.RequestCount() != 1 {
		t.Fatalf("plan request counts = native %d compat %d, want 1/1", plan.Native.RequestCount(), plan.Compat.RequestCount())
	}
	if got := plan.Native.Requests[0].ProjectionCount(); got != 2 {
		t.Fatalf("native projection count = %d, want int plus dictionary string", got)
	}
	if got := plan.Compat.Requests[0].ProjectionCount(); got != 1 {
		t.Fatalf("compat projection count = %d, want backing string only", got)
	}
	if plan.Compat.Requests[0].ProjectionFields[0].Field != "o_comment" {
		t.Fatalf("compat field = %#v, want o_comment", plan.Compat.Requests[0].ProjectionFields)
	}
}

func TestProjectionMaterializationCapabilityReportUsesRuntimeFallbackProbesAsTruth(t *testing.T) {
	request := qsbridge.ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index: "orders",
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Field: "o_orderkey", Type: qsbridge.DataTypeInt},
			},
		}},
	}
	result := qsbridge.ProjectionMaterializationKernelResult{
		Probes: []qsbridge.ProjectionProbe{
			{Name: "projection_materialization_fallback_to_compat", Value: "true"},
			{Name: "projection_materialization_fallback_diagnostic_count", Value: "2"},
		},
	}

	report := ProjectionMaterializationCapabilityReportForRequest(request, result)

	if !report.RuntimeFallbackObserved || report.FallbackDiagnosticCount != 2 {
		t.Fatalf("report = %#v, want runtime fallback with two diagnostics", report)
	}
	if !report.LegacyMaterializerReachable || !report.LegacyMaterializerUsed {
		t.Fatalf("legacy materializer visibility = reachable %v used %v, want observed compat use", report.LegacyMaterializerReachable, report.LegacyMaterializerUsed)
	}
	if report.NativeFieldCount != 0 || report.CompatFallbackFieldCount != 1 ||
		report.Fields[0].Status != ProjectionMaterializationCapabilityCompatFallback {
		t.Fatalf("fields = %#v, want runtime fallback to override static native classification", report.Fields)
	}
	if report.Fields[0].Source != "compat_materializer" || report.Fields[0].ReasonCode != ProjectionMaterializationReasonRuntimeFallback {
		t.Fatalf("runtime fallback field = %#v, want compat materializer reason", report.Fields[0])
	}
}
