package qsruntime

import (
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ProjectionMaterializationCapabilityStatus identifies how one field was or would be materialized.
type ProjectionMaterializationCapabilityStatus string

const (
	// ProjectionMaterializationCapabilityUnknown means no capability has been classified.
	ProjectionMaterializationCapabilityUnknown ProjectionMaterializationCapabilityStatus = ""
	// ProjectionMaterializationCapabilityNative means the field is expected to use native projection materialization.
	ProjectionMaterializationCapabilityNative ProjectionMaterializationCapabilityStatus = "native"
	// ProjectionMaterializationCapabilityCompatFallback means the field currently requires compatibility materialization.
	ProjectionMaterializationCapabilityCompatFallback ProjectionMaterializationCapabilityStatus = "compat_fallback"
)

// ProjectionMaterializationCapabilityReason identifies the stable cause behind a materialization choice.
type ProjectionMaterializationCapabilityReason string

const (
	// ProjectionMaterializationReasonUnknown means no stable materialization reason has been assigned.
	ProjectionMaterializationReasonUnknown ProjectionMaterializationCapabilityReason = ""
	// ProjectionMaterializationReasonNativeInlineBSI means BSI state can be projected without side lookup.
	ProjectionMaterializationReasonNativeInlineBSI ProjectionMaterializationCapabilityReason = "native_inline_bsi"
	// ProjectionMaterializationReasonNativeDictionary means StringEnum ids can be rehydrated through dictionary metadata.
	ProjectionMaterializationReasonNativeDictionary ProjectionMaterializationCapabilityReason = "native_dictionary"
	// ProjectionMaterializationReasonNativeBackingString means backing-string ids can be rehydrated through KV/cache lookup.
	ProjectionMaterializationReasonNativeBackingString ProjectionMaterializationCapabilityReason = "native_backing_string_kvstore"
	// ProjectionMaterializationReasonBackingStringKV means StringLexBSI/backing-string fields still need KV/cache rehydration.
	ProjectionMaterializationReasonBackingStringKV ProjectionMaterializationCapabilityReason = "backing_string_kvstore_needed"
	// ProjectionMaterializationReasonUnsupportedEncoding means the encoding is not yet natively materializable.
	ProjectionMaterializationReasonUnsupportedEncoding ProjectionMaterializationCapabilityReason = "unsupported_encoding"
	// ProjectionMaterializationReasonRuntimeFallback means the native path reported an unsupported runtime fallback.
	ProjectionMaterializationReasonRuntimeFallback ProjectionMaterializationCapabilityReason = "runtime_fallback"
)

// ProjectionMaterializationFieldCapability describes one projection field's materialization path.
type ProjectionMaterializationFieldCapability struct {
	Index      string
	Field      string
	Type       qsbridge.DataType
	Encoding   qsbridge.EncodingKind
	Status     ProjectionMaterializationCapabilityStatus
	LookupKind NativeProjectionLookupKind
	Source     string
	ReasonCode ProjectionMaterializationCapabilityReason
	Reason     string
}

// ProjectionMaterializationCapabilityReport summarizes native vs compatibility materialization.
type ProjectionMaterializationCapabilityReport struct {
	Fields                      []ProjectionMaterializationFieldCapability
	NativeFieldCount            int
	CompatFallbackFieldCount    int
	RuntimeFallbackObserved     bool
	FallbackDiagnosticCount     int
	LegacyMaterializerReachable bool
	LegacyMaterializerUsed      bool
}

// ProjectionMaterializationCapabilityReportForRequest classifies fields and runtime fallback probes.
func ProjectionMaterializationCapabilityReportForRequest(request qsbridge.ProjectionMaterializationKernelRequest, result qsbridge.ProjectionMaterializationKernelResult) ProjectionMaterializationCapabilityReport {
	return ProjectionMaterializationCapabilityReportForRequestWithCatalog(request, result, qsbridge.QueryCatalogView{})
}

// ProjectionMaterializationCapabilityReportForRequestWithCatalog classifies fields using query-facing catalog metadata.
func ProjectionMaterializationCapabilityReportForRequestWithCatalog(request qsbridge.ProjectionMaterializationKernelRequest, result qsbridge.ProjectionMaterializationKernelResult, catalog qsbridge.QueryCatalogView) ProjectionMaterializationCapabilityReport {
	report := ProjectionMaterializationCapabilityReport{}
	report.RuntimeFallbackObserved, report.FallbackDiagnosticCount = projectionMaterializationFallbackObserved(result.Probes)
	runtimeStatus := ProjectionMaterializationCapabilityUnknown
	runtimeReasonCode := ProjectionMaterializationReasonUnknown
	runtimeReason := ""
	if report.RuntimeFallbackObserved {
		runtimeStatus = ProjectionMaterializationCapabilityCompatFallback
		runtimeReasonCode = ProjectionMaterializationReasonRuntimeFallback
		runtimeReason = "runtime fallback probe observed"
	}
	for _, materializationRequest := range request.Requests {
		for _, field := range materializationRequest.ProjectionFields {
			capability := projectionMaterializationFieldCapability(materializationRequest.Index, field, catalog)
			if runtimeStatus != ProjectionMaterializationCapabilityUnknown {
				capability.Status = runtimeStatus
				capability.Source = "compat_materializer"
				capability.ReasonCode = runtimeReasonCode
				capability.Reason = runtimeReason
			}
			report.Fields = append(report.Fields, capability)
			switch capability.Status {
			case ProjectionMaterializationCapabilityNative:
				report.NativeFieldCount++
			case ProjectionMaterializationCapabilityCompatFallback:
				report.CompatFallbackFieldCount++
			}
		}
	}
	report.LegacyMaterializerUsed = report.RuntimeFallbackObserved
	report.LegacyMaterializerReachable = report.CompatFallbackFieldCount > 0 || report.LegacyMaterializerUsed
	return report
}

// ProjectionMaterializationSelectionPlan splits materialization requests by native-vs-compat capability.
type ProjectionMaterializationSelectionPlan struct {
	Report ProjectionMaterializationCapabilityReport
	Native qsbridge.ProjectionMaterializationKernelRequest
	Compat qsbridge.ProjectionMaterializationKernelRequest
}

// HasNativeWork reports whether any fields can use native materialization.
func (p ProjectionMaterializationSelectionPlan) HasNativeWork() bool {
	return p.Native.RequestCount() > 0
}

// HasCompatWork reports whether any fields need compatibility materialization.
func (p ProjectionMaterializationSelectionPlan) HasCompatWork() bool {
	return p.Compat.RequestCount() > 0
}

// SplitRequired reports whether the original request contains both native and compat fields.
func (p ProjectionMaterializationSelectionPlan) SplitRequired() bool {
	return p.HasNativeWork() && p.HasCompatWork()
}

// ProjectionMaterializationSelectionPlanForRequest chooses native or compatibility materialization per field.
func ProjectionMaterializationSelectionPlanForRequest(request qsbridge.ProjectionMaterializationKernelRequest, catalog qsbridge.QueryCatalogView) ProjectionMaterializationSelectionPlan {
	plan := ProjectionMaterializationSelectionPlan{
		Report: ProjectionMaterializationCapabilityReportForRequestWithCatalog(request, qsbridge.ProjectionMaterializationKernelResult{}, catalog),
		Native: qsbridge.ProjectionMaterializationKernelRequest{
			ID:          request.ID,
			ProbePrefix: request.ProbePrefix,
		},
		Compat: qsbridge.ProjectionMaterializationKernelRequest{
			ID:          request.ID,
			ProbePrefix: request.ProbePrefix,
		},
	}
	capabilityIndex := 0
	for _, materializationRequest := range request.Requests {
		nativeRequest := cloneMaterializationRequestWithoutProjectionFields(materializationRequest)
		compatRequest := cloneMaterializationRequestWithoutProjectionFields(materializationRequest)
		for _, field := range materializationRequest.ProjectionFields {
			if capabilityIndex >= len(plan.Report.Fields) {
				break
			}
			switch plan.Report.Fields[capabilityIndex].Status {
			case ProjectionMaterializationCapabilityNative:
				nativeRequest.ProjectionFields = append(nativeRequest.ProjectionFields, field)
			default:
				compatRequest.ProjectionFields = append(compatRequest.ProjectionFields, field)
			}
			capabilityIndex++
		}
		if nativeRequest.ProjectionCount() > 0 {
			plan.Native.Requests = append(plan.Native.Requests, nativeRequest)
		}
		if compatRequest.ProjectionCount() > 0 {
			plan.Compat.Requests = append(plan.Compat.Requests, compatRequest)
		}
	}
	return plan
}

func cloneMaterializationRequestWithoutProjectionFields(request qsbridge.QuantaMaterializationRequest) qsbridge.QuantaMaterializationRequest {
	cloned := request
	cloned.Rownums = append([]qsbridge.QuantaRownum(nil), request.Rownums...)
	cloned.ProjectionFields = nil
	return cloned
}

func projectionMaterializationFieldCapability(defaultIndex string, field qsbridge.QuantaProjectionField, catalog qsbridge.QueryCatalogView) ProjectionMaterializationFieldCapability {
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	index := field.Index
	if index == "" {
		index = defaultIndex
	}
	capability := ProjectionMaterializationFieldCapability{
		Index: index,
		Field: name,
		Type:  field.Type,
	}
	if definition, ok := projectionMaterializationFieldDefinition(catalog, index, name); ok {
		capability.Encoding = definition.Encoding.Kind
	}
	switch field.Type {
	case qsbridge.DataTypeInt, qsbridge.DataTypeFloat, qsbridge.DataTypeTime, qsbridge.DataTypeBool:
		capability.Status = ProjectionMaterializationCapabilityNative
		capability.Source = "native_bsi"
		capability.ReasonCode = ProjectionMaterializationReasonNativeInlineBSI
		capability.Reason = "scalar BSI-compatible projection"
	case qsbridge.DataTypeString:
		if _, ok := nativeProjectionDictionaryRefFromCatalog(catalog, index, field); ok {
			capability.Status = ProjectionMaterializationCapabilityNative
			capability.Encoding = qsbridge.EncodingStringEnum
			capability.LookupKind = NativeProjectionLookupDictionary
			capability.Source = "dictionary_resolver"
			capability.ReasonCode = ProjectionMaterializationReasonNativeDictionary
			capability.Reason = "StringEnum dictionary label rehydration available through query catalog"
		} else if capability.Encoding == qsbridge.EncodingBackingString {
			capability.Status = ProjectionMaterializationCapabilityNative
			capability.LookupKind = NativeProjectionLookupBackingString
			capability.Source = "backing_string_lookup_reader"
			capability.ReasonCode = ProjectionMaterializationReasonNativeBackingString
			capability.Reason = "backing-string KVStore/cache rehydration available through native lookup reader"
		} else {
			capability.Status = ProjectionMaterializationCapabilityCompatFallback
			if capability.Encoding == "" {
				capability.Encoding = qsbridge.EncodingBackingString
			}
			capability.LookupKind = NativeProjectionLookupBackingString
			capability.Source = "kvstore_needed"
			capability.ReasonCode = ProjectionMaterializationReasonBackingStringKV
			capability.Reason = "backing-string lookup requires KVStore/cache-backed rehydration"
		}
	default:
		capability.Status = ProjectionMaterializationCapabilityCompatFallback
		capability.Source = "compat_materializer"
		capability.ReasonCode = ProjectionMaterializationReasonUnsupportedEncoding
		capability.Reason = "unknown projection type"
	}
	return capability
}

func projectionMaterializationFieldDefinition(catalog qsbridge.QueryCatalogView, index string, field string) (qsbridge.FieldDefinition, bool) {
	for _, table := range catalog.Tables {
		if !strings.EqualFold(table.Name, index) {
			continue
		}
		for _, definition := range table.Fields {
			if strings.EqualFold(definition.Name, field) || (definition.PhysicalName != "" && strings.EqualFold(definition.PhysicalName, field)) {
				return definition, true
			}
		}
	}
	return qsbridge.FieldDefinition{}, false
}

func projectionMaterializationFallbackObserved(probes []qsbridge.ProjectionProbe) (bool, int) {
	observed := false
	diagnosticCount := 0
	for _, probe := range probes {
		switch {
		case strings.HasSuffix(probe.Name, "fallback_to_compat") && probe.Value == "true":
			observed = true
		case strings.HasSuffix(probe.Name, "fallback_diagnostic_count"):
			count, err := strconv.Atoi(probe.Value)
			if err == nil {
				diagnosticCount += count
			}
		}
	}
	return observed, diagnosticCount
}
