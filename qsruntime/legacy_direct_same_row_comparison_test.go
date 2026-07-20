package qsruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectSameRowBSIComparisonKernelFiltersRownumsWithoutMaterialization(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	leftBSI := roaring64.NewBSI(0, 64)
	rightBSI := roaring64.NewBSI(0, 64)
	leftBSI.SetValue(1, 10)
	leftBSI.SetValue(2, 30)
	leftBSI.SetValue(3, 40)
	rightBSI.SetValue(1, 20)
	rightBSI.SetValue(2, 20)
	rightBSI.SetValue(3, 40)

	requests := []NativeProjectionBSIReadRequest{}
	kernel := LegacyDirectSameRowBSIComparisonKernel{
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			requests = append(requests, request)
			switch request.PhysicalField {
			case "l_receiptdate":
				return NativeProjectionBSIReadResult{BSI: leftBSI}, nil, nil
			case "l_commitdate":
				return NativeProjectionBSIReadResult{BSI: rightBSI}, nil, nil
			default:
				t.Fatalf("unexpected physical field %q", request.PhysicalField)
				return NativeProjectionBSIReadResult{}, nil, nil
			}
		}),
	}

	result, err := kernel.CompareSameRowFields(context.Background(), qsbridge.SameRowComparisonRequest{
		ID:          "q21_late_receipt",
		ProbePrefix: "q21_late_receipt_",
		Domain: qsbridge.RownumDomainSet{
			Domain:  qsbridge.RownumDomain{Table: lineitem, Role: "l"},
			Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4},
		},
		Left:     qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Right:    qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Operator: qsbridge.BinaryOpGreater,
		Kind:     qsbridge.SameRowComparisonBSI,
	})
	if err != nil {
		t.Fatalf("CompareSameRowFields: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got, want := result.Domain.Rownums, []qsbridge.QuantaRownum{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rownums = %#v, want %#v", got, want)
	}
	if len(requests) != 2 {
		t.Fatalf("reader calls = %d, want 2", len(requests))
	}
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_late_receipt_input_count", "4")
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_late_receipt_strategy", "bsi_bitwise")
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_late_receipt_output_count", "1")
}

func TestLegacyDirectSameRowBSIComparisonKernelUsesComparatorWhenMounted(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	readerCalled := false
	var compareRequest NativeSameRowBSICompareRequest
	kernel := LegacyDirectSameRowBSIComparisonKernel{
		Reader: NativeProjectionBSIReaderFunc(func(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			readerCalled = true
			return NativeProjectionBSIReadResult{}, nil, nil
		}),
		Comparator: NativeSameRowBSIComparatorFunc(func(_ context.Context, request NativeSameRowBSICompareRequest) (NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error) {
			compareRequest = request
			return NativeSameRowBSICompareResult{
				Rownums: []qsbridge.QuantaRownum{2},
				Probes: []ExecutionProbe{{
					Section: "same_row_comparison",
					Name:    "mounted_comparator",
					Value:   "called",
				}},
			}, nil, nil
		}),
	}

	result, err := kernel.CompareSameRowFields(context.Background(), qsbridge.SameRowComparisonRequest{
		ID:          "q21_late_receipt",
		ProbePrefix: "q21_late_receipt_",
		Domain: qsbridge.RownumDomainSet{
			Domain:  qsbridge.RownumDomain{Table: lineitem, Role: "l"},
			Rownums: []qsbridge.QuantaRownum{1, 2, 3},
		},
		Left:     qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Right:    qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime},
		Operator: qsbridge.BinaryOpGreater,
		Kind:     qsbridge.SameRowComparisonBSI,
	})
	if err != nil {
		t.Fatalf("CompareSameRowFields: %v", err)
	}
	if readerCalled {
		t.Fatalf("projection reader was called despite mounted comparator")
	}
	if got, want := result.Domain.Rownums, []qsbridge.QuantaRownum{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rownums = %#v, want %#v", got, want)
	}
	if compareRequest.LeftField != "l_receiptdate" || compareRequest.RightField != "l_commitdate" {
		t.Fatalf("compare fields = %s/%s", compareRequest.LeftField, compareRequest.RightField)
	}
	if compareRequest.Operation != roaring64.GT || compareRequest.Invert {
		t.Fatalf("operation = %v invert=%v, want GT false", compareRequest.Operation, compareRequest.Invert)
	}
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "mounted_comparator", "called")
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_late_receipt_strategy", "node_local_bsi_compare")
}

func TestLegacyDirectSameRowBSIComparisonKernelUsesSharedExistenceForNotEqual(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	leftBSI := roaring64.NewDefaultBSI()
	rightBSI := roaring64.NewDefaultBSI()
	leftBSI.SetValue(1, 10)
	rightBSI.SetValue(1, 10)
	leftBSI.SetValue(2, 11)
	rightBSI.SetValue(2, 12)
	leftBSI.SetValue(3, 13)
	rightBSI.SetValue(4, 14)

	kernel := LegacyDirectSameRowBSIComparisonKernel{
		Reader: NativeProjectionBSIReaderFunc(func(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
			switch request.PhysicalField {
			case "l_suppkey":
				return NativeProjectionBSIReadResult{BSI: leftBSI}, nil, nil
			case "l_other_suppkey":
				return NativeProjectionBSIReadResult{BSI: rightBSI}, nil, nil
			default:
				t.Fatalf("unexpected physical field %q", request.PhysicalField)
				return NativeProjectionBSIReadResult{}, nil, nil
			}
		}),
	}

	result, err := kernel.CompareSameRowFields(context.Background(), qsbridge.SameRowComparisonRequest{
		ID:          "q21_other_supplier",
		ProbePrefix: "q21_other_supplier_",
		Domain: qsbridge.RownumDomainSet{
			Domain:  qsbridge.RownumDomain{Table: lineitem, Role: "l"},
			Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4},
		},
		Left:     qsbridge.FieldRef{Table: lineitem, Name: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
		Right:    qsbridge.FieldRef{Table: lineitem, Name: "l_other_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
		Operator: qsbridge.BinaryOpNotEqual,
		Kind:     qsbridge.SameRowComparisonBSI,
	})
	if err != nil {
		t.Fatalf("CompareSameRowFields: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got, want := result.Domain.Rownums, []qsbridge.QuantaRownum{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rownums = %#v, want %#v", got, want)
	}
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_other_supplier_strategy", "bsi_bitwise")
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "q21_other_supplier_output_count", "1")
}

func TestUnsupportedSameRowComparisonKernelReportsBoundary(t *testing.T) {
	result, err := UnsupportedSameRowComparisonKernel{}.CompareSameRowFields(context.Background(), qsbridge.SameRowComparisonRequest{
		ID:          "unsupported",
		ProbePrefix: "unsupported_",
		Kind:        qsbridge.SameRowComparisonBSI,
	})
	if err != nil {
		t.Fatalf("CompareSameRowFields: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "unsupported_kernel", "unsupported")
}
