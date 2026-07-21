package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// LegacyDirectSameRowBSIComparisonKernel compares same-row BSI fields through direct storage reads.
type LegacyDirectSameRowBSIComparisonKernel struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
	Reader     NativeProjectionBSIReader
	Comparator NativeSameRowBSIComparator
}

// CompareSameRowFields filters candidate rownums by comparing two BSI-backed fields in one rownum domain.
func (k LegacyDirectSameRowBSIComparisonKernel) CompareSameRowFields(ctx context.Context, request qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
	start := time.Now()
	result := qsbridge.SameRowComparisonResult{
		ID: request.ID,
		Domain: qsbridge.RownumDomainSet{
			Domain: request.Domain.Domain,
		},
		Probes: []qsbridge.ProjectionProbe{
			sameRowComparisonProbe(request, "input_count", strconv.Itoa(request.CandidateCount()), ""),
			sameRowComparisonProbe(request, "kind", string(request.Kind), ""),
			sameRowComparisonProbe(request, "operator", string(request.Operator), ""),
			sameRowComparisonProbe(request, "left_field", request.Left.QualifiedName(), ""),
			sameRowComparisonProbe(request, "right_field", request.Right.QualifiedName(), ""),
		},
	}
	if request.Kind != "" && request.Kind != qsbridge.SameRowComparisonBSI {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			fmt.Sprintf("same-row comparison does not support kind %q", request.Kind),
		))
		return result, nil
	}
	if !legacyDirectSameRowComparisonSameDomain(request) {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"same-row comparison requires one table/role rownum domain",
		))
		return result, nil
	}
	if len(request.Domain.Rownums) == 0 {
		result.Probes = append(result.Probes,
			sameRowComparisonProbe(request, "output_count", "0", ""),
			sameRowComparisonProbe(request, "elapsed", time.Since(start).String(), ""),
		)
		return result, nil
	}
	if k.Comparator != nil {
		compareStart := time.Now()
		compareRequest, compareDiagnostics := legacyDirectSameRowComparisonCompareRequest(request)
		result.Diagnostics = append(result.Diagnostics, compareDiagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, nil
		}
		compareResult, diagnostics, err := k.Comparator.CompareSameRowBSI(ctx, compareRequest)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		result.Probes = append(result.Probes, compareResult.Probes...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return result, err
		}
		result.Domain.Rownums = append(result.Domain.Rownums, compareResult.Rownums...)
		result.Probes = append(result.Probes,
			sameRowComparisonProbe(request, "strategy", "node_local_bsi_compare", ""),
			sameRowComparisonProbe(request, "compare_elapsed", time.Since(compareStart).String(), ""),
			sameRowComparisonProbe(request, "output_count", strconv.Itoa(result.CandidateCount()), ""),
			sameRowComparisonProbe(request, "rows_removed", strconv.Itoa(request.CandidateCount()-result.CandidateCount()), ""),
			sameRowComparisonProbe(request, "elapsed", time.Since(start).String(), ""),
		)
		return result, nil
	}
	reader := k.reader()
	if reader == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			"same-row comparison has no BSI reader",
		))
		return result, nil
	}
	leftStart := time.Now()
	left, leftDiagnostics, err := reader.ReadProjectionBSI(ctx, legacyDirectSameRowComparisonBSIReadRequest(request, request.Left))
	leftElapsed := time.Since(leftStart)
	result.Probes = append(result.Probes, sameRowComparisonProbe(request, "left_read_elapsed", leftElapsed.String(), request.Left.QualifiedName()))
	result.Probes = append(result.Probes, left.Probes...)
	result.Diagnostics = append(result.Diagnostics, leftDiagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	rightStart := time.Now()
	right, rightDiagnostics, err := reader.ReadProjectionBSI(ctx, legacyDirectSameRowComparisonBSIReadRequest(request, request.Right))
	rightElapsed := time.Since(rightStart)
	result.Probes = append(result.Probes, sameRowComparisonProbe(request, "right_read_elapsed", rightElapsed.String(), request.Right.QualifiedName()))
	result.Probes = append(result.Probes, right.Probes...)
	result.Diagnostics = append(result.Diagnostics, rightDiagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	if left.BSI == nil || right.BSI == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"same-row comparison requires both BSI projections",
		))
		return result, nil
	}
	compareOp, invert, ok := legacyDirectSameRowComparisonOperation(request.Operator)
	if !ok {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			fmt.Sprintf("same-row comparison does not support operator %q", request.Operator),
		))
		return result, nil
	}
	compareStart := time.Now()
	candidates := legacyDirectSameRowComparisonFoundSet(request.Domain.Rownums)
	comparisonUniverse := candidates.Clone()
	comparisonUniverse.And(left.BSI.GetExistenceBitmap())
	comparisonUniverse.And(right.BSI.GetExistenceBitmap())
	matches := left.BSI.CompareBSI(compareOp, right.BSI, comparisonUniverse)
	if invert {
		comparisonUniverse.AndNot(matches)
		matches = comparisonUniverse
	}
	for _, rownum := range request.Domain.Rownums {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if matches.Contains(uint64(rownum)) {
			result.Domain.Rownums = append(result.Domain.Rownums, rownum)
		}
	}
	result.Probes = append(result.Probes,
		sameRowComparisonProbe(request, "strategy", "bsi_bitwise", ""),
		sameRowComparisonProbe(request, "compare_elapsed", time.Since(compareStart).String(), ""),
		sameRowComparisonProbe(request, "output_count", strconv.Itoa(result.CandidateCount()), ""),
		sameRowComparisonProbe(request, "rows_removed", strconv.Itoa(request.CandidateCount()-result.CandidateCount()), ""),
		sameRowComparisonProbe(request, "elapsed", time.Since(start).String(), ""),
	)
	return result, nil
}

func legacyDirectSameRowComparisonCompareRequest(request qsbridge.SameRowComparisonRequest) (NativeSameRowBSICompareRequest, qsbridge.DiagnosticSet) {
	compareOp, invert, ok := legacyDirectSameRowComparisonOperation(request.Operator)
	if !ok {
		return NativeSameRowBSICompareRequest{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				fmt.Sprintf("same-row comparison does not support operator %q", request.Operator),
			),
		}
	}
	index := request.Left.Table.Table
	return NativeSameRowBSICompareRequest{
		Index:           index,
		ProbePrefix:     request.ProbePrefix,
		LeftField:       directBitmapFieldPhysicalName(request.Left),
		RightField:      directBitmapFieldPhysicalName(request.Right),
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Domain.Rownums...),
		Operation:       compareOp,
		Invert:          invert,
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}, nil
}

func (k LegacyDirectSameRowBSIComparisonKernel) reader() NativeProjectionBSIReader {
	if k.Reader != nil {
		return k.Reader
	}
	if k.Source == nil {
		return nil
	}
	return LegacyDirectProjectionBSIReader{Source: k.Source, TableCache: k.TableCache}
}

func legacyDirectSameRowComparisonSameDomain(request qsbridge.SameRowComparisonRequest) bool {
	leftTable := request.Left.Table.Table
	rightTable := request.Right.Table.Table
	if leftTable == "" || rightTable == "" || !strings.EqualFold(leftTable, rightTable) {
		return false
	}
	leftRole := materializationFieldRole(leftTable, request.Left)
	rightRole := materializationFieldRole(rightTable, request.Right)
	domain := request.Domain.Domain.Name()
	return strings.EqualFold(leftRole, rightRole) &&
		(domain == "" || strings.EqualFold(domain, leftRole))
}

func legacyDirectSameRowComparisonFoundSet(rownums []qsbridge.QuantaRownum) *roaring64.Bitmap {
	foundSet := roaring64.NewBitmap()
	for _, rownum := range rownums {
		foundSet.Add(uint64(rownum))
	}
	return foundSet
}

func legacyDirectSameRowComparisonBSIReadRequest(request qsbridge.SameRowComparisonRequest, field qsbridge.FieldRef) NativeProjectionBSIReadRequest {
	index := field.Table.Table
	return NativeProjectionBSIReadRequest{
		Index: index,
		Field: qsbridge.QuantaProjectionField{
			Index:        index,
			Field:        directBitmapFieldPhysicalName(field),
			Type:         field.Type,
			PhysicalName: field.PhysicalName,
			Role:         qsbridge.TableInstanceID(materializationFieldRole(index, field)),
			Roles:        field.Roles,
			Visible:      false,
		},
		PhysicalField:   directBitmapFieldPhysicalName(field),
		Rownums:         append([]qsbridge.QuantaRownum(nil), request.Domain.Rownums...),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
}

func legacyDirectSameRowComparisonOperation(op qsbridge.BinaryOp) (roaring64.Operation, bool, bool) {
	switch op {
	case qsbridge.BinaryOpEqual:
		return roaring64.EQ, false, true
	case qsbridge.BinaryOpNotEqual:
		return roaring64.EQ, true, true
	case qsbridge.BinaryOpLess:
		return roaring64.LT, false, true
	case qsbridge.BinaryOpLessEqual:
		return roaring64.LE, false, true
	case qsbridge.BinaryOpGreater:
		return roaring64.GT, false, true
	case qsbridge.BinaryOpGreaterEqual:
		return roaring64.GE, false, true
	default:
		return 0, false, false
	}
}

func legacyDirectSameRowComparisonSetValue(value int64) *big.Int {
	return big.NewInt(value)
}
