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
)

// LegacyDirectSameRowBSIComparisonKernel compares same-row BSI fields through legacy-direct storage reads.
type LegacyDirectSameRowBSIComparisonKernel struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
	Reader     NativeProjectionBSIReader
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
			fmt.Sprintf("legacy-direct same-row comparison does not support kind %q", request.Kind),
		))
		return result, nil
	}
	if !legacyDirectSameRowComparisonSameDomain(request) {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"legacy-direct same-row comparison requires one table/role rownum domain",
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
	reader := k.reader()
	if reader == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			"legacy-direct same-row comparison has no BSI reader",
		))
		return result, nil
	}
	left, leftDiagnostics, err := reader.ReadProjectionBSI(ctx, legacyDirectSameRowComparisonBSIReadRequest(request, request.Left))
	result.Probes = append(result.Probes, left.Probes...)
	result.Diagnostics = append(result.Diagnostics, leftDiagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	right, rightDiagnostics, err := reader.ReadProjectionBSI(ctx, legacyDirectSameRowComparisonBSIReadRequest(request, request.Right))
	result.Probes = append(result.Probes, right.Probes...)
	result.Diagnostics = append(result.Diagnostics, rightDiagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, err
	}
	if left.BSI == nil || right.BSI == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"legacy-direct same-row comparison requires both BSI projections",
		))
		return result, nil
	}
	for _, rownum := range request.Domain.Rownums {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		leftValue, leftOK := left.BSI.GetBigValue(uint64(rownum))
		rightValue, rightOK := right.BSI.GetBigValue(uint64(rownum))
		if !leftOK || !rightOK || leftValue == nil || rightValue == nil {
			continue
		}
		matched, ok := legacyDirectSameRowComparisonMatch(request.Operator, leftValue.Cmp(rightValue))
		if !ok {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				fmt.Sprintf("legacy-direct same-row comparison does not support operator %q", request.Operator),
			))
			return result, nil
		}
		if matched {
			result.Domain.Rownums = append(result.Domain.Rownums, rownum)
		}
	}
	result.Probes = append(result.Probes,
		sameRowComparisonProbe(request, "output_count", strconv.Itoa(result.CandidateCount()), ""),
		sameRowComparisonProbe(request, "rows_removed", strconv.Itoa(request.CandidateCount()-result.CandidateCount()), ""),
		sameRowComparisonProbe(request, "elapsed", time.Since(start).String(), ""),
	)
	return result, nil
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

func legacyDirectSameRowComparisonMatch(op qsbridge.BinaryOp, compare int) (bool, bool) {
	switch op {
	case qsbridge.BinaryOpEqual:
		return compare == 0, true
	case qsbridge.BinaryOpNotEqual:
		return compare != 0, true
	case qsbridge.BinaryOpLess:
		return compare < 0, true
	case qsbridge.BinaryOpLessEqual:
		return compare <= 0, true
	case qsbridge.BinaryOpGreater:
		return compare > 0, true
	case qsbridge.BinaryOpGreaterEqual:
		return compare >= 0, true
	default:
		return false, false
	}
}

func legacyDirectSameRowComparisonSetValue(value int64) *big.Int {
	return big.NewInt(value)
}
