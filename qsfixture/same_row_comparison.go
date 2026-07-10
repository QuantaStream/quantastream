package qsfixture

import (
	"context"
	"fmt"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// SameRowComparisonFixtureKernel evaluates same-row comparison requests against in-memory rows.
//
// The fixture models the native BSI comparison contract: it accepts candidate
// rownums, compares aligned field values in the same rownum domain, and returns
// surviving rownums without producing projection vectors.
type SameRowComparisonFixtureKernel struct {
	rows map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell
}

var _ qsbridge.SameRowComparisonKernel = SameRowComparisonFixtureKernel{}

// NewSameRowComparisonFixtureKernel creates an immutable fixture kernel from rownum-keyed cells.
func NewSameRowComparisonFixtureKernel(rows map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell) SameRowComparisonFixtureKernel {
	copied := make(map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell, len(rows))
	for rownum, row := range rows {
		copiedRow := make(map[string]qsbridge.ResultCell, len(row))
		for field, cell := range row {
			copiedRow[field] = cell
		}
		copied[rownum] = copiedRow
	}
	return SameRowComparisonFixtureKernel{rows: copied}
}

// CompareSameRowFields filters candidate rownums by comparing two same-row fields.
func (k SameRowComparisonFixtureKernel) CompareSameRowFields(ctx context.Context, request qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
	result := qsbridge.SameRowComparisonResult{
		ID: request.ID,
		Domain: qsbridge.RownumDomainSet{
			Domain: request.Domain.Domain,
		},
		Probes: []qsbridge.ProjectionProbe{
			{
				Section: "same_row_comparison",
				Name:    request.ProbePrefix + "input_count",
				Value:   strconv.Itoa(request.CandidateCount()),
			},
		},
	}
	if request.Kind != "" && request.Kind != qsbridge.SameRowComparisonBSI {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedPredicate,
			qsbridge.PhaseExecute,
			fmt.Sprintf("runtime fixture same-row comparison does not support kind %q", request.Kind),
		))
		return result, nil
	}
	for _, rownum := range request.Domain.Rownums {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		row, ok := k.rows[rownum]
		if !ok {
			continue
		}
		left, leftOK := runtimeFixtureFieldCell(runtimeFixtureRow(row), request.Left)
		right, rightOK := runtimeFixtureFieldCell(runtimeFixtureRow(row), request.Right)
		if !leftOK || !rightOK || left.Kind == qsbridge.ValueNull || right.Kind == qsbridge.ValueNull {
			continue
		}
		matched, ok := runtimeFixtureSameRowComparisonMatch(request.Operator, runtimeFixtureCellCompare(left, right))
		if !ok {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedPredicate,
				qsbridge.PhaseExecute,
				fmt.Sprintf("runtime fixture same-row comparison does not support operator %q", request.Operator),
			))
			return result, nil
		}
		if matched {
			result.Domain.Rownums = append(result.Domain.Rownums, rownum)
		}
	}
	result.Probes = append(result.Probes, qsbridge.ProjectionProbe{
		Section: "same_row_comparison",
		Name:    request.ProbePrefix + "output_count",
		Value:   strconv.Itoa(result.CandidateCount()),
	})
	return result, nil
}

func runtimeFixtureSameRowComparisonMatch(op qsbridge.BinaryOp, compare int) (bool, bool) {
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
