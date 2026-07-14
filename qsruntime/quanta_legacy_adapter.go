package qsruntime

import (
	"fmt"
	"math/big"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/qsbridge"
	legacy "github.com/QuantaStream/quantastream/shared"
)

// LegacyBitmapQueryAdapter converts qsbridge's neutral Quanta dialect to legacy runtime payloads.
//
// qsbridge intentionally does not import the legacy shared or gRPC packages.
// This package is the first runtime-side adapter that is allowed to bridge
// those worlds.
type LegacyBitmapQueryAdapter struct{}

// ValidateExecutionRequest validates a lowered request before legacy bitmap structs are built.
func (a LegacyBitmapQueryAdapter) ValidateExecutionRequest(request ExecutionRequest) qsbridge.DiagnosticSet {
	return a.ValidateIntermediateQuery(request.Query)
}

// ValidateIntermediateQuery reports adapter-shape blockers before legacy conversion.
func (a LegacyBitmapQueryAdapter) ValidateIntermediateQuery(query qsbridge.QuantaIntermediateQuery) qsbridge.DiagnosticSet {
	var diagnostics qsbridge.DiagnosticSet
	for i, fragment := range query.Fragments {
		diagnostics = append(diagnostics, a.validateFragment(i, fragment)...)
	}
	return diagnostics
}

// ToBitmapQuery converts a neutral Quanta intermediate query to legacy shared.BitmapQuery.
func (a LegacyBitmapQueryAdapter) ToBitmapQuery(query qsbridge.QuantaIntermediateQuery) *legacy.BitmapQuery {
	bitmap := legacy.NewBitmapQuery()
	for _, fragment := range query.Fragments {
		bitmap.AddFragment(a.toQueryFragment(bitmap, fragment))
	}
	return bitmap
}

// ToBitmapQueryFromRequest converts a neutral runtime execution request to legacy shared.BitmapQuery.
func (a LegacyBitmapQueryAdapter) ToBitmapQueryFromRequest(request ExecutionRequest) *legacy.BitmapQuery {
	query := a.ToBitmapQuery(request.Query)
	a.applyDefaultTimeWindow(query)
	a.applyTimeWindow(query, request)
	return query
}

// ToProto converts a neutral Quanta intermediate query to the legacy gRPC bitmap payload.
func (a LegacyBitmapQueryAdapter) ToProto(query qsbridge.QuantaIntermediateQuery) *pb.BitmapQuery {
	return a.ToBitmapQuery(query).ToProto()
}

// ToProtoFromRequest converts a neutral runtime execution request to the legacy gRPC bitmap payload.
func (a LegacyBitmapQueryAdapter) ToProtoFromRequest(request ExecutionRequest) *pb.BitmapQuery {
	return a.ToBitmapQueryFromRequest(request).ToProto()
}

func (a LegacyBitmapQueryAdapter) toQueryFragment(bitmap *legacy.BitmapQuery, fragment qsbridge.QuantaQueryFragment) *legacy.QueryFragment {
	legacyFragment := bitmap.NewQueryFragment()
	legacyFragment.Index = fragment.Index
	legacyFragment.Field = fragment.Field
	legacyFragment.Operation = string(fragment.Operation)
	legacyFragment.BSIOp = string(fragment.BSIOp)
	legacyFragment.Value = cloneBigInt(fragment.Value)
	legacyFragment.Values = cloneBigIntSlice(fragment.Values)
	legacyFragment.Begin = cloneBigInt(fragment.Begin)
	legacyFragment.End = cloneBigInt(fragment.End)
	legacyFragment.Negate = fragment.Negate
	legacyFragment.NullCheck = fragment.NullCheck
	return legacyFragment
}

func (a LegacyBitmapQueryAdapter) applyDefaultTimeWindow(query *legacy.BitmapQuery) {
	if query.FromTime == "" {
		query.FromTime = time.Unix(0, 0).UTC().Format(legacy.YMDHTimeFmt)
	}
	if query.ToTime == "" {
		query.ToTime = time.Unix(0, 0).UTC().Format(legacy.YMDHTimeFmt)
	}
}
func (a LegacyBitmapQueryAdapter) applyTimeWindow(query *legacy.BitmapQuery, request ExecutionRequest) {
	for _, fragment := range request.Query.Fragments {
		if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !fragment.ShardWindow {
			continue
		}
		query.FromTime = legacyBitmapQueryTimeWindowValue(fragment.Begin.Int64())
		query.ToTime = legacyBitmapQueryTimeWindowValue(fragment.End.Int64())
		return
	}
}

func legacyRequestFragmentIsTimeField(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) bool {
	for _, field := range request.Query.ProjectionFields {
		if legacyProjectionFieldMatchesFragment(field, fragment) && field.Type == qsbridge.DataTypeTime {
			return true
		}
	}
	for _, field := range request.Materialization.ProjectionFields {
		if legacyProjectionFieldMatchesFragment(field, fragment) && field.Type == qsbridge.DataTypeTime {
			return true
		}
	}
	return false
}

func legacyProjectionFieldMatchesFragment(field qsbridge.QuantaProjectionField, fragment qsbridge.QuantaQueryFragment) bool {
	if field.Index != fragment.Index {
		return false
	}
	return field.Field == fragment.Field || field.PhysicalName == fragment.Field
}

func legacyBitmapQueryTimeWindowValue(epochValue int64) string {
	return time.UnixMilli(legacyBitmapQueryEpochValueMillis(epochValue)).UTC().Format(legacy.YMDHTimeFmt)
}

func legacyBitmapQueryEpochValueMillis(epochValue int64) int64 {
	abs := epochValue
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= int64(100000000000000000):
		return epochValue / int64(time.Millisecond)
	case abs >= int64(100000000000000):
		return epochValue / int64(time.Millisecond/time.Microsecond)
	case abs > 0 && abs < int64(100000000000):
		return epochValue * int64(time.Second/time.Millisecond)
	default:
		return epochValue
	}
}

func (a LegacyBitmapQueryAdapter) validateFragment(index int, fragment qsbridge.QuantaQueryFragment) qsbridge.DiagnosticSet {
	var diagnostics qsbridge.DiagnosticSet
	if fragment.Index == "" {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInvalidExecutionOption,
			qsbridge.PhaseExecute,
			fmt.Sprintf("bitmap fragment %d has no index", index),
		))
	}
	if fragment.Field == "" {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInvalidExecutionOption,
			qsbridge.PhaseExecute,
			fmt.Sprintf("bitmap fragment %d has no field", index),
		))
	}
	if !legacyBitmapOperationSupported(fragment.Operation) {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedPredicate,
			qsbridge.PhaseExecute,
			fmt.Sprintf("bitmap fragment %d uses unsupported operation %q", index, fragment.Operation),
		))
	}
	if !legacyBitmapBSIOpSupported(fragment.BSIOp) {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedPredicate,
			qsbridge.PhaseExecute,
			fmt.Sprintf("bitmap fragment %d uses unsupported BSI operation %q", index, fragment.BSIOp),
		))
	}
	switch fragment.BSIOp {
	case qsbridge.QuantaBSIOpLT, qsbridge.QuantaBSIOpLE, qsbridge.QuantaBSIOpEQ, qsbridge.QuantaBSIOpGE, qsbridge.QuantaBSIOpGT:
		if fragment.Value == nil {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				fmt.Sprintf("bitmap fragment %d BSI operation %q requires value", index, fragment.BSIOp),
			))
		}
	case qsbridge.QuantaBSIOpRange:
		if fragment.Begin == nil || fragment.End == nil {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				fmt.Sprintf("bitmap fragment %d range operation requires begin and end", index),
			))
		}
	case qsbridge.QuantaBSIOpBatchEQ:
		if len(fragment.Values) == 0 {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				fmt.Sprintf("bitmap fragment %d batch equality operation requires values", index),
			))
		}
	}
	return diagnostics
}

func legacyBitmapOperationSupported(operation qsbridge.QuantaFragmentOperation) bool {
	switch operation {
	case qsbridge.QuantaOperationIntersect,
		qsbridge.QuantaOperationUnion,
		qsbridge.QuantaOperationDifference,
		qsbridge.QuantaOperationInnerJoin,
		qsbridge.QuantaOperationOuterJoin:
		return true
	default:
		return false
	}
}

func legacyBitmapBSIOpSupported(op qsbridge.QuantaBSIOp) bool {
	switch op {
	case qsbridge.QuantaBSIOpNone,
		qsbridge.QuantaBSIOpLT,
		qsbridge.QuantaBSIOpLE,
		qsbridge.QuantaBSIOpEQ,
		qsbridge.QuantaBSIOpGE,
		qsbridge.QuantaBSIOpGT,
		qsbridge.QuantaBSIOpRange,
		qsbridge.QuantaBSIOpBatchEQ:
		return true
	default:
		return false
	}
}

func cloneBigInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

func cloneBigIntSlice(values []*big.Int) []*big.Int {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]*big.Int, len(values))
	for i, value := range values {
		cloned[i] = cloneBigInt(value)
	}
	return cloned
}
