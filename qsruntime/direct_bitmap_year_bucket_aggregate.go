package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type directBitmapYearBucketPlan struct {
	GroupExpr qsbridge.Expr
	Field     qsbridge.FieldRef
	YearMin   int
	YearMax   int
	BoundMode string
}

func (r DirectBitmapRuntime) directBitmapYearBucketCountAggregateResult(ctx context.Context, request ExecutionRequest, result ExecutionResult) (ExecutionResult, bool) {
	var boundsProvider DirectTimeBucketBoundsProvider
	if provider, ok := r.Sessions.(DirectTimeBucketBoundsProvider); ok {
		boundsProvider = provider
	}
	plan, ok := directBitmapYearBucketCountPlan(ctx, request, boundsProvider)
	if !ok {
		return result, false
	}
	if r.Sessions == nil {
		return result, false
	}
	start := time.Now()
	rows := make([]directBitmapGroupedAggregateRow, 0, max(0, plan.YearMax-plan.YearMin+1))
	queries := 0
	for year := plan.YearMin; year <= plan.YearMax; year++ {
		bucketRequest, ok := directBitmapYearBucketRequest(request, plan.Field, year)
		if !ok {
			return result, false
		}
		session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, bucketRequest)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				err.Error(),
			))
			return result, true
		}
		if result.Diagnostics.BlocksNative() {
			return result, true
		}
		if session == nil {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"direct session provider returned nil session for year bucket aggregate",
			))
			return result, true
		}
		bucket, bucketDiagnostics, bucketErr := session.QueryBitmap(ctx, bucketRequest)
		releaseDiagnostics := session.Release(ctx)
		queries++
		result.Diagnostics = append(result.Diagnostics, bucketDiagnostics...)
		result.Diagnostics = append(result.Diagnostics, releaseDiagnostics...)
		if bucketErr != nil {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				bucketErr.Error(),
			))
			return result, true
		}
		if result.Diagnostics.BlocksNative() {
			return result, true
		}
		if bucket.Count == 0 {
			continue
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key: strconv.Itoa(year),
			Groups: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueInt,
				Value: int64(year),
			}},
			Aggs: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueInt,
				Value: int64(bucket.Count),
			}},
		})
	}
	groupExpressions := []directBitmapGroupExpression{{Expr: plan.GroupExpr, Field: plan.Field}}
	var diagnostics qsbridge.DiagnosticSet
	rows, diagnostics = directBitmapOrderGroupedAggregateRows(request, rows, groupExpressions)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	rowSet, diagnostics := directBitmapGroupedAggregateRowSet(request, rows, groupExpressions)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit)
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	probes := []ExecutionProbe{
		{Section: "grouped_aggregate", Name: "year_bucket_mode", Value: "timestamp_bsi_range"},
		{Section: "grouped_aggregate", Name: "year_bucket_queries", Value: strconv.Itoa(queries)},
		{Section: "grouped_aggregate", Name: "year_bucket_groups", Value: strconv.Itoa(len(rows))},
		{Section: "grouped_aggregate", Name: "year_bucket_bound_mode", Value: plan.BoundMode, Detail: directBitmapFieldPhysicalName(plan.Field)},
		{Section: "grouped_aggregate", Name: "year_bucket_range", Value: fmt.Sprintf("%d-%d", plan.YearMin, plan.YearMax), Detail: directBitmapFieldPhysicalName(plan.Field)},
		{Section: "grouped_aggregate", Name: "phase_year_bucket_elapsed", Value: time.Since(start).String()},
	}
	result.Probes = append(result.Probes, probes...)
	recordExecutionProbes(ctx, probes)
	return result, true
}

func directBitmapYearBucketCountPlan(ctx context.Context, request ExecutionRequest, boundsProvider DirectTimeBucketBoundsProvider) (directBitmapYearBucketPlan, bool) {
	if request.HasCandidateSet ||
		len(request.GroupBy) != 1 ||
		len(request.SQLAggregates) != 1 ||
		len(request.Having) != 0 ||
		len(request.Joins) != 0 ||
		len(request.Memberships) != 0 ||
		len(request.Query.Seeds) != 0 ||
		!request.Query.Filter.Empty() ||
		directBitmapHasResidualScanPredicates(request) ||
		!request.NativePredicates.Empty() ||
		!directBitmapCountAllAggregate(request.SQLAggregates[0]) {
		return directBitmapYearBucketPlan{}, false
	}
	field, ok := directBitmapYearBucketGroupField(request.GroupBy[0])
	if !ok {
		return directBitmapYearBucketPlan{}, false
	}
	root, ok := request.RootIndex()
	if !ok || !strings.EqualFold(root, field.Table.Table) {
		return directBitmapYearBucketPlan{}, false
	}
	yearMin, yearMax, boundMode, ok := directBitmapYearBucketBounds(request, field)
	if !ok || yearMax < yearMin {
		return directBitmapYearBucketPlan{}, false
	}
	if boundsProvider != nil {
		if observedMin, observedMax, observedOK := boundsProvider.TimeBucketYearBounds(ctx, request, field); observedOK {
			if observedMin > yearMin {
				yearMin = observedMin
			}
			if observedMax < yearMax {
				yearMax = observedMax
			}
			boundMode = "observed_shards"
		}
	}
	if yearMax < yearMin {
		return directBitmapYearBucketPlan{}, false
	}
	return directBitmapYearBucketPlan{
		GroupExpr: request.GroupBy[0],
		Field:     field,
		YearMin:   yearMin,
		YearMax:   yearMax,
		BoundMode: boundMode,
	}, true
}

func directBitmapYearBucketGroupField(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	call, ok := directBitmapCallExpr(expr)
	if !ok || !directBitmapYearFunctionName(call.Name) || len(call.Args) != 1 {
		return qsbridge.FieldRef{}, false
	}
	field, ok := directBitmapExprField(call.Args[0])
	if !ok || field.Type != qsbridge.DataTypeTime {
		return qsbridge.FieldRef{}, false
	}
	return field, true
}

func directBitmapYearFunctionName(name string) bool {
	switch strings.ToLower(name) {
	case "year", "yy":
		return true
	default:
		return false
	}
}

func directBitmapYearBucketBounds(request ExecutionRequest, field qsbridge.FieldRef) (int, int, string, bool) {
	boundMode := "full_time_window"
	fullRangeStart := time.UnixMilli(legacyDirectRelationshipFullTimeRangeBeginMillis).UTC()
	fullRangeEndExclusive := time.UnixMilli(legacyDirectRelationshipFullTimeRangeEndMillis).UTC()
	lower, ok := directBitmapEncodeTimeValue(field.Encoding.Granularity, fullRangeStart)
	if !ok {
		return 0, 0, "", false
	}
	upperExclusive, ok := directBitmapEncodeTimeValue(field.Encoding.Granularity, fullRangeEndExclusive)
	if !ok {
		return 0, 0, "", false
	}
	upper := upperExclusive - 1
	for _, fragment := range request.Query.Fragments {
		if !directBitmapYearBucketFragmentTargetsField(fragment, field) || fragment.Operation != qsbridge.QuantaOperationIntersect || fragment.Negate {
			continue
		}
		switch fragment.BSIOp {
		case qsbridge.QuantaBSIOpRange:
			if fragment.Begin != nil && fragment.Begin.Int64() > lower {
				lower = fragment.Begin.Int64()
				boundMode = "predicate_bounds"
			}
			if fragment.End != nil && fragment.End.Int64() < upper {
				upper = fragment.End.Int64()
				boundMode = "predicate_bounds"
			}
		case qsbridge.QuantaBSIOpEQ:
			if fragment.Value == nil {
				continue
			}
			value := fragment.Value.Int64()
			if value > lower {
				lower = value
				boundMode = "predicate_bounds"
			}
			if value < upper {
				upper = value
				boundMode = "predicate_bounds"
			}
		case qsbridge.QuantaBSIOpGE:
			if fragment.Value != nil && fragment.Value.Int64() > lower {
				lower = fragment.Value.Int64()
				boundMode = "predicate_bounds"
			}
		case qsbridge.QuantaBSIOpGT:
			if fragment.Value != nil && fragment.Value.Int64()+1 > lower {
				lower = fragment.Value.Int64() + 1
				boundMode = "predicate_bounds"
			}
		case qsbridge.QuantaBSIOpLE:
			if fragment.Value != nil && fragment.Value.Int64() < upper {
				upper = fragment.Value.Int64()
				boundMode = "predicate_bounds"
			}
		case qsbridge.QuantaBSIOpLT:
			if fragment.Value != nil && fragment.Value.Int64()-1 < upper {
				upper = fragment.Value.Int64() - 1
				boundMode = "predicate_bounds"
			}
		}
	}
	if upper < lower {
		return 0, 0, "", false
	}
	lowerTime, ok := directBitmapDecodeTimeValue(field.Encoding.Granularity, lower)
	if !ok {
		return 0, 0, "", false
	}
	upperTime, ok := directBitmapDecodeTimeValue(field.Encoding.Granularity, upper)
	if !ok {
		return 0, 0, "", false
	}
	return lowerTime.Year(), upperTime.Year(), boundMode, true
}

func directBitmapYearBucketRequest(request ExecutionRequest, field qsbridge.FieldRef, year int) (ExecutionRequest, bool) {
	begin, ok := directBitmapEncodeTimeValue(field.Encoding.Granularity, time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		return ExecutionRequest{}, false
	}
	next, ok := directBitmapEncodeTimeValue(field.Encoding.Granularity, time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		return ExecutionRequest{}, false
	}
	fragment := qsbridge.QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      directBitmapYearBucketTableRole(field.Table),
		Field:     directBitmapFieldPhysicalName(field),
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpRange,
		Begin:     big.NewInt(begin),
		End:       big.NewInt(next - 1),
	}
	if fragment.Field == "" {
		fragment.Field = field.Name
	}
	bucketRequest := request
	bucketRequest.Query = cloneIntermediateQuery(request.Query)
	bucketRequest.Query.Fragments = append(bucketRequest.Query.Fragments, fragment)
	bucketRequest.SQLAggregates = nil
	bucketRequest.GroupBy = nil
	bucketRequest.Having = nil
	bucketRequest.OrderBy = nil
	bucketRequest.Projection = nil
	bucketRequest.Result = qsbridge.ResultShape{}
	bucketRequest.NativePredicates = NativePredicateSet{}
	return bucketRequest, true
}

func directBitmapYearBucketFragmentTargetsField(fragment qsbridge.QuantaQueryFragment, field qsbridge.FieldRef) bool {
	if !strings.EqualFold(fragment.Index, field.Table.Table) {
		return false
	}
	physical := directBitmapFieldPhysicalName(field)
	if physical == "" {
		physical = field.Name
	}
	if !strings.EqualFold(fragment.Field, physical) {
		return false
	}
	role := directBitmapYearBucketTableRole(field.Table)
	return fragment.Role == "" || role == "" || fragment.Role == role
}

func directBitmapYearBucketTableRole(table qsbridge.TableInstance) qsbridge.TableInstanceID {
	if table.Alias != "" {
		return qsbridge.TableInstanceID(table.Alias)
	}
	if table.ID != "" {
		return table.ID
	}
	return qsbridge.TableInstanceID(table.Table)
}

func directBitmapEncodeTimeValue(granularity qsbridge.TimeGranularity, value time.Time) (int64, bool) {
	normalized := value.UTC()
	switch granularity {
	case qsbridge.TimeGranularityUnknown, qsbridge.TimeGranularityMillisecond:
		return normalized.UnixMilli(), true
	case qsbridge.TimeGranularitySecond:
		return normalized.Unix(), true
	case qsbridge.TimeGranularityMicrosecond:
		return normalized.UnixMicro(), true
	case qsbridge.TimeGranularityNanosecond:
		return normalized.UnixNano(), true
	case qsbridge.TimeGranularityDay:
		return normalized.Truncate(24*time.Hour).Unix() / int64((24*time.Hour)/time.Second), true
	default:
		return 0, false
	}
}

func directBitmapDecodeTimeValue(granularity qsbridge.TimeGranularity, encoded int64) (time.Time, bool) {
	switch granularity {
	case qsbridge.TimeGranularityUnknown, qsbridge.TimeGranularityMillisecond:
		return time.UnixMilli(encoded).UTC(), true
	case qsbridge.TimeGranularitySecond:
		return time.Unix(encoded, 0).UTC(), true
	case qsbridge.TimeGranularityMicrosecond:
		return time.UnixMicro(encoded).UTC(), true
	case qsbridge.TimeGranularityNanosecond:
		return time.Unix(0, encoded).UTC(), true
	case qsbridge.TimeGranularityDay:
		return time.Unix(encoded*int64((24*time.Hour)/time.Second), 0).UTC(), true
	default:
		return time.Time{}, false
	}
}
