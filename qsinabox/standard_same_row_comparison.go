package qsinabox

import (
	"context"
	"strconv"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
)

// StandardSameRowBSIComparator compares same-row BSI fields inside the mounted
// in-process bitmap index and returns only matching rownums to the executor.
type StandardSameRowBSIComparator struct {
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

// CompareSameRowBSI applies a row-local BSI comparison without projecting the
// compared BSI vectors back through the materialization path.
func (c StandardSameRowBSIComparator) CompareSameRowBSI(ctx context.Context, request qsruntime.NativeSameRowBSICompareRequest) (qsruntime.NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error) {
	if c.Direct == nil {
		return qsruntime.NativeSameRowBSICompareResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard same-row BSI comparator has no local bitmap index"),
		}, nil
	}
	if err := ctx.Err(); err != nil {
		return qsruntime.NativeSameRowBSICompareResult{}, nil, err
	}
	foundSet := standardProjectionBitmap(request.Rownums)
	fromTime, toTime := standardProjectionWindowNanos(c.TableCache, request.Index, request.FromEpochMillis, request.ToEpochMillis)
	start := time.Now()
	matches, stats, err := c.Direct.CompareBSIFieldsWithStats(request.Index, request.LeftField, request.RightField, fromTime, toTime, foundSet, request.Operation, request.Invert)
	elapsed := time.Since(start)
	if err != nil {
		return qsruntime.NativeSameRowBSICompareResult{}, nil, err
	}
	rownums := make([]qsbridge.QuantaRownum, 0, len(request.Rownums))
	for _, rownum := range request.Rownums {
		if err := ctx.Err(); err != nil {
			return qsruntime.NativeSameRowBSICompareResult{}, nil, err
		}
		if matches != nil && matches.Contains(uint64(rownum)) {
			rownums = append(rownums, rownum)
		}
	}
	probes := []qsruntime.ExecutionProbe{
		standardSameRowBSIProbe(request.ProbePrefix+"standard_compare_transport", "local_direct", request.Index),
		standardSameRowBSIProbe(request.ProbePrefix+"standard_compare_elapsed", elapsed.String(), request.Index),
		standardSameRowBSIProbe(request.ProbePrefix+"standard_compare_bsi_elapsed", stats.CompareElapsed.String(), request.Index),
		standardSameRowBSIProbe(request.ProbePrefix+"standard_compare_input_rows", strconv.Itoa(len(request.Rownums)), request.Index),
		standardSameRowBSIProbe(request.ProbePrefix+"standard_compare_output_rows", strconv.FormatUint(stats.OutputRows, 10), request.Index),
	}
	probes = append(probes, standardSameRowBSIStatsProbes(request.ProbePrefix, "left", request.Index, request.LeftField, stats.Left)...)
	probes = append(probes, standardSameRowBSIStatsProbes(request.ProbePrefix, "right", request.Index, request.RightField, stats.Right)...)
	return qsruntime.NativeSameRowBSICompareResult{
		Rownums: rownums,
		Probes:  probes,
	}, nil, nil
}

func standardSameRowBSIProbe(name, value, detail string) qsruntime.ExecutionProbe {
	return qsruntime.ExecutionProbe{
		Section: "same_row_comparison",
		Name:    name,
		Value:   value,
		Detail:  detail,
	}
}

func standardSameRowBSIStatsProbes(prefix, side, index, field string, stats server.ProjectBSIStats) []qsruntime.ExecutionProbe {
	detail := side + " " + index + "." + field
	return []qsruntime.ExecutionProbe{
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_shards_visited", strconv.Itoa(stats.ShardsVisited), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_shards_in_window", strconv.Itoa(stats.ShardsInWindow), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_shards_local", strconv.Itoa(stats.ShardsLocal), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_shards_retained", strconv.Itoa(stats.ShardsRetained), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_rows_retained", strconv.FormatUint(stats.RetainedRows, 10), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_retain_elapsed", stats.RetainElapsed.String(), detail),
		standardSameRowBSIProbe(prefix+"standard_compare_"+side+"_merge_elapsed", stats.MergeElapsed.String(), detail),
	}
}
