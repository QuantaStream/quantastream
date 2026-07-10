package qsbridge

import (
	"fmt"
	"sort"
)

// TopNRankRowKind identifies whether one ranked row is a data value, an OTHER bucket, or the TOTAL bucket.
type TopNRankRowKind string

const (
	// TopNRankValue marks a row for one concrete field value.
	TopNRankValue TopNRankRowKind = "value"
	// TopNRankOther marks the aggregate bucket for values outside the requested top-N window.
	TopNRankOther TopNRankRowKind = "other"
	// TopNRankTotal marks the final total row for all ranked values.
	TopNRankTotal TopNRankRowKind = "total"
)

// TopNRankEntry is one value/cardinality pair produced by a bitmap-backed ranking source.
type TopNRankEntry struct {
	Value ResultCell
	Count uint64
}

// TopNRankRow is one protocol-neutral top-N ranking result.
type TopNRankRow struct {
	Kind    TopNRankRowKind
	Value   ResultCell
	Count   uint64
	Percent float64
}

// BuildTopNRankRows assembles sorted top-N rows plus optional OTHER and final TOTAL rows.
//
// A count of zero means "return all concrete values", matching the legacy
// Projector.Rank behavior. The input is copied before sorting so runtime
// adapters can safely pass cached cardinality slices.
func BuildTopNRankRows(entries []TopNRankEntry, count int) []TopNRankRow {
	ordered := append([]TopNRankEntry(nil), entries...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Count != ordered[j].Count {
			return ordered[i].Count > ordered[j].Count
		}
		return topNRankStableValueText(ordered[i].Value) < topNRankStableValueText(ordered[j].Value)
	})

	var total uint64
	for _, entry := range ordered {
		total += entry.Count
	}

	limit := count
	if limit <= 0 || limit > len(ordered) {
		limit = len(ordered)
	}
	rows := make([]TopNRankRow, 0, limit+2)
	var other uint64
	for index, entry := range ordered {
		if index < limit {
			rows = append(rows, TopNRankRow{
				Kind:    TopNRankValue,
				Value:   entry.Value,
				Count:   entry.Count,
				Percent: topNRankPercent(entry.Count, total),
			})
			continue
		}
		other += entry.Count
	}
	if other > 0 {
		rows = append(rows, TopNRankRow{
			Kind:    TopNRankOther,
			Value:   ResultCell{Kind: ValueString, Value: "OTHER:"},
			Count:   other,
			Percent: topNRankPercent(other, total),
		})
	}
	rows = append(rows, TopNRankRow{
		Kind:    TopNRankTotal,
		Value:   ResultCell{Kind: ValueString, Value: "TOTAL:"},
		Count:   total,
		Percent: topNRankPercent(total, total),
	})
	return rows
}

// ResultRow converts a ranked row to the three-column topn value/count/percent shape.
func (r TopNRankRow) ResultRow() ResultRow {
	return ResultRow{
		r.Value,
		{Kind: ValueInt, Value: int64(r.Count)},
		{Kind: ValueFloat, Value: r.Percent},
	}
}

func topNRankPercent(count, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(count) / float64(total) * 100
}

func topNRankStableValueText(value ResultCell) string {
	return fmt.Sprintf("%s:%v", value.Kind, value.Value)
}
