package qsbridge

import "testing"

func TestBuildTopNRankRowsSortsAndBucketsOther(t *testing.T) {
	rows := BuildTopNRankRows([]TopNRankEntry{
		{Value: ResultCell{Kind: ValueString, Value: "MAIL"}, Count: 15},
		{Value: ResultCell{Kind: ValueString, Value: "AIR"}, Count: 20},
		{Value: ResultCell{Kind: ValueString, Value: "SHIP"}, Count: 5},
	}, 2)

	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4: %#v", len(rows), rows)
	}
	assertTopNRankRow(t, rows[0], TopNRankValue, "AIR", 20, 50)
	assertTopNRankRow(t, rows[1], TopNRankValue, "MAIL", 15, 37.5)
	assertTopNRankRow(t, rows[2], TopNRankOther, "OTHER:", 5, 12.5)
	assertTopNRankRow(t, rows[3], TopNRankTotal, "TOTAL:", 40, 100)
}

func TestBuildTopNRankRowsCountZeroReturnsAllConcreteValues(t *testing.T) {
	rows := BuildTopNRankRows([]TopNRankEntry{
		{Value: ResultCell{Kind: ValueString, Value: "RAIL"}, Count: 10},
		{Value: ResultCell{Kind: ValueString, Value: "AIR"}, Count: 10},
	}, 0)

	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3: %#v", len(rows), rows)
	}
	assertTopNRankRow(t, rows[0], TopNRankValue, "AIR", 10, 50)
	assertTopNRankRow(t, rows[1], TopNRankValue, "RAIL", 10, 50)
	assertTopNRankRow(t, rows[2], TopNRankTotal, "TOTAL:", 20, 100)
}

func TestBuildTopNRankRowsHandlesEmptyInput(t *testing.T) {
	rows := BuildTopNRankRows(nil, 5)

	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1: %#v", len(rows), rows)
	}
	assertTopNRankRow(t, rows[0], TopNRankTotal, "TOTAL:", 0, 0)
	result := rows[0].ResultRow()
	if len(result) != 3 || result[0].Value != "TOTAL:" || result[1].Value != int64(0) || result[2].Value != float64(0) {
		t.Fatalf("result row = %#v, want TOTAL:/0/0", result)
	}
}

func assertTopNRankRow(t *testing.T, row TopNRankRow, kind TopNRankRowKind, value string, count uint64, percent float64) {
	t.Helper()
	if row.Kind != kind {
		t.Fatalf("row kind = %s, want %s: %#v", row.Kind, kind, row)
	}
	if row.Value.Kind != ValueString || row.Value.Value != value {
		t.Fatalf("row value = %#v, want string %q", row.Value, value)
	}
	if row.Count != count {
		t.Fatalf("row count = %d, want %d: %#v", row.Count, count, row)
	}
	if row.Percent != percent {
		t.Fatalf("row percent = %v, want %v: %#v", row.Percent, percent, row)
	}
}
