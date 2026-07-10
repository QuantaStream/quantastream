package qsbridge

import "testing"

func TestExecutionInspectionResultColumnsAreStable(t *testing.T) {
	columns := ExecutionInspectionResultColumns()
	if len(columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(columns))
	}
	for index, want := range []string{"section", "name", "value", "detail"} {
		if columns[index].Name != want || columns[index].Type != DataTypeString {
			t.Fatalf("column %d = %#v, want %s string", index, columns[index], want)
		}
	}
}

func TestExecutionInspectionRowsChunkConvertsRows(t *testing.T) {
	chunk := ExecutionInspectionRowsChunk([]ExecutionInspectionRow{{
		Section: "route",
		Name:    "path",
		Value:   "direct",
		Detail:  "local=true",
	}}, 7, true)

	if chunk.Sequence != 7 || !chunk.Final {
		t.Fatalf("chunk metadata = %#v, want sequence 7 final", chunk)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 4 {
		t.Fatalf("rows = %#v, want one four-cell inspection row", chunk.Rows)
	}
	for index, want := range []string{"route", "path", "direct", "local=true"} {
		cell := chunk.Rows[0][index]
		if cell.Kind != ValueString || cell.Value != want {
			t.Fatalf("cell %d = %#v, want string %q", index, cell, want)
		}
	}
}
