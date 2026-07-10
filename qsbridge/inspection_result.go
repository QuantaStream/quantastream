package qsbridge

// ExecutionInspectionRow is one protocol-neutral inspection record.
//
// Runtime, planner, optimizer, and adapter inspections can all expose rows in
// this stable four-column shape without depending on a particular wire
// protocol or SQL client renderer.
type ExecutionInspectionRow struct {
	Section string
	Name    string
	Value   string
	Detail  string
}

// ExecutionInspectionResultColumns returns stable metadata for inspection result rows.
func ExecutionInspectionResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "section", Type: DataTypeString},
		{Name: "name", Type: DataTypeString},
		{Name: "value", Type: DataTypeString},
		{Name: "detail", Type: DataTypeString},
	}
}

// ExecutionInspectionRowsChunk converts inspection rows into a protocol-neutral result chunk.
func ExecutionInspectionRowsChunk(rows []ExecutionInspectionRow, sequence int, final bool) ResultChunk {
	chunk := ResultChunk{
		Rows:     make([]ResultRow, 0, len(rows)),
		Sequence: sequence,
		Final:    final,
	}
	for _, row := range rows {
		chunk.Rows = append(chunk.Rows, ResultRow{
			executionInspectionStringCell(row.Section),
			executionInspectionStringCell(row.Name),
			executionInspectionStringCell(row.Value),
			executionInspectionStringCell(row.Detail),
		})
	}
	return chunk
}

func executionInspectionStringCell(value string) ResultCell {
	return ResultCell{Kind: ValueString, Value: value}
}
