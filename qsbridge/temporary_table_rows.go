package qsbridge

func cloneTemporaryTableRowsMap(rows map[string]TemporaryTableData) map[string]TemporaryTableData {
	if rows == nil {
		return nil
	}
	cloned := make(map[string]TemporaryTableData, len(rows))
	for key, data := range rows {
		cloned[key] = cloneTemporaryTableData(data)
	}
	return cloned
}

func cloneTemporaryTableData(data TemporaryTableData) TemporaryTableData {
	return TemporaryTableData{Rows: cloneTemporaryTableRows(data.Rows)}
}

func cloneTemporaryTableRows(rows []TemporaryTableRow) []TemporaryTableRow {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([]TemporaryTableRow, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, TemporaryTableRow{Values: cloneResultRow(row.Values)})
	}
	return cloned
}
