package qsbridge

import (
	"fmt"
	"strings"
)

func (c *BindContext) addInlineRowSet(table UnboundTable) (BoundTable, DiagnosticSet) {
	rowSet := table.InlineRows
	if rowSet == nil {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "inline rowset is nil"),
		})
	}
	if len(rowSet.Columns) == 0 {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "inline rowset has no columns"),
		})
	}
	if len(rowSet.Rows) == 0 {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "inline rowset has no rows"),
		})
	}
	fields, diagnostics := inlineRowSetFields(*rowSet)
	if diagnostics.BlocksNative() {
		return BoundTable{}, c.record(diagnostics)
	}
	rows, diagnostics := inlineRowSetRows(*rowSet, len(fields))
	if diagnostics.BlocksNative() {
		return BoundTable{}, c.record(diagnostics)
	}

	schema := table.Schema
	if schema == "" {
		schema = c.DefaultSchema
	}
	definition := TableDefinition{
		Schema: schema,
		Name:   table.Name,
		Fields: fields,
	}
	c.nextTableID++
	instance := definition.Instance(TableInstanceID(fmt.Sprintf("%s_%d", table.Name, c.nextTableID)), table.Alias)
	instance.Role = table.Role
	bound := BoundTable{
		Instance:   instance,
		Definition: definition,
	}
	c.tables = append(c.tables, bound)
	c.inlineRowSets = append(c.inlineRowSets, InlineRowSet{
		Source: instance,
		Fields: append([]FieldDefinition(nil), fields...),
		Rows:   cloneResultRows(rows),
	})
	return bound, nil
}

func inlineRowSetFields(rowSet UnboundInlineRowSet) ([]FieldDefinition, DiagnosticSet) {
	fields := make([]FieldDefinition, 0, len(rowSet.Columns))
	seen := make(map[string]struct{}, len(rowSet.Columns))
	for columnIndex, projection := range rowSet.Columns {
		name := strings.TrimSpace(projection.Alias)
		if name == "" {
			name = inlineRowSetExpressionColumnName(projection.Expr, columnIndex)
		}
		if name == "" {
			return nil, inlineRowSetDiagnostic(fmt.Sprintf("inline UNION ALL column %d requires an alias", columnIndex+1))
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return nil, inlineRowSetDiagnostic("duplicate inline UNION ALL column: " + name)
		}
		seen[key] = struct{}{}
		fields = append(fields, FieldDefinition{
			Name:         name,
			PhysicalName: name,
			Type:         inlineRowSetColumnType(rowSet, columnIndex),
			Nullable:     inlineRowSetColumnNullable(rowSet, columnIndex),
		})
	}
	return fields, nil
}

func inlineRowSetRows(rowSet UnboundInlineRowSet, columnCount int) ([]ResultRow, DiagnosticSet) {
	rows := make([]ResultRow, 0, len(rowSet.Rows))
	for rowIndex, values := range rowSet.Rows {
		if len(values) != columnCount {
			return nil, inlineRowSetDiagnostic(fmt.Sprintf("inline UNION ALL row %d has %d columns, expected %d", rowIndex+1, len(values), columnCount))
		}
		row := make(ResultRow, 0, columnCount)
		for _, expr := range values {
			cell, diagnostics := inlineRowSetCell(expr)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			row = append(row, cell)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func inlineRowSetCell(expr UnboundExpr) (ResultCell, DiagnosticSet) {
	literal, ok := expr.(UnboundLiteralExpr)
	if !ok {
		return ResultCell{}, inlineRowSetDiagnostic("inline UNION ALL branches must project constant literals")
	}
	return ResultCell{Kind: literal.Kind, Value: literal.Value}, nil
}

func inlineRowSetExpressionColumnName(expr UnboundExpr, columnIndex int) string {
	if field, ok := expr.(UnboundFieldExpr); ok && field.Name != "" {
		return field.Name
	}
	if columnIndex >= 0 {
		return fmt.Sprintf("column_%d", columnIndex+1)
	}
	return ""
}

func inlineRowSetColumnType(rowSet UnboundInlineRowSet, columnIndex int) DataType {
	for _, row := range rowSet.Rows {
		if columnIndex >= len(row) {
			continue
		}
		literal, ok := row[columnIndex].(UnboundLiteralExpr)
		if !ok || literal.Kind == ValueNull {
			continue
		}
		return literalDataType(literal.Kind)
	}
	return DataTypeUnknown
}

func inlineRowSetColumnNullable(rowSet UnboundInlineRowSet, columnIndex int) bool {
	for _, row := range rowSet.Rows {
		if columnIndex >= len(row) {
			return true
		}
		literal, ok := row[columnIndex].(UnboundLiteralExpr)
		if !ok || literal.Kind == ValueNull {
			return true
		}
	}
	return false
}

func inlineRowSetDiagnostic(message string) DiagnosticSet {
	return DiagnosticSet{
		ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, message),
	}
}

func cloneInlineRowSets(rowSets []InlineRowSet) []InlineRowSet {
	if len(rowSets) == 0 {
		return nil
	}
	cloned := make([]InlineRowSet, 0, len(rowSets))
	for _, rowSet := range rowSets {
		cloned = append(cloned, InlineRowSet{
			Source: rowSet.Source,
			Fields: append([]FieldDefinition(nil), rowSet.Fields...),
			Rows:   cloneResultRows(rowSet.Rows),
		})
	}
	return cloned
}

func cloneResultRows(rows []ResultRow) []ResultRow {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([]ResultRow, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append(ResultRow(nil), row...))
	}
	return cloned
}
