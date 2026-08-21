package qsbridge

import (
	"fmt"
	"sort"
	"strings"
)

// RelationshipTupleRow carries role-qualified rownums for one logical relationship graph row.
type RelationshipTupleRow struct {
	Rownums map[TableInstanceID]QuantaRownum
}

// NewRelationshipTupleRow returns a tuple row containing one role binding.
func NewRelationshipTupleRow(role TableInstanceID, rownum QuantaRownum) RelationshipTupleRow {
	return RelationshipTupleRow{Rownums: map[TableInstanceID]QuantaRownum{role: rownum}}
}

// Rownum returns the rownum bound to role.
func (r RelationshipTupleRow) Rownum(role TableInstanceID) (QuantaRownum, bool) {
	rownum, ok := r.Rownums[role]
	return rownum, ok
}

// WithRownum returns a copy of r with role bound to rownum.
func (r RelationshipTupleRow) WithRownum(role TableInstanceID, rownum QuantaRownum) RelationshipTupleRow {
	cloned := RelationshipTupleRow{Rownums: make(map[TableInstanceID]QuantaRownum, len(r.Rownums)+1)}
	for existingRole, existingRownum := range r.Rownums {
		cloned.Rownums[existingRole] = existingRownum
	}
	cloned.Rownums[role] = rownum
	return cloned
}

// RelationshipTupleRowSet is a role-qualified row stream for relationship-vector graph execution.
type RelationshipTupleRowSet struct {
	Rows []RelationshipTupleRow
}

// NewRelationshipTupleRowSet seeds a tuple rowset from one role and its candidate rownums.
func NewRelationshipTupleRowSet(role TableInstanceID, rownums []QuantaRownum) RelationshipTupleRowSet {
	rows := make([]RelationshipTupleRow, 0, len(rownums))
	for _, rownum := range rownums {
		rows = append(rows, NewRelationshipTupleRow(role, rownum))
	}
	return RelationshipTupleRowSet{Rows: rows}
}

// NewRelationshipTupleRowSetFromRootExpansions seeds root rows and applies relationship expansions in order.
func NewRelationshipTupleRowSetFromRootExpansions(rootRole TableInstanceID, rootRows []QuantaRownum, expansions []RelationshipTupleExpansion) RelationshipTupleRowSet {
	return NewRelationshipTupleRowSet(rootRole, rootRows).ExpandAll(expansions)
}

// NewRelationshipTupleRowSetFromAlignedRownums converts aligned role rownum vectors into tuple rows.
func NewRelationshipTupleRowSetFromAlignedRownums(aligned map[string][]QuantaRownum) (RelationshipTupleRowSet, DiagnosticSet) {
	roles := make([]string, 0, len(aligned))
	rowCount := -1
	for role, rownums := range aligned {
		roles = append(roles, role)
		if rowCount == -1 {
			rowCount = len(rownums)
			continue
		}
		if len(rownums) != rowCount {
			return RelationshipTupleRowSet{}, relationshipTupleDiagnostics(fmt.Sprintf("aligned tuple role %s has %d rows, want %d", role, len(rownums), rowCount))
		}
	}
	sort.Strings(roles)
	if rowCount < 0 {
		return RelationshipTupleRowSet{}, nil
	}
	rows := make([]RelationshipTupleRow, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		row := RelationshipTupleRow{Rownums: make(map[TableInstanceID]QuantaRownum, len(roles))}
		for _, role := range roles {
			row.Rownums[TableInstanceID(role)] = aligned[role][i]
		}
		rows = append(rows, row)
	}
	return RelationshipTupleRowSet{Rows: rows}, nil
}

// CandidateCount reports the number of logical tuple rows.
func (s RelationshipTupleRowSet) CandidateCount() int {
	return len(s.Rows)
}

// Roles returns the sorted role names present in the rowset.
func (s RelationshipTupleRowSet) Roles() []TableInstanceID {
	seen := make(map[TableInstanceID]struct{})
	for _, row := range s.Rows {
		for role := range row.Rownums {
			seen[role] = struct{}{}
		}
	}
	roles := make([]TableInstanceID, 0, len(seen))
	for role := range seen {
		roles = append(roles, role)
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i] < roles[j] })
	return roles
}

// RelationshipTupleExpansion describes one parent-to-child relationship-vector expansion.
type RelationshipTupleExpansion struct {
	ParentRole        TableInstanceID
	ChildRole         TableInstanceID
	ChildRowsByParent map[QuantaRownum][]QuantaRownum
	NullExtend        bool
}

// Expand returns the expansion for one child role.
func (s RelationshipTupleRowSet) Expand(expansion RelationshipTupleExpansion) RelationshipTupleRowSet {
	rows := make([]RelationshipTupleRow, 0, len(s.Rows))
	for _, row := range s.Rows {
		parent, ok := row.Rownum(expansion.ParentRole)
		if !ok {
			if expansion.NullExtend {
				rows = append(rows, row)
			}
			continue
		}
		children := expansion.ChildRowsByParent[parent]
		if len(children) == 0 {
			if expansion.NullExtend {
				rows = append(rows, row)
			}
			continue
		}
		for _, child := range children {
			rows = append(rows, row.WithRownum(expansion.ChildRole, child))
		}
	}
	return RelationshipTupleRowSet{Rows: rows}
}

// ExpandAll applies relationship expansions in order.
func (s RelationshipTupleRowSet) ExpandAll(expansions []RelationshipTupleExpansion) RelationshipTupleRowSet {
	for _, expansion := range expansions {
		s = s.Expand(expansion)
	}
	return s
}

// ToProjectedRowSet materializes tuple rows into the existing projected-rowset shape.
func (s RelationshipTupleRowSet) ToProjectedRowSet(index string, fields []QuantaProjectionField, values RelationshipTupleValueStore) (QuantaProjectedRowSet, DiagnosticSet) {
	rowSet := QuantaProjectedRowSet{
		Index:   index,
		Rownums: make([]QuantaRownum, len(s.Rows)),
	}
	for i := range s.Rows {
		rowSet.Rownums[i] = QuantaRownum(i + 1)
	}
	for _, field := range fields {
		key := RelationshipTupleValueKeyForField(field)
		byRownum, ok := values[key]
		if !ok {
			return QuantaProjectedRowSet{}, relationshipTupleDiagnostics(fmt.Sprintf("tuple materialization missing values for %s", relationshipTupleValueKeyDebug(key)))
		}
		vector := QuantaProjectionVector{Field: field}
		for _, row := range s.Rows {
			rownum, ok := row.Rownum(key.Role)
			if !ok {
				vector.Values = append(vector.Values, ResultCell{Kind: ValueNull, Value: nil})
				continue
			}
			cell, ok := byRownum[rownum]
			if !ok {
				return QuantaProjectedRowSet{}, relationshipTupleDiagnostics(fmt.Sprintf("tuple materialization missing value for %s row %d", relationshipTupleValueKeyDebug(key), rownum))
			}
			vector.Values = append(vector.Values, cell)
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet, rowSet.ValidateShape()
}

// FilterByIndexes returns tuple rows at the supplied zero-based row indexes.
func (s RelationshipTupleRowSet) FilterByIndexes(indexes []int) RelationshipTupleRowSet {
	rows := make([]RelationshipTupleRow, 0, len(indexes))
	for _, index := range indexes {
		if index >= 0 && index < len(s.Rows) {
			rows = append(rows, s.Rows[index])
		}
	}
	return RelationshipTupleRowSet{Rows: rows}
}

func relationshipTupleDiagnostics(message string) DiagnosticSet {
	return DiagnosticSet{ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, message)}
}

func relationshipTupleValueKeyDebug(key RelationshipTupleValueKey) string {
	parts := []string{string(key.Role)}
	if key.Field != "" {
		parts = append(parts, key.Field)
	}
	return strings.Join(parts, ".")
}
