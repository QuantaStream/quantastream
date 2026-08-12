package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// LegacyDirectSharedRelationshipVectorAggregateReader provides relationship
// aggregate capability for shared/cluster runtimes by using the existing
// cluster projection path and reducing the aligned rows in the query process.
type LegacyDirectSharedRelationshipVectorAggregateReader struct {
	Source legacyDirectRelationshipAggregateProjectionSource
}

type legacyDirectRelationshipAggregateProjectionSource interface {
	ReadProjectionBSI(context.Context, NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error)
}

// ReadRelationshipVectorAggregate returns mergeable grouped aggregate state for
// an already-aligned relationship vector. It intentionally handles only the
// same-index aligned shape; artifact-specific and remote node-local pushdown can
// layer behind this interface later.
func (r LegacyDirectSharedRelationshipVectorAggregateReader) ReadRelationshipVectorAggregate(ctx context.Context, read LegacyDirectRelationshipVectorAggregateRequest) (LegacyDirectRelationshipVectorAggregateResult, qsbridge.DiagnosticSet, bool, error) {
	if read.VectorIndex == "" || read.ValueField == "" {
		return LegacyDirectRelationshipVectorAggregateResult{}, nil, false, nil
	}
	valueIndex := read.ValueIndex
	if valueIndex == "" {
		valueIndex = read.VectorIndex
	}
	if valueIndex != read.VectorIndex {
		return LegacyDirectRelationshipVectorAggregateResult{}, nil, false, nil
	}
	if len(read.ChildRows) != len(read.ParentRows) {
		return LegacyDirectRelationshipVectorAggregateResult{}, nil, true, fmt.Errorf("relationship aggregate requires aligned child and parent rows")
	}
	projectionSource := r.Source
	if projectionSource == nil {
		return LegacyDirectRelationshipVectorAggregateResult{}, nil, false, nil
	}
	result := LegacyDirectRelationshipVectorAggregateResult{
		Mode:         "shared_projection_aligned_sum",
		Rows:         uint64(len(read.ChildRows)),
		SourceValues: legacyDirectRelationshipAggregateUniqueParentCount(read.ParentRows),
	}
	if len(read.ChildRows) == 0 {
		return result, nil, true, nil
	}

	projectionStart := time.Now()
	projection, diagnostics, err := projectionSource.ReadProjectionBSI(ctx, NativeProjectionBSIReadRequest{
		Index: valueIndex,
		Field: qsbridge.QuantaProjectionField{
			Index:        valueIndex,
			Field:        read.ValueField,
			PhysicalName: read.ValueField,
		},
		PhysicalField:   read.ValueField,
		Rownums:         append([]qsbridge.QuantaRownum(nil), read.ChildRows...),
		FromEpochMillis: read.FromEpochMillis,
		ToEpochMillis:   read.ToEpochMillis,
	})
	result.ProjectionElapsed = time.Since(projectionStart)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, true, err
	}
	if projection.BSI == nil {
		return result, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship aggregate projection returned nil BSI"),
		}, true, nil
	}

	aggregateStart := time.Now()
	groupsByParent := make(map[qsbridge.QuantaRownum]*LegacyDirectRelationshipVectorAggregateGroup, result.SourceValues)
	values := projection.BSI.GetBigValues(nativeProjectionRownumColumnIDs(read.ChildRows))
	for i, value := range values {
		if i >= len(read.ChildRows) || i >= len(read.ParentRows) || value == nil {
			continue
		}
		parentRow := read.ParentRows[i]
		group := groupsByParent[parentRow]
		if group == nil {
			group = &LegacyDirectRelationshipVectorAggregateGroup{
				ParentRow:              parentRow,
				RepresentativeChildRow: read.ChildRows[i],
				Sum:                    big.NewInt(0),
			}
			groupsByParent[parentRow] = group
		}
		group.Count++
		group.Sum.Add(group.Sum, value)
	}
	result.AggregateElapsed = time.Since(aggregateStart)
	result.Groups = legacyDirectRelationshipAggregateSortedGroups(groupsByParent)
	result.Values = uint64(len(result.Groups))
	for _, group := range result.Groups {
		result.TargetRows += group.Count
	}
	return result, nil, true, nil
}

func legacyDirectRelationshipAggregateUniqueParentCount(parentRows []qsbridge.QuantaRownum) int {
	if len(parentRows) == 0 {
		return 0
	}
	seen := make(map[qsbridge.QuantaRownum]struct{}, len(parentRows))
	for _, row := range parentRows {
		seen[row] = struct{}{}
	}
	return len(seen)
}

func legacyDirectRelationshipAggregateSortedGroups(groupsByParent map[qsbridge.QuantaRownum]*LegacyDirectRelationshipVectorAggregateGroup) []LegacyDirectRelationshipVectorAggregateGroup {
	if len(groupsByParent) == 0 {
		return nil
	}
	keys := make([]qsbridge.QuantaRownum, 0, len(groupsByParent))
	for row := range groupsByParent {
		keys = append(keys, row)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	groups := make([]LegacyDirectRelationshipVectorAggregateGroup, 0, len(keys))
	for _, row := range keys {
		group := groupsByParent[row]
		if group == nil {
			continue
		}
		groups = append(groups, LegacyDirectRelationshipVectorAggregateGroup{
			ParentRow:              group.ParentRow,
			RepresentativeChildRow: group.RepresentativeChildRow,
			Count:                  group.Count,
			Sum:                    new(big.Int).Set(group.Sum),
		})
	}
	return groups
}
