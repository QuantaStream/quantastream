package qsinabox

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// StandardRelationshipVectorProjectionReader projects relationship-vector FK
// BSIs through the in-process inabox-standard session pool.
type StandardRelationshipVectorProjectionReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

// StandardRelationshipReverseArtifactCandidateReader reads maintained
// parent-to-child artifacts from the in-process bitmap tier.
type StandardRelationshipReverseArtifactCandidateReader struct {
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

// StandardRelationshipVectorAggregateReader performs storage-side relationship
// aggregates through the in-process bitmap tier.
type StandardRelationshipVectorAggregateReader struct {
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

// RelationshipVectorReverseArtifactStats returns artifact cardinality without
// materializing candidate rows.
func (r StandardRelationshipReverseArtifactCandidateReader) RelationshipVectorReverseArtifactStats(
	_ context.Context,
	read qsruntime.LegacyDirectRelationshipVectorReadRequest,
) (qsruntime.LegacyDirectRelationshipVectorReverseArtifactStats, bool, error) {
	if r.Direct == nil || read.VectorIndex == "" || read.VectorField == "" {
		return qsruntime.LegacyDirectRelationshipVectorReverseArtifactStats{}, false, nil
	}
	stats, ok, err := r.Direct.RelationshipReverseArtifactStats(read.VectorIndex, read.VectorField)
	if err != nil || !ok {
		return qsruntime.LegacyDirectRelationshipVectorReverseArtifactStats{}, ok, err
	}
	return qsruntime.LegacyDirectRelationshipVectorReverseArtifactStats{
		Rows:   stats.Rows,
		Values: stats.Values,
	}, true, nil
}

// ReadRelationshipVectorReverseArtifactCandidates returns candidate child rows
// from a schema-declared reverse artifact when the local bitmap tier owns one.
func (r StandardRelationshipReverseArtifactCandidateReader) ReadRelationshipVectorReverseArtifactCandidates(
	_ context.Context,
	read qsruntime.LegacyDirectRelationshipVectorReadRequest,
	sourceValues []int64,
) (qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult, qsbridge.DiagnosticSet, bool, error) {
	if r.Direct == nil || read.VectorIndex == "" || read.VectorField == "" {
		return qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult{}, nil, false, nil
	}
	rownums, parentValues, stats, ok, err := r.Direct.RelationshipReverseArtifactCandidateValues(read.VectorIndex, read.VectorField, sourceValues)
	if err != nil || !ok {
		return qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult{}, nil, ok, err
	}
	candidateRows := make([]qsbridge.QuantaRownum, 0, len(rownums))
	for _, rownum := range rownums {
		candidateRows = append(candidateRows, qsbridge.QuantaRownum(rownum))
	}
	parentValueByChild := make(map[qsbridge.QuantaRownum]int64, len(parentValues))
	for child, parentValue := range parentValues {
		parentValueByChild[qsbridge.QuantaRownum(child)] = parentValue
	}
	return qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
		Candidates: qsbridge.QuantaCandidateSet{
			Index:   read.TargetDomain,
			Rownums: candidateRows,
		},
		ParentValueByChild: parentValueByChild,
		Mode:               "reverse_artifact_server",
		CacheHit:           true,
		Rows:               stats.Rows,
		Values:             stats.Values,
		SourceValues:       stats.SourceValues,
		TargetRows:         stats.TargetRows,
		LookupElapsed:      stats.LookupElapsed,
	}, nil, true, nil
}

// ReadRelationshipSiblingDiversityCandidates returns child rows whose parent
// bucket has at least one sibling with a different value.
func (r StandardRelationshipReverseArtifactCandidateReader) ReadRelationshipSiblingDiversityCandidates(
	_ context.Context,
	read qsruntime.RelationshipSiblingDiversityReadRequest,
) (qsruntime.RelationshipSiblingDiversityReadResult, qsbridge.DiagnosticSet, bool, error) {
	if r.Direct == nil || read.Index == "" || read.ParentField == "" || read.ValueField == "" {
		return qsruntime.RelationshipSiblingDiversityReadResult{}, nil, false, nil
	}
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.Index, read.FromEpochMillis, read.ToEpochMillis)
	rownums, stats, ok, err := r.Direct.RelationshipSiblingDiversityCandidates(
		read.Index,
		read.ParentField,
		read.ValueField,
		fromTime,
		toTime,
		standardRelationshipAggregateRows(read.CandidateRows),
	)
	result := qsruntime.RelationshipSiblingDiversityReadResult{
		Rows:              stats.Rows,
		Values:            stats.Values,
		CandidateRows:     stats.CandidateRows,
		ProjectionRows:    stats.ProjectionRows,
		TargetRows:        stats.TargetRows,
		Groups:            stats.Groups,
		DiverseGroups:     stats.DiverseGroups,
		CacheHit:          stats.CacheHit,
		Reason:            stats.SkipReason,
		LookupElapsed:     stats.LookupElapsed,
		BuildElapsed:      stats.BuildElapsed,
		ProjectionElapsed: stats.ProjectionElapsed,
		EvaluationElapsed: stats.EvaluationElapsed,
	}
	if err != nil || !ok {
		return result, nil, ok, err
	}
	candidateRows := make([]qsbridge.QuantaRownum, 0, len(rownums))
	for _, rownum := range rownums {
		candidateRows = append(candidateRows, qsbridge.QuantaRownum(rownum))
	}
	result.Candidates = qsbridge.QuantaCandidateSet{
		Index:   read.Index,
		Rownums: candidateRows,
	}
	result.Mode = stats.Mode
	if result.Mode == "" {
		result.Mode = "reverse_artifact_sibling_diversity"
	}
	return result, nil, true, nil
}

// ReadRelationshipVectorAggregate returns raw BSI aggregate state grouped by a
// schema-declared relationship vector.
func (r StandardRelationshipVectorAggregateReader) ReadRelationshipVectorAggregate(
	_ context.Context,
	read qsruntime.LegacyDirectRelationshipVectorAggregateRequest,
) (qsruntime.LegacyDirectRelationshipVectorAggregateResult, qsbridge.DiagnosticSet, bool, error) {
	if r.Direct == nil || read.VectorIndex == "" || read.VectorField == "" || read.ValueField == "" {
		return qsruntime.LegacyDirectRelationshipVectorAggregateResult{}, nil, false, nil
	}
	if read.ValueIndex != "" && !strings.EqualFold(read.ValueIndex, read.VectorIndex) {
		return qsruntime.LegacyDirectRelationshipVectorAggregateResult{}, nil, false, nil
	}
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.VectorIndex, read.FromEpochMillis, read.ToEpochMillis)
	if len(read.ChildRows) > 0 && len(read.ChildRows) == len(read.ParentRows) {
		groups, stats, ok, err := r.Direct.RelationshipAlignedValueSum(
			read.VectorIndex,
			read.ValueField,
			fromTime,
			toTime,
			standardRelationshipAggregateRows(read.ChildRows),
			standardRelationshipAggregateRows(read.ParentRows),
		)
		if err != nil || ok {
			return standardRelationshipAggregateResult(groups, stats, "aligned_relationship_sum"), nil, ok, err
		}
	}
	groups, stats, ok, err := r.Direct.RelationshipReverseArtifactSum(
		read.VectorIndex,
		read.VectorField,
		read.ValueField,
		fromTime,
		toTime,
		standardRelationshipAggregateRows(read.ChildRows),
		standardRelationshipAggregateRows(read.ParentRows),
	)
	if err != nil || !ok {
		return qsruntime.LegacyDirectRelationshipVectorAggregateResult{}, nil, ok, err
	}
	return standardRelationshipAggregateResult(groups, stats, "reverse_artifact_sum"), nil, true, nil
}

func standardRelationshipAggregateResult(groups []server.RelationshipReverseArtifactSumGroup, stats server.RelationshipReverseArtifactSumStats, mode string) qsruntime.LegacyDirectRelationshipVectorAggregateResult {
	resultGroups := make([]qsruntime.LegacyDirectRelationshipVectorAggregateGroup, 0, len(groups))
	for _, group := range groups {
		resultGroups = append(resultGroups, qsruntime.LegacyDirectRelationshipVectorAggregateGroup{
			ParentRow:              qsbridge.QuantaRownum(group.ParentValue),
			RepresentativeChildRow: qsbridge.QuantaRownum(group.RepresentativeRow),
			Count:                  group.Count,
			Sum:                    group.Sum,
		})
	}
	return qsruntime.LegacyDirectRelationshipVectorAggregateResult{
		Groups:            resultGroups,
		Mode:              mode,
		Rows:              stats.Rows,
		Values:            stats.Values,
		SourceValues:      stats.SourceValues,
		TargetRows:        stats.TargetRows,
		LookupElapsed:     stats.LookupElapsed,
		ProjectionElapsed: stats.ProjectionElapsed,
		AggregateElapsed:  stats.AggregateElapsed,
	}
}

// ReadRelationshipVectorProjection returns the requested relationship-vector BSI.
func (r StandardRelationshipVectorProjectionReader) ReadRelationshipVectorProjection(ctx context.Context, read qsruntime.LegacyDirectRelationshipVectorReadRequest) (*roaring64.BSI, qsbridge.DiagnosticSet, error) {
	if read.VectorIndex == "" || read.VectorField == "" {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "inabox-standard relationship-vector projection requires vector index and field"),
		}, nil
	}
	if r.Pool == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard relationship-vector projection has no session pool"),
		}, nil
	}
	foundSet := standardRelationshipVectorProjectionFoundSet(read)
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.VectorIndex, 0, 0)
	if r.Direct != nil {
		fkBSI, err := r.Direct.ProjectBSI(read.VectorIndex, read.VectorField, fromTime, toTime, foundSet, false)
		if err != nil {
			return nil, nil, err
		}
		if fkBSI == nil {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					fmt.Sprintf("inabox-standard relationship-vector projection did not return %s.%s", read.VectorIndex, read.VectorField),
				),
			}, nil
		}
		return fkBSI, nil, nil
	}
	session, err := r.Pool.Borrow(read.VectorIndex)
	if err != nil {
		return nil, nil, err
	}
	defer r.Pool.Return(read.VectorIndex, session)
	if session == nil || session.BitIndex == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard relationship-vector projection has no bitmap index"),
		}, nil
	}
	bsiByField, _, err := session.BitIndex.Projection(read.VectorIndex, []string{read.VectorField}, fromTime, toTime, foundSet, false)
	if err != nil {
		return nil, nil, err
	}
	fkBSI := bsiByField[read.VectorField]
	if fkBSI == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				fmt.Sprintf("inabox-standard relationship-vector projection did not return %s.%s", read.VectorIndex, read.VectorField),
			),
		}, nil
	}
	return fkBSI, nil, nil
}

func standardRelationshipVectorProjectionFoundSet(read qsruntime.LegacyDirectRelationshipVectorReadRequest) *roaring64.Bitmap {
	if len(read.SourceCandidates.Rownums) == 0 || read.SourceDomain != read.VectorIndex {
		return nil
	}
	foundSet := roaring64.NewBitmap()
	for _, rownum := range read.SourceCandidates.Rownums {
		foundSet.Add(uint64(rownum))
	}
	return foundSet
}

func standardRelationshipAggregateRows(rownums []qsbridge.QuantaRownum) []uint64 {
	rows := make([]uint64, 0, len(rownums))
	for _, rownum := range rownums {
		rows = append(rows, uint64(rownum))
	}
	return rows
}
