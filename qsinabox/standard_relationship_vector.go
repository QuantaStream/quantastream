package qsinabox

import (
	"context"
	"fmt"

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
	Direct *server.BitmapIndex
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
	rownums, stats, ok, err := r.Direct.RelationshipReverseArtifactCandidates(read.VectorIndex, read.VectorField, sourceValues)
	if err != nil || !ok {
		return qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult{}, nil, ok, err
	}
	candidateRows := make([]qsbridge.QuantaRownum, 0, len(rownums))
	for _, rownum := range rownums {
		candidateRows = append(candidateRows, qsbridge.QuantaRownum(rownum))
	}
	return qsruntime.LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
		Candidates: qsbridge.QuantaCandidateSet{
			Index:   read.TargetDomain,
			Rownums: candidateRows,
		},
		Mode:          "reverse_artifact_server",
		CacheHit:      true,
		Rows:          stats.Rows,
		Values:        stats.Values,
		SourceValues:  stats.SourceValues,
		TargetRows:    stats.TargetRows,
		LookupElapsed: stats.LookupElapsed,
	}, nil, true, nil
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
