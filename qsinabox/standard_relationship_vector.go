package qsinabox

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// StandardRelationshipVectorProjectionReader projects relationship-vector FK
// BSIs through the in-process inabox-standard session pool.
type StandardRelationshipVectorProjectionReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
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
	foundSet := standardRelationshipVectorProjectionFoundSet(read)
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.VectorIndex, 0, 0)
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
