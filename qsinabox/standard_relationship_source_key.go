package qsinabox

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
)

// StandardRelationshipVectorSourceKeyReader projects source-domain key values
// for local relationship-vector expansion.
type StandardRelationshipVectorSourceKeyReader struct {
	Reader StandardProjectionBSIReader
}

var _ qsruntime.LegacyDirectRelationshipVectorSourceKeyReader = StandardRelationshipVectorSourceKeyReader{}

// ReadRelationshipVectorSourceKeyValues returns source key values aligned to
// the requested source rownums.
func (r StandardRelationshipVectorSourceKeyReader) ReadRelationshipVectorSourceKeyValues(ctx context.Context, read qsruntime.LegacyDirectRelationshipVectorReadRequest) ([]int64, qsbridge.DiagnosticSet, error) {
	if len(read.SourceCandidates.Rownums) == 0 {
		return nil, nil, nil
	}
	field, ok := standardRelationshipVectorSourceKeyField(read)
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "standard relationship-vector source-key reader cannot find source key field"),
		}, nil
	}
	index := field.Table.Table
	physicalField := field.PhysicalName
	if physicalField == "" {
		physicalField = field.Name
	}
	if index == "" || physicalField == "" {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "standard relationship-vector source-key reader requires a source key table and field"),
		}, nil
	}
	values, diagnostics, err := r.Reader.ReadProjectionBSIValues(ctx, []qsruntime.NativeProjectionBSIReadRequest{{
		Index:         index,
		Field:         standardRelationshipVectorSourceProjectionField(field),
		PhysicalField: physicalField,
		Rownums:       append([]qsbridge.QuantaRownum(nil), read.SourceCandidates.Rownums...),
	}})
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if len(values) != 1 {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("standard relationship-vector source-key reader returned %d field reads for 1 request", len(values))),
		}, nil
	}
	aligned := values[0].Values
	if len(aligned) != len(read.SourceCandidates.Rownums) {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("standard relationship-vector source-key reader returned %d values for %d rownums", len(aligned), len(read.SourceCandidates.Rownums))),
		}, nil
	}
	out := make([]int64, 0, len(aligned))
	for _, value := range aligned {
		if value == nil {
			continue
		}
		out = append(out, value.Int64())
	}
	return out, nil, nil
}

func standardRelationshipVectorSourceKeyField(read qsruntime.LegacyDirectRelationshipVectorReadRequest) (qsbridge.FieldRef, bool) {
	if strings.EqualFold(read.Edge.Left.Table.Table, read.SourceDomain) {
		return read.Edge.Left, true
	}
	if strings.EqualFold(read.Edge.Right.Table.Table, read.SourceDomain) {
		return read.Edge.Right, true
	}
	return qsbridge.FieldRef{}, false
}

func standardRelationshipVectorSourceProjectionField(field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	physical := field.PhysicalName
	if physical == "" {
		physical = field.Name
	}
	return qsbridge.QuantaProjectionField{
		Index:        field.Table.Table,
		Field:        physical,
		PhysicalName: physical,
		Type:         field.Type,
		Role:         qsbridge.TableInstanceID(field.Table.RefName()),
	}
}
