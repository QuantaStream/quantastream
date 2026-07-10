package qsfixture

import (
	"context"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ProjectionMaterializationFixtureKernel materializes candidate rownums from in-memory row values.
type ProjectionMaterializationFixtureKernel struct {
	rows map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell
}

var _ qsbridge.ProjectionMaterializationKernel = ProjectionMaterializationFixtureKernel{}

// NewProjectionMaterializationFixtureKernel creates a deterministic materialization kernel.
func NewProjectionMaterializationFixtureKernel(rows map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell) ProjectionMaterializationFixtureKernel {
	return ProjectionMaterializationFixtureKernel{
		rows: cloneProjectionMaterializationRows(rows),
	}
}

// MaterializeProjectionBatches materializes every grouped request into columnar row sets.
func (k ProjectionMaterializationFixtureKernel) MaterializeProjectionBatches(ctx context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
	result := qsbridge.ProjectionMaterializationKernelResult{
		ID: request.ID,
		Probes: []qsbridge.ProjectionProbe{{
			Section: "projection_materialization",
			Name:    request.ProbePrefix + "request_count",
			Value:   strconv.Itoa(len(request.Requests)),
		}},
	}
	for _, materialization := range request.Requests {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		materialized := k.materialize(materialization)
		result.Results = append(result.Results, materialized)
		result.Diagnostics = append(result.Diagnostics, materialized.Diagnostics...)
	}
	result.Probes = append(result.Probes, qsbridge.ProjectionProbe{
		Section: "projection_materialization",
		Name:    request.ProbePrefix + "result_count",
		Value:   strconv.Itoa(len(result.Results)),
	})
	return result, nil
}

func (k ProjectionMaterializationFixtureKernel) materialize(request qsbridge.QuantaMaterializationRequest) qsbridge.ProjectionMaterializationResult {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:        request.Index,
		LogicalShard: request.LogicalShard,
		Replica:      request.Replica,
		DependencyID: request.DependencyID,
		Batch:        request.Batch,
		Rownums:      append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	}
	result := qsbridge.ProjectionMaterializationResult{
		ID:      request.DependencyID,
		Request: request,
		RowSet:  rowSet,
		Probes: []qsbridge.ProjectionProbe{{
			Section: "projection_materialization",
			Name:    request.ProbePrefix + "candidate_count",
			Value:   strconv.Itoa(len(request.Rownums)),
		}},
	}
	tableRows := k.rows[request.Index]
	for _, field := range request.ProjectionFields {
		vector := qsbridge.QuantaProjectionVector{Field: field}
		for _, rownum := range request.Rownums {
			cell, ok := tableRows[rownum][projectionMaterializationFieldKey(field)]
			if !ok {
				cell = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
				result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					"projection materialization fixture has no value for "+request.Index+"."+projectionMaterializationFieldKey(field),
				))
			}
			vector.Values = append(vector.Values, cell)
		}
		result.RowSet.ProjectionVectors = append(result.RowSet.ProjectionVectors, vector)
	}
	result.Diagnostics = append(result.Diagnostics, result.RowSet.ValidateShape()...)
	return result
}

func projectionMaterializationFieldKey(field qsbridge.QuantaProjectionField) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Field
}

func cloneProjectionMaterializationRows(rows map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell) map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell {
	cloned := make(map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell, len(rows))
	for table, tableRows := range rows {
		cloned[table] = make(map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell, len(tableRows))
		for rownum, values := range tableRows {
			cloned[table][rownum] = make(map[string]qsbridge.ResultCell, len(values))
			for field, cell := range values {
				cloned[table][rownum][field] = cell
			}
		}
	}
	return cloned
}
