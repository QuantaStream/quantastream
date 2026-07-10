package qsbridge

import (
	"context"
	"strconv"
)

// ProjectionProbe is one low-cardinality materialization or projector-kernel observation.
//
// It is intentionally protocol-neutral so runtime adapters can expose timing,
// cache, batching, and legacy-compatibility details without importing qsruntime
// or legacy projector packages into qsbridge.
type ProjectionProbe struct {
	Section string
	Name    string
	Value   string
	Detail  string
}

// ProjectionBatchIntent names why a materialization request is batched.
type ProjectionBatchIntent string

const (
	// ProjectionBatchIntentUnspecified leaves batching to the runtime implementation.
	ProjectionBatchIntentUnspecified ProjectionBatchIntent = ""
	// ProjectionBatchIntentTimeToFirstByte favors early result chunks over full materialization.
	ProjectionBatchIntentTimeToFirstByte ProjectionBatchIntent = "time_to_first_byte"
	// ProjectionBatchIntentBoundedWork favors predictable memory and work units.
	ProjectionBatchIntentBoundedWork ProjectionBatchIntent = "bounded_work"
)

// ProjectionBatch describes one materialization batch.
//
// Size is the requested maximum row count, Sequence identifies the batch in a
// forward-only stream, and Final says no later batch is expected. Intent keeps
// time-to-first-byte policy explicit instead of hiding it behind a
// Projector.Next-style call.
type ProjectionBatch struct {
	Size     int
	Sequence int
	Final    bool
	Intent   ProjectionBatchIntent
}

// TimeToFirstByte reports whether this batch is shaped for early client output.
func (b ProjectionBatch) TimeToFirstByte() bool {
	return b.Intent == ProjectionBatchIntentTimeToFirstByte
}

// ProjectionMaterializer fetches projection values for candidate rownums.
//
// Implementations may call in-process native kernels, a remote node adapter, or
// a temporary compatibility bridge. The contract remains columnar and
// late-materialization oriented: candidates are supplied as rownums, fields are
// requested explicitly, and returned values stay in projection vectors until
// the executor chooses to assemble result rows.
type ProjectionMaterializer interface {
	Materialize(ctx context.Context, request QuantaMaterializationRequest) (QuantaProjectedRowSet, DiagnosticSet, error)
}

// ProjectionMaterializerWithProbes reports materialization observations.
//
// This optional extension keeps instrumentation visible at the native contract
// boundary. Runtime implementations should use it to expose field hydration,
// rehydration, batching, cache, and legacy fallback costs.
type ProjectionMaterializerWithProbes interface {
	MaterializeWithProbes(ctx context.Context, request QuantaMaterializationRequest) (QuantaProjectedRowSet, DiagnosticSet, []ProjectionProbe, error)
}

// ProjectionMaterializerFunc adapts a function to ProjectionMaterializer.
type ProjectionMaterializerFunc func(ctx context.Context, request QuantaMaterializationRequest) (QuantaProjectedRowSet, DiagnosticSet, error)

// Materialize calls f(ctx, request).
func (f ProjectionMaterializerFunc) Materialize(ctx context.Context, request QuantaMaterializationRequest) (QuantaProjectedRowSet, DiagnosticSet, error) {
	return f(ctx, request)
}

// ProjectionMaterializationKernelRequest groups materialization reads for one projector step.
type ProjectionMaterializationKernelRequest struct {
	ID          string
	ProbePrefix string
	Requests    []QuantaMaterializationRequest
}

// RequestCount reports how many materialization reads are grouped in this kernel request.
func (r ProjectionMaterializationKernelRequest) RequestCount() int {
	return len(r.Requests)
}

// ProjectionMaterializationResult is one materialized batch response.
type ProjectionMaterializationResult struct {
	ID          string
	Request     QuantaMaterializationRequest
	RowSet      QuantaProjectedRowSet
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// ProjectionMaterializationKernelResult carries all materialized batches for one projector step.
type ProjectionMaterializationKernelResult struct {
	ID          string
	Results     []ProjectionMaterializationResult
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// RowSets returns copied row sets in kernel result order.
func (r ProjectionMaterializationKernelResult) RowSets() []QuantaProjectedRowSet {
	rowSets := make([]QuantaProjectedRowSet, 0, len(r.Results))
	for _, result := range r.Results {
		rowSets = append(rowSets, cloneProjectedRowSet(result.RowSet))
	}
	return rowSets
}

// ProjectionMaterializationKernel materializes projector batches without assuming a storage backend.
type ProjectionMaterializationKernel interface {
	MaterializeProjectionBatches(context.Context, ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error)
}

// ProjectionResultAssemblyRequest carries materialized rowsets into result assembly.
type ProjectionResultAssemblyRequest struct {
	ID          string
	ProbePrefix string
	RowSets     []QuantaProjectedRowSet
}

// ProjectionResultAssemblyResult is the native rowset-to-result assembly output.
type ProjectionResultAssemblyResult struct {
	ID          string
	RowSet      QuantaProjectedRowSet
	Chunks      []ResultChunk
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// AssembleProjectionMaterializationResult zips materialized rowsets into one assembly result.
func AssembleProjectionMaterializationResult(request ProjectionResultAssemblyRequest) ProjectionResultAssemblyResult {
	probePrefix := request.ProbePrefix
	if probePrefix == "" {
		probePrefix = "projection_assembly_"
	}
	result := ProjectionResultAssemblyResult{
		ID: request.ID,
		Probes: []ProjectionProbe{{
			Section: "projection_assembly",
			Name:    probePrefix + "rowset_count",
			Value:   strconv.Itoa(len(request.RowSets)),
		}},
	}
	for i, rowSet := range request.RowSets {
		rowSet = cloneProjectedRowSet(rowSet)
		result.RowSet = appendProjectedRowSet(result.RowSet, rowSet)
		chunk, diagnostics := rowSet.ToResultChunk(i, i == len(request.RowSets)-1)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if diagnostics.BlocksNative() {
			continue
		}
		result.Chunks = append(result.Chunks, chunk)
	}
	result.Diagnostics = append(result.Diagnostics, result.RowSet.ValidateShape()...)
	result.Probes = append(result.Probes, ProjectionProbe{
		Section: "projection_assembly",
		Name:    probePrefix + "assembled_rows",
		Value:   strconv.Itoa(result.RowSet.CandidateCount()),
	})
	return result
}

// CandidateSetFromBitmapResult builds a late-materialization candidate set from a bitmap result.
func CandidateSetFromBitmapResult(index string, result QuantaBitmapQueryResult) QuantaCandidateSet {
	return QuantaCandidateSet{
		Index:   index,
		Rownums: append([]QuantaRownum(nil), result.Rownums...),
	}
}

func cloneProjectedRowSet(rowSet QuantaProjectedRowSet) QuantaProjectedRowSet {
	rowSet.Rownums = append([]QuantaRownum(nil), rowSet.Rownums...)
	rowSet.ProjectionVectors = cloneProjectionVectors(rowSet.ProjectionVectors)
	return rowSet
}

func cloneProjectionVectors(vectors []QuantaProjectionVector) []QuantaProjectionVector {
	if len(vectors) == 0 {
		return nil
	}
	cloned := make([]QuantaProjectionVector, len(vectors))
	for i, vector := range vectors {
		cloned[i] = vector
		cloned[i].Values = append([]ResultCell(nil), vector.Values...)
	}
	return cloned
}

func appendProjectedRowSet(left, right QuantaProjectedRowSet) QuantaProjectedRowSet {
	if left.Index == "" {
		return cloneProjectedRowSet(right)
	}
	left.Rownums = append(left.Rownums, right.Rownums...)
	for _, rightVector := range right.ProjectionVectors {
		index := projectionVectorIndex(left.ProjectionVectors, rightVector.Field)
		if index == -1 {
			left.ProjectionVectors = append(left.ProjectionVectors, QuantaProjectionVector{
				Field:  rightVector.Field,
				Values: append([]ResultCell(nil), rightVector.Values...),
			})
			continue
		}
		left.ProjectionVectors[index].Values = append(left.ProjectionVectors[index].Values, rightVector.Values...)
	}
	return left
}

func projectionVectorIndex(vectors []QuantaProjectionVector, field QuantaProjectionField) int {
	for i, vector := range vectors {
		if vector.Field.Index == field.Index &&
			vector.Field.Role == field.Role &&
			vector.Field.Field == field.Field &&
			vector.Field.PhysicalName == field.PhysicalName {
			return i
		}
	}
	return -1
}
