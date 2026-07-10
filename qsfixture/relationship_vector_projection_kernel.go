package qsfixture

import (
	"context"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RelationshipVectorProjectionFixtureKernel evaluates vector projection reads against in-memory mappings.
type RelationshipVectorProjectionFixtureKernel struct {
	vectors map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum
}

var _ qsbridge.RelationshipVectorProjectionKernel = RelationshipVectorProjectionFixtureKernel{}

// NewRelationshipVectorProjectionFixtureKernel creates a deterministic vector projection kernel.
func NewRelationshipVectorProjectionFixtureKernel(vectors map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum) RelationshipVectorProjectionFixtureKernel {
	return RelationshipVectorProjectionFixtureKernel{
		vectors: cloneRelationshipVectorMap(vectors),
	}
}

// LoadRelationshipVectorProjections maps each read's input rownums into its output rownum domain.
func (k RelationshipVectorProjectionFixtureKernel) LoadRelationshipVectorProjections(ctx context.Context, request qsbridge.RelationshipVectorProjectionKernelRequest) (qsbridge.RelationshipVectorProjectionKernelResult, error) {
	result := qsbridge.RelationshipVectorProjectionKernelResult{
		ID: request.ID,
		Probes: []qsbridge.ProjectionProbe{{
			Section: "relationship_vector_projection",
			Name:    request.ProbePrefix + "read_count",
			Value:   strconv.Itoa(len(request.Reads)),
		}},
	}
	for _, read := range request.Reads {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		projected, diagnostics := k.project(read)
		result.Results = append(result.Results, projected)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
	}
	result.Probes = append(result.Probes, qsbridge.ProjectionProbe{
		Section: "relationship_vector_projection",
		Name:    request.ProbePrefix + "result_count",
		Value:   strconv.Itoa(len(result.Results)),
	})
	return result, nil
}

func (k RelationshipVectorProjectionFixtureKernel) project(read qsbridge.RelationshipVectorProjectionRead) (qsbridge.RelationshipVectorProjectionResult, qsbridge.DiagnosticSet) {
	result := qsbridge.RelationshipVectorProjectionResult{
		ID:          read.ID,
		Input:       read.Input,
		Translation: read.Translation,
		Output: qsbridge.RownumDomainSet{
			Domain: read.OutputDomain,
		},
	}
	vector, ok := k.vectors[read.ID]
	if !ok {
		return result, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"relationship-vector projection fixture has no vector mapping for "+read.ID,
			),
		}
	}
	seen := map[qsbridge.QuantaRownum]bool{}
	for _, source := range read.Input.Rownums {
		for _, target := range vector[source] {
			if seen[target] {
				continue
			}
			seen[target] = true
			result.Output.Rownums = append(result.Output.Rownums, target)
		}
	}
	result.Probes = []qsbridge.ProjectionProbe{{
		Section: "relationship_vector_projection",
		Name:    read.ProbePrefix + "output_count",
		Value:   strconv.Itoa(result.Output.CandidateCount()),
	}}
	return result, nil
}
