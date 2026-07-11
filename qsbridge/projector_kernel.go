package qsbridge

import (
	"context"
	"strconv"
	"strings"
)

// ProjectorKernelStageKind names one executor primitive in the projector lifecycle.
type ProjectorKernelStageKind string

const (
	// ProjectorKernelSeedCandidates starts from one or more predicate-produced candidate sets.
	ProjectorKernelSeedCandidates ProjectorKernelStageKind = "seed_candidates"
	// ProjectorKernelLoadRelationshipVectors retrieves relationship/vector BSI data needed by joins.
	ProjectorKernelLoadRelationshipVectors ProjectorKernelStageKind = "load_relationship_vectors"
	// ProjectorKernelBatchCandidates slices candidate rownums for time-to-first-byte and bounded work.
	ProjectorKernelBatchCandidates ProjectorKernelStageKind = "batch_candidates"
	// ProjectorKernelMaterializeFields retrieves BSI, bitmap, or dictionary-backed field values.
	ProjectorKernelMaterializeFields ProjectorKernelStageKind = "materialize_fields"
	// ProjectorKernelRehydrateValues converts encoded values back to SQL-visible values.
	ProjectorKernelRehydrateValues ProjectorKernelStageKind = "rehydrate_values"
	// ProjectorKernelAssembleRows zips columnar projected values into result rows.
	ProjectorKernelAssembleRows ProjectorKernelStageKind = "assemble_rows"
	// ProjectorKernelAggregateRows applies grouped or global aggregation after candidate materialization.
	ProjectorKernelAggregateRows ProjectorKernelStageKind = "aggregate_rows"
	// ProjectorKernelRankRows applies top-N style rank assembly.
	ProjectorKernelRankRows ProjectorKernelStageKind = "rank_rows"
)

// ProjectorRelationshipVectorDependency describes one relationship vector needed for projection or joins.
//
// This is the executor-side replacement vocabulary for the relationship BSI
// work that legacy core.Projector kept internally. Runtime adapters can map it
// to in-process bitmap calls, inabox-direct calls, or future native execution
// without making qsbridge import core.
type ProjectorRelationshipVectorDependency struct {
	ID          string
	ProbeName   string
	Edge        RelationshipJoinPlanEdge
	Intent      RelationshipJoinOperationIntent
	Child       FieldRef
	Parent      FieldRef
	Translation RownumDomainTranslation
	Coverage    RelationshipVectorProjectionCoveragePlan
}

// ProjectorMaterializationDependency describes one projected field fetch over a candidate set.
type ProjectorMaterializationDependency struct {
	ID         string
	ProbeName  string
	Field      QuantaProjectionField
	Candidate  string
	Rehydrates bool
}

// ProjectorMaterializationRequestPlan describes one grouped materialization read.
type ProjectorMaterializationRequestPlan struct {
	ID           string
	Candidate    string
	ProbePrefix  string
	Request      QuantaMaterializationRequest
	Dependencies []ProjectorMaterializationDependency
}

// ProjectorCandidateBatch describes one bounded candidate window for projector work.
type ProjectorCandidateBatch struct {
	ID          string
	Candidate   string
	ProbePrefix string
	Set         QuantaCandidateSet
	Batch       ProjectionBatch
}

// ProjectorKernelExecutionStep describes one ordered projector executor handoff.
type ProjectorKernelExecutionStep struct {
	ID                           string
	Kind                         ProjectorKernelStageKind
	ProbePrefix                  string
	RelationshipVectorProjection RelationshipVectorProjectionKernelRequest
	CandidateBatches             []ProjectorCandidateBatch
	MaterializationKernel        ProjectionMaterializationKernelRequest
	Materialization              []ProjectorMaterializationRequestPlan
}

// ProjectorKernelExecutionPlan sketches the runnable projector lifecycle without touching storage.
type ProjectorKernelExecutionPlan struct {
	Driver string
	Stages []ProjectorKernelExecutionStep
}

// ProjectorKernelRuntime supplies the storage-facing kernels used by a projector execution plan.
type ProjectorKernelRuntime struct {
	RelationshipVectors RelationshipVectorProjectionKernel
	Materialization     ProjectionMaterializationKernel
}

// ProjectorKernelExecutionResult is the protocol-neutral output of one projector lifecycle run.
type ProjectorKernelExecutionResult struct {
	Plan                ProjectorKernelExecutionPlan
	RelationshipVectors RelationshipVectorProjectionKernelResult
	Materialization     ProjectionMaterializationKernelResult
	Assembly            ProjectionResultAssemblyResult
	RowSet              QuantaProjectedRowSet
	Chunks              []ResultChunk
	Probes              []ProjectionProbe
	Diagnostics         DiagnosticSet
}

// ProjectorKernelPlan describes the executor primitives formerly hidden inside core.Projector.
//
// The plan is intentionally dependency-light: it carries candidate rownums,
// relationship-vector dependencies, projection fields, and stage ordering, but
// not bitmap or BSI implementation objects. That keeps SQL meaning in qsbridge
// while leaving storage calls to runtime adapters.
type ProjectorKernelPlan struct {
	Driver          string
	Candidates      []QuantaCandidateSet
	Relationship    []ProjectorRelationshipVectorDependency
	Materialization []ProjectorMaterializationDependency
	BatchSize       int
	BatchIntent     ProjectionBatchIntent
	FromEpochMillis int64
	ToEpochMillis   int64
	Aggregate       bool
	Rank            bool
}

// ProjectorKernelSpec is the construction input for a ProjectorKernelPlan.
type ProjectorKernelSpec struct {
	Driver              string
	Candidates          []QuantaCandidateSet
	RelationshipPlan    RelationshipJoinPlan
	ProjectionFields    []QuantaProjectionField
	RehydrationFields   map[string]bool
	BatchSize           int
	BatchIntent         ProjectionBatchIntent
	FromEpochMillis     int64
	ToEpochMillis       int64
	RequiresAggregation bool
	RequiresRanking     bool
}

// BuildProjectorKernelPlan turns predicate and join outputs into projector execution stages.
func BuildProjectorKernelPlan(spec ProjectorKernelSpec) ProjectorKernelPlan {
	candidates := cloneQuantaCandidateSets(spec.Candidates)
	driver := spec.Driver
	if driver == "" && len(candidates) > 0 {
		driver = candidates[0].Index
	}
	plan := ProjectorKernelPlan{
		Driver:          driver,
		Candidates:      candidates,
		Relationship:    projectorRelationshipDependencies(spec.RelationshipPlan),
		Materialization: projectorMaterializationDependencies(driver, spec.ProjectionFields, spec.RehydrationFields),
		BatchSize:       spec.BatchSize,
		BatchIntent:     spec.BatchIntent,
		FromEpochMillis: spec.FromEpochMillis,
		ToEpochMillis:   spec.ToEpochMillis,
		Aggregate:       spec.RequiresAggregation,
		Rank:            spec.RequiresRanking,
	}
	return plan
}

// ExecuteProjectorKernelPlan runs the ordered projector handoffs against neutral kernels.
func ExecuteProjectorKernelPlan(ctx context.Context, plan ProjectorKernelPlan, runtime ProjectorKernelRuntime, inputs map[string]RownumDomainSet) (ProjectorKernelExecutionResult, error) {
	execution := plan.ExecutionPlan(inputs)
	result := ProjectorKernelExecutionResult{Plan: execution}
	domainInputs := cloneRownumDomainSetMap(inputs)
	for _, stage := range execution.Stages {
		switch stage.Kind {
		case ProjectorKernelLoadRelationshipVectors:
			if runtime.RelationshipVectors == nil {
				result.Diagnostics = append(result.Diagnostics, ErrorDiagnostic(
					DiagnosticUnsupportedSQL,
					PhaseExecute,
					"projector relationship-vector kernel is not wired yet",
				))
				return result, nil
			}
			vectorResult, err := runtime.RelationshipVectors.LoadRelationshipVectorProjections(ctx, stage.RelationshipVectorProjection)
			result.RelationshipVectors = vectorResult
			result.Probes = append(result.Probes, vectorResult.Probes...)
			result.Diagnostics = append(result.Diagnostics, vectorResult.Diagnostics...)
			for _, item := range vectorResult.Results {
				result.Probes = append(result.Probes, item.Probes...)
				result.Diagnostics = append(result.Diagnostics, item.Diagnostics...)
			}
			for name, output := range vectorResult.OutputDomainSets() {
				domainInputs[name] = output
			}
			if err != nil || result.Diagnostics.BlocksNative() {
				return result, err
			}
		case ProjectorKernelMaterializeFields:
			if runtime.Materialization == nil {
				result.Diagnostics = append(result.Diagnostics, ErrorDiagnostic(
					DiagnosticUnsupportedSQL,
					PhaseExecute,
					"projector materialization kernel is not wired yet",
				))
				return result, nil
			}
			materializationResult, err := runtime.Materialization.MaterializeProjectionBatches(ctx, stage.MaterializationKernel)
			result.Materialization = materializationResult
			result.Probes = append(result.Probes, materializationResult.Probes...)
			result.Diagnostics = append(result.Diagnostics, materializationResult.Diagnostics...)
			for _, item := range materializationResult.Results {
				result.Probes = append(result.Probes, item.Probes...)
				result.Diagnostics = append(result.Diagnostics, item.Diagnostics...)
			}
			if err != nil || result.Diagnostics.BlocksNative() {
				return result, err
			}
		case ProjectorKernelAssembleRows:
			assembly := AssembleProjectionMaterializationResult(ProjectionResultAssemblyRequest{
				ID:          "projection_assembly",
				ProbePrefix: "projection_assembly_",
				RowSets:     result.Materialization.RowSets(),
			})
			result.Assembly = assembly
			result.RowSet = assembly.RowSet
			result.Chunks = append(result.Chunks, assembly.Chunks...)
			result.Probes = append(result.Probes, assembly.Probes...)
			result.Diagnostics = append(result.Diagnostics, assembly.Diagnostics...)
			if result.Diagnostics.BlocksNative() {
				return result, nil
			}
		}
	}
	_ = domainInputs
	return result, nil
}

// StageKinds returns the ordered executor primitives required by this plan.
func (p ProjectorKernelPlan) StageKinds() []ProjectorKernelStageKind {
	stages := []ProjectorKernelStageKind{ProjectorKernelSeedCandidates}
	if len(p.Relationship) > 0 {
		stages = append(stages, ProjectorKernelLoadRelationshipVectors)
	}
	stages = append(stages, ProjectorKernelBatchCandidates)
	if len(p.Materialization) > 0 {
		stages = append(stages, ProjectorKernelMaterializeFields)
	}
	if p.NeedsRehydration() {
		stages = append(stages, ProjectorKernelRehydrateValues)
	}
	stages = append(stages, ProjectorKernelAssembleRows)
	if p.Aggregate {
		stages = append(stages, ProjectorKernelAggregateRows)
	}
	if p.Rank {
		stages = append(stages, ProjectorKernelRankRows)
	}
	return stages
}

// ExecutionPlan expands stage kinds into the concrete handoffs a projector executor would call.
func (p ProjectorKernelPlan) ExecutionPlan(inputs map[string]RownumDomainSet) ProjectorKernelExecutionPlan {
	stages := make([]ProjectorKernelExecutionStep, 0, len(p.StageKinds()))
	for _, kind := range p.StageKinds() {
		step := ProjectorKernelExecutionStep{
			ID:          projectorExecutionStepID(kind),
			Kind:        kind,
			ProbePrefix: projectorProbeName(projectorExecutionStepID(kind)) + "_",
		}
		switch kind {
		case ProjectorKernelLoadRelationshipVectors:
			step.RelationshipVectorProjection = p.RelationshipVectorProjectionKernelRequest(inputs)
		case ProjectorKernelBatchCandidates:
			step.CandidateBatches = p.CandidateBatches()
		case ProjectorKernelMaterializeFields:
			step.MaterializationKernel = p.ProjectionMaterializationKernelRequest()
			step.Materialization = p.MaterializationBatchRequestPlans()
		}
		stages = append(stages, step)
	}
	return ProjectorKernelExecutionPlan{
		Driver: p.Driver,
		Stages: stages,
	}
}

// NeedsRelationshipVectors reports whether this plan must retrieve relationship/vector data.
func (p ProjectorKernelPlan) NeedsRelationshipVectors() bool {
	return len(p.Relationship) > 0
}

// NeedsRehydration reports whether any materialized field needs dictionary or string rehydration.
func (p ProjectorKernelPlan) NeedsRehydration() bool {
	for _, dependency := range p.Materialization {
		if dependency.Rehydrates {
			return true
		}
	}
	return false
}

// MaterializationRequestPlans builds one planned late-materialization read per candidate set.
func (p ProjectorKernelPlan) MaterializationRequestPlans() []ProjectorMaterializationRequestPlan {
	dependenciesByCandidate := make(map[string][]ProjectorMaterializationDependency)
	for _, dependency := range p.Materialization {
		dependenciesByCandidate[dependency.Candidate] = append(dependenciesByCandidate[dependency.Candidate], dependency)
	}

	plans := make([]ProjectorMaterializationRequestPlan, 0, len(p.Candidates))
	for _, candidate := range p.Candidates {
		dependencies := dependenciesByCandidate[candidate.Index]
		if len(dependencies) == 0 && candidate.Index == p.Driver {
			dependencies = dependenciesByCandidate[""]
		}
		if len(dependencies) == 0 {
			continue
		}
		fields := make([]QuantaProjectionField, 0, len(dependencies))
		for _, dependency := range dependencies {
			fields = append(fields, dependency.Field)
		}
		id := projectorMaterializationRequestID(len(plans)+1, candidate.Index)
		probePrefix := projectorProbeName(id) + "_"
		request := candidate.MaterializationRequest(fields)
		request.DependencyID = id
		request.ProbePrefix = probePrefix
		request.Batch = ProjectionBatch{Size: p.BatchSize, Sequence: len(plans), Intent: p.BatchIntent}
		request.FromEpochMillis = p.FromEpochMillis
		request.ToEpochMillis = p.ToEpochMillis
		plans = append(plans, ProjectorMaterializationRequestPlan{
			ID:           id,
			Candidate:    candidate.Index,
			ProbePrefix:  probePrefix,
			Request:      request,
			Dependencies: append([]ProjectorMaterializationDependency(nil), dependencies...),
		})
	}
	if len(plans) > 0 {
		plans[len(plans)-1].Request.Batch.Final = true
	}
	return plans
}

// MaterializationRequests builds one late-materialization request per candidate set.
func (p ProjectorKernelPlan) MaterializationRequests() []QuantaMaterializationRequest {
	plans := p.MaterializationRequestPlans()
	requests := make([]QuantaMaterializationRequest, 0, len(plans))
	for _, plan := range plans {
		requests = append(requests, plan.Request)
	}
	return requests
}

// RelationshipVectorProjectionKernelRequest builds the vector-load request for projector work.
func (p ProjectorKernelPlan) RelationshipVectorProjectionKernelRequest(inputs map[string]RownumDomainSet) RelationshipVectorProjectionKernelRequest {
	return RelationshipVectorProjectionKernelRequest{
		ID:          "relationship_vector_projection",
		ProbePrefix: "relationship_vector_projection_",
		Reads:       p.RelationshipVectorProjectionReads(inputs),
	}
}

// RelationshipVectorProjectionReads builds projection reads for planned relationship dependencies.
func (p ProjectorKernelPlan) RelationshipVectorProjectionReads(inputs map[string]RownumDomainSet) []RelationshipVectorProjectionRead {
	reads := make([]RelationshipVectorProjectionRead, 0, len(p.Relationship))
	for _, dependency := range p.Relationship {
		input := inputs[dependency.Translation.From.Name()]
		if input.Domain.Name() == "" {
			input.Domain = dependency.Translation.From
		}
		reads = append(reads, RelationshipVectorProjectionRead{
			ID:              dependency.ID,
			ProbePrefix:     dependency.ProbeName + "_",
			Edge:            dependency.Edge,
			Intent:          dependency.Intent,
			Input:           input,
			OutputDomain:    dependency.Translation.To,
			Translation:     dependency.Translation,
			ProjectionScope: relationshipVectorProjectionScope(dependency.Edge),
			CoveragePlan:    dependency.Coverage,
			Cacheable:       true,
			FromEpochMillis: p.FromEpochMillis,
			ToEpochMillis:   p.ToEpochMillis,
		})
	}
	return reads
}

// MaterializationBatchRequestPlans builds late-materialization reads over candidate batches.
func (p ProjectorKernelPlan) MaterializationBatchRequestPlans() []ProjectorMaterializationRequestPlan {
	dependenciesByCandidate := p.materializationDependenciesByCandidate()
	plans := []ProjectorMaterializationRequestPlan{}
	batches := p.CandidateBatches()
	for _, batch := range batches {
		dependencies := dependenciesByCandidate[batch.Candidate]
		if len(dependencies) == 0 && batch.Candidate == p.Driver {
			dependencies = dependenciesByCandidate[""]
		}
		if len(dependencies) == 0 {
			continue
		}
		fields := projectorMaterializationFields(dependencies)
		id := projectorMaterializationBatchRequestID(len(plans)+1, batch.Candidate)
		probePrefix := projectorProbeName(id) + "_"
		request := batch.Set.MaterializationRequest(fields)
		request.DependencyID = id
		request.ProbePrefix = probePrefix
		request.Batch = batch.Batch
		request.FromEpochMillis = p.FromEpochMillis
		request.ToEpochMillis = p.ToEpochMillis
		plans = append(plans, ProjectorMaterializationRequestPlan{
			ID:           id,
			Candidate:    batch.Candidate,
			ProbePrefix:  probePrefix,
			Request:      request,
			Dependencies: append([]ProjectorMaterializationDependency(nil), dependencies...),
		})
	}
	return plans
}

// MaterializationBatchRequests builds one late-materialization request per candidate batch.
func (p ProjectorKernelPlan) MaterializationBatchRequests() []QuantaMaterializationRequest {
	plans := p.MaterializationBatchRequestPlans()
	requests := make([]QuantaMaterializationRequest, 0, len(plans))
	for _, plan := range plans {
		requests = append(requests, plan.Request)
	}
	return requests
}

// ProjectionMaterializationKernelRequest builds the grouped materialization handoff for projector work.
func (p ProjectorKernelPlan) ProjectionMaterializationKernelRequest() ProjectionMaterializationKernelRequest {
	return ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests:    p.MaterializationBatchRequests(),
	}
}

// CandidateBatches slices candidate rownums into stable projector work units.
func (p ProjectorKernelPlan) CandidateBatches() []ProjectorCandidateBatch {
	batches := []ProjectorCandidateBatch{}
	for _, candidate := range p.Candidates {
		size := p.BatchSize
		if size <= 0 || size > len(candidate.Rownums) {
			size = len(candidate.Rownums)
		}
		if size == 0 {
			size = 1
		}
		for start := 0; start < len(candidate.Rownums) || (len(candidate.Rownums) == 0 && start == 0); start += size {
			end := start + size
			if end > len(candidate.Rownums) {
				end = len(candidate.Rownums)
			}
			window := candidate
			window.Rownums = append([]QuantaRownum(nil), candidate.Rownums[start:end]...)
			id := projectorCandidateBatchID(len(batches)+1, candidate.Index)
			batches = append(batches, ProjectorCandidateBatch{
				ID:          id,
				Candidate:   candidate.Index,
				ProbePrefix: projectorProbeName(id) + "_",
				Set:         window,
				Batch: ProjectionBatch{
					Size:     p.BatchSize,
					Sequence: len(batches),
					Intent:   p.BatchIntent,
				},
			})
			if len(candidate.Rownums) == 0 {
				break
			}
		}
	}
	if len(batches) > 0 {
		batches[len(batches)-1].Batch.Final = true
	}
	return batches
}

// DriverCandidate returns the candidate set chosen as the projector driver.
func (p ProjectorKernelPlan) DriverCandidate() (QuantaCandidateSet, bool) {
	for _, candidate := range p.Candidates {
		if candidate.Index == p.Driver {
			return candidate, true
		}
	}
	return QuantaCandidateSet{}, false
}

func projectorRelationshipDependencies(plan RelationshipJoinPlan) []ProjectorRelationshipVectorDependency {
	dependencies := make([]ProjectorRelationshipVectorDependency, 0, len(plan.Edges))
	for _, edge := range plan.Edges {
		if edge.ExecutionKind != RelationshipJoinExecutionVector {
			continue
		}
		id := relationshipVectorProjectionID(len(dependencies)+1, edge)
		translation := RownumDomainTranslation{
			ID:        id + ".translation",
			From:      relationshipDomainForEndpoint(edge.Left, edge.LeftRole),
			To:        relationshipDomainForEndpoint(edge.Right, edge.RightRole),
			Edge:      edge,
			Direction: RownumDomainTranslationChildToParent,
			Intent:    edge.Intent,
		}
		if edge.Intent == RelationshipJoinOperationExpand {
			translation.From, translation.To = translation.To, translation.From
			translation.Direction = RownumDomainTranslationParentToChild
		}
		dependencies = append(dependencies, ProjectorRelationshipVectorDependency{
			ID:          id,
			ProbeName:   projectorProbeName(id),
			Edge:        edge,
			Intent:      edge.Intent,
			Child:       edge.Left,
			Parent:      edge.Right,
			Translation: translation,
			Coverage:    relationshipVectorCoveragePlan(edge),
		})
	}
	return dependencies
}

func projectorMaterializationDependencies(driver string, fields []QuantaProjectionField, rehydration map[string]bool) []ProjectorMaterializationDependency {
	dependencies := make([]ProjectorMaterializationDependency, 0, len(fields))
	for _, field := range fields {
		candidate := field.Index
		if candidate == "" {
			candidate = driver
		}
		id := projectorMaterializationDependencyID(candidate, field)
		dependencies = append(dependencies, ProjectorMaterializationDependency{
			ID:         id,
			ProbeName:  projectorProbeName(id),
			Field:      field,
			Candidate:  candidate,
			Rehydrates: rehydration[projectorFieldKey(field)],
		})
	}
	return dependencies
}

func (p ProjectorKernelPlan) materializationDependenciesByCandidate() map[string][]ProjectorMaterializationDependency {
	dependenciesByCandidate := make(map[string][]ProjectorMaterializationDependency)
	for _, dependency := range p.Materialization {
		dependenciesByCandidate[dependency.Candidate] = append(dependenciesByCandidate[dependency.Candidate], dependency)
	}
	return dependenciesByCandidate
}

func projectorMaterializationFields(dependencies []ProjectorMaterializationDependency) []QuantaProjectionField {
	fields := make([]QuantaProjectionField, 0, len(dependencies))
	for _, dependency := range dependencies {
		fields = append(fields, dependency.Field)
	}
	return fields
}

func projectorFieldKey(field QuantaProjectionField) string {
	if field.Index == "" {
		return field.Field
	}
	return field.Index + "." + field.Field
}

func projectorMaterializationDependencyID(candidate string, field QuantaProjectionField) string {
	fieldName := field.Field
	if fieldName == "" {
		fieldName = field.PhysicalName
	}
	if candidate == "" {
		candidate = "driver"
	}
	if fieldName == "" {
		fieldName = "field"
	}
	return "materialize." + candidate + "." + fieldName
}

func projectorMaterializationRequestID(sequence int, candidate string) string {
	if candidate == "" {
		candidate = "driver"
	}
	return "materialization." + strconv.Itoa(sequence) + "." + candidate
}

func projectorMaterializationBatchRequestID(sequence int, candidate string) string {
	if candidate == "" {
		candidate = "driver"
	}
	return "materialization_batch." + strconv.Itoa(sequence) + "." + candidate
}

func projectorCandidateBatchID(sequence int, candidate string) string {
	if candidate == "" {
		candidate = "driver"
	}
	return "candidate_batch." + strconv.Itoa(sequence) + "." + candidate
}

func projectorExecutionStepID(kind ProjectorKernelStageKind) string {
	return "projector." + string(kind)
}

func projectorProbeName(id string) string {
	name := strings.ToLower(id)
	var builder strings.Builder
	lastUnderscore := false
	for _, current := range name {
		if (current >= 'a' && current <= 'z') || (current >= '0' && current <= '9') {
			builder.WriteRune(current)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(builder.String(), "_")
}

func cloneQuantaCandidateSets(candidates []QuantaCandidateSet) []QuantaCandidateSet {
	cloned := make([]QuantaCandidateSet, len(candidates))
	for i, candidate := range candidates {
		cloned[i] = candidate
		cloned[i].Rownums = append([]QuantaRownum(nil), candidate.Rownums...)
	}
	return cloned
}

func cloneRownumDomainSetMap(inputs map[string]RownumDomainSet) map[string]RownumDomainSet {
	cloned := make(map[string]RownumDomainSet, len(inputs))
	for name, set := range inputs {
		set.Rownums = append([]QuantaRownum(nil), set.Rownums...)
		cloned[name] = set
	}
	return cloned
}
