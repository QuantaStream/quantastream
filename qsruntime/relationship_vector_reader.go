package qsruntime

import (
	"context"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RelationshipVectorReader aliases the qsbridge relationship-vector reader during the package split.
type RelationshipVectorReader = qsbridge.RelationshipVectorReader

// RelationshipVectorResultReader aliases the qsbridge timed relationship-vector reader during the package split.
type RelationshipVectorResultReader = qsbridge.RelationshipVectorResultReader

// InMemoryRelationshipVectorIndex aliases the qsbridge deterministic test/vector reader.
type InMemoryRelationshipVectorIndex = qsbridge.InMemoryRelationshipVectorIndex

// LegacyDirectRelationshipVectorBackend executes a prepared relationship-vector read.
type LegacyDirectRelationshipVectorBackend interface {
	ReadRelationshipVectorCandidates(context.Context, LegacyDirectRelationshipVectorReadRequest) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error)
}

// LegacyDirectRelationshipVectorResultBackend returns relationship-vector candidates with backend timing metadata.
type LegacyDirectRelationshipVectorResultBackend interface {
	ReadRelationshipVectorCandidateResult(context.Context, LegacyDirectRelationshipVectorReadRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error)
}

// LegacyDirectRelationshipVectorReadRequest is the backend-ready form of a filter-domain vector read.
type LegacyDirectRelationshipVectorReadRequest struct {
	SourceFragment   qsbridge.QuantaQueryFragment
	SourceCandidates qsbridge.QuantaCandidateSet
	TargetFilter     qsbridge.QuantaFilterExpression
	SourceDomain     string
	TargetDomain     string
	Edge             qsbridge.RelationshipJoinPlanEdge
	Direction        qsbridge.FilterDomainRelationshipVectorDirection
	Strategy         qsbridge.PhysicalStrategy
	VectorIndex      string
	VectorField      string
	// AllowCandidateSuperset permits membership seed reads to reuse a broader
	// target candidate set because the correlated membership evaluator rechecks
	// the relationship key before accepting rows.
	AllowCandidateSuperset bool
	MaxEstimatedTargetRows int
	TargetCandidateRows    []qsbridge.QuantaRownum
	PreserveArtifactOrder  bool
	DeriveArtifactRows     bool
}

// LegacyDirectRelationshipVectorReader is the future bridge into relationship-vector reads.
type LegacyDirectRelationshipVectorReader struct {
	Backend     LegacyDirectRelationshipVectorBackend
	LastRequest qsbridge.FilterDomainRelationshipVectorRequest
	LastRead    LegacyDirectRelationshipVectorReadRequest
}

// ReadRelatedCandidates records the request and delegates to the configured relationship-vector backend.
func (r *LegacyDirectRelationshipVectorReader) ReadRelatedCandidates(ctx context.Context, request qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	result, diagnostics, err := r.ReadRelatedCandidateResult(ctx, request)
	return result.TargetCandidates, diagnostics, err
}

// ReadRelatedCandidateResult records the request and returns candidates plus backend timing metadata when available.
func (r *LegacyDirectRelationshipVectorReader) ReadRelatedCandidateResult(ctx context.Context, request qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error) {
	if r != nil {
		r.LastRequest = request
	}
	read, diagnostics := NewLegacyDirectRelationshipVectorReadRequest(request)
	if diagnostics.BlocksNative() {
		return qsbridge.FilterDomainRelationshipVectorResult{}, diagnostics, nil
	}
	if r != nil {
		r.LastRead = read
	}
	if r == nil || r.Backend == nil {
		return qsbridge.FilterDomainRelationshipVectorResult{}, legacyDirectRelationshipVectorReaderBoundary(read), nil
	}
	if backend, ok := r.Backend.(LegacyDirectRelationshipVectorResultBackend); ok {
		result, diagnostics, err := backend.ReadRelationshipVectorCandidateResult(ctx, read)
		result.Request = request
		return result, diagnostics, err
	}
	candidates, diagnostics, err := r.Backend.ReadRelationshipVectorCandidates(ctx, read)
	return qsbridge.FilterDomainRelationshipVectorResult{
		Request:          request,
		TargetCandidates: candidates,
	}, diagnostics, err
}

// relationshipVectorReaderWithRequestProjectionCache wraps older call paths
// with a fallback projection cache. QueryScratchpad-aware requests use the
// shared scratchpad cache instead.
func relationshipVectorReaderWithRequestProjectionCache(reader RelationshipVectorReader) RelationshipVectorReader {
	cache := NewLegacyDirectRelationshipVectorProjectionCache()
	switch r := reader.(type) {
	case *LegacyDirectRelationshipVectorReader:
		cloned := *r
		cloned.Backend = legacyDirectRelationshipVectorBackendWithProjectionCache(r.Backend, cache)
		return &cloned
	default:
		return reader
	}
}

// legacyDirectRelationshipVectorBackendWithProjectionCache clones known backends with the request cache attached.
func legacyDirectRelationshipVectorBackendWithProjectionCache(backend LegacyDirectRelationshipVectorBackend, cache *LegacyDirectRelationshipVectorProjectionCache) LegacyDirectRelationshipVectorBackend {
	switch b := backend.(type) {
	case LegacyDirectBitIndexRelationshipVectorBackend:
		b.ProjectionCache = cache
		return b
	case *LegacyDirectBitIndexRelationshipVectorBackend:
		cloned := *b
		cloned.ProjectionCache = cache
		return &cloned
	default:
		return backend
	}
}

// NewLegacyDirectRelationshipVectorReadRequest derives the concrete vector field a backend must read.
func NewLegacyDirectRelationshipVectorReadRequest(request qsbridge.FilterDomainRelationshipVectorRequest) (LegacyDirectRelationshipVectorReadRequest, qsbridge.DiagnosticSet) {
	read := LegacyDirectRelationshipVectorReadRequest{
		SourceFragment:   request.SourceFragment,
		SourceCandidates: request.SourceCandidates,
		TargetFilter:     request.TargetFilter,
		SourceDomain:     request.SourceDomain,
		TargetDomain:     request.TargetDomain,
		Edge:             request.Edge,
		Direction:        request.Direction,
		Strategy:         request.Strategy,
	}
	vectorField, diagnostics := legacyDirectRelationshipVectorField(request)
	if diagnostics.BlocksNative() {
		return read, diagnostics
	}
	read.VectorIndex = vectorField.Table.Table
	read.VectorField = vectorField.PhysicalName
	if read.VectorField == "" {
		read.VectorField = vectorField.Name
	}
	return read, nil
}

func legacyDirectRelationshipVectorReaderBoundary(read LegacyDirectRelationshipVectorReadRequest) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"relationship-vector reader is not wired yet: source="+read.SourceDomain+" target="+read.TargetDomain+" vector="+read.VectorIndex+"."+read.VectorField+" leaf="+read.SourceFragment.Index+"."+read.SourceFragment.Field,
		),
	}
}

func legacyDirectRelationshipVectorField(request qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.FieldRef, qsbridge.DiagnosticSet) {
	leftParentRelation := legacyDirectRelationshipVectorFieldIsParentRelation(request.Edge.Left)
	rightParentRelation := legacyDirectRelationshipVectorFieldIsParentRelation(request.Edge.Right)
	switch {
	case leftParentRelation && !rightParentRelation:
		return request.Edge.Left, nil
	case rightParentRelation && !leftParentRelation:
		return request.Edge.Right, nil
	case leftParentRelation && rightParentRelation:
		return qsbridge.FieldRef{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"relationship-vector reader found ParentRelation metadata on both edge endpoints",
			),
		}
	}
	switch request.Direction {
	case qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight:
		if field, ok := legacyDirectRelationshipVectorEndpointForDomain(request, request.SourceDomain); ok {
			return field, nil
		}
	case qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft:
		if field, ok := legacyDirectRelationshipVectorEndpointForDomain(request, request.TargetDomain); ok {
			return field, nil
		}
	}
	if field, ok := legacyDirectRelationshipVectorEndpointForDomain(request, request.TargetDomain); ok {
		return field, nil
	}
	if field, ok := legacyDirectRelationshipVectorEndpointForDomain(request, request.SourceDomain); ok {
		return field, nil
	}
	return qsbridge.FieldRef{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"relationship-vector reader cannot derive vector field for direction "+string(request.Direction),
		),
	}
}

func legacyDirectRelationshipVectorFieldIsParentRelation(field qsbridge.FieldRef) bool {
	return strings.EqualFold(field.Encoding.LegacyName, "ParentRelation")
}

func legacyDirectRelationshipVectorEndpointForDomain(request qsbridge.FilterDomainRelationshipVectorRequest, domain string) (qsbridge.FieldRef, bool) {
	if strings.EqualFold(request.Edge.Left.Table.Table, domain) {
		return request.Edge.Left, true
	}
	if strings.EqualFold(request.Edge.Right.Table.Table, domain) {
		return request.Edge.Right, true
	}
	return qsbridge.FieldRef{}, false
}
