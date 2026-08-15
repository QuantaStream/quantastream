package qsruntime

import (
	"context"
	"math/big"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// LegacyDirectRelationshipVectorAggregateRequest asks a physical tier to
// produce mergeable aggregate state keyed by relationship-vector parent value.
type LegacyDirectRelationshipVectorAggregateRequest struct {
	VectorIndex     string
	VectorField     string
	ValueIndex      string
	ValueField      string
	ChildRows       []qsbridge.QuantaRownum
	ParentRows      []qsbridge.QuantaRownum
	SourceValues    []int64
	FromEpochMillis int64
	ToEpochMillis   int64
}

// LegacyDirectRelationshipVectorAggregateGroup is one grouped aggregate state.
type LegacyDirectRelationshipVectorAggregateGroup struct {
	ParentRow              qsbridge.QuantaRownum
	RepresentativeChildRow qsbridge.QuantaRownum
	Count                  uint64
	Sum                    *big.Int
}

// LegacyDirectRelationshipVectorAggregateResult is the physical-tier response.
type LegacyDirectRelationshipVectorAggregateResult struct {
	Groups                     []LegacyDirectRelationshipVectorAggregateGroup
	Mode                       string
	Rows                       uint64
	Values                     uint64
	SourceValues               int
	TargetRows                 uint64
	LookupElapsed              time.Duration
	ProjectionElapsed          time.Duration
	AggregateElapsed           time.Duration
	Nodes                      uint64
	ProjectionShardsVisited    int
	ProjectionShardsInWindow   int
	ProjectionShardsLocal      int
	ProjectionShardsRetained   int
	ProjectionRetainedRows     uint64
	ProjectionRetainBypassRows uint64
	ProjectionRetainElapsed    time.Duration
	ProjectionValueElapsed     time.Duration
	ProjectionMergeElapsed     time.Duration
	ClientRPCElapsed           time.Duration
	MaxClientRPCElapsed        time.Duration
}

// LegacyDirectRelationshipVectorAggregateReader exposes storage-side
// relationship aggregation when a deployment topology can perform it.
type LegacyDirectRelationshipVectorAggregateReader interface {
	ReadRelationshipVectorAggregate(
		context.Context,
		LegacyDirectRelationshipVectorAggregateRequest,
	) (LegacyDirectRelationshipVectorAggregateResult, qsbridge.DiagnosticSet, bool, error)
}
