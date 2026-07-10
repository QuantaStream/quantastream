package qsbridge

// ClientRendezvousPlacementRow is one adapter-visible rendezvous placement owner row.
type ClientRendezvousPlacementRow struct {
	ShardKey           DataShardKey
	TopologyGeneration TopologyGeneration
	ReplicationFactor  int
	OwnerRank          int
	Node               NodeID
	Complete           bool
	CacheKey           string
}

// ClientRendezvousPlacementExchange is adapter-facing rendezvous placement metadata.
type ClientRendezvousPlacementExchange struct {
	Connection   ConnectionContext
	Input        RendezvousPlacementInput
	Topology     ClusterTopologyProfile
	Placement    RendezvousPlacementResult
	Rows         []ClientRendezvousPlacementRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ExplainClientRendezvousPlacement returns adapter-visible owner rows for one shard key.
func (s PlanningService) ExplainClientRendezvousPlacement(connection ConnectionContext, input RendezvousPlacementInput) ClientRendezvousPlacementExchange {
	return s.ExplainClientRendezvousPlacementWithTopology(connection, input, DefaultClusterTopologyProfile())
}

// ExplainClientRendezvousPlacementWithTopology returns owner rows using explicit topology metadata.
func (s PlanningService) ExplainClientRendezvousPlacementWithTopology(connection ConnectionContext, input RendezvousPlacementInput, topology ClusterTopologyProfile) ClientRendezvousPlacementExchange {
	_ = s
	exchange := ClientRendezvousPlacementExchange{
		Connection:  cloneConnectionContext(connection),
		Input:       cloneRendezvousPlacementInput(input),
		Topology:    cloneClusterTopologyProfile(topology),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Placement = ResolveRendezvousPlacementWithTopology(input, topology)
		exchange.Rows = clientRendezvousPlacementRows(exchange.Placement)
	}
	exchange.Result = exchange.rendezvousPlacementResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether rendezvous placement metadata can be returned.
func (e ClientRendezvousPlacementExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts rendezvous placement diagnostics into protocol-facing errors.
func (e ClientRendezvousPlacementExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking rendezvous placement error, if any.
func (e ClientRendezvousPlacementExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientRendezvousPlacementExchange) rendezvousPlacementResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     rendezvousPlacementResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.rendezvousPlacementResultRows(),
		Final: true,
	})
}

func rendezvousPlacementResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Shard_key", Type: DataTypeString},
		{Name: "Topology_generation", Type: DataTypeString},
		{Name: "Replication_factor", Type: DataTypeInt},
		{Name: "Owner_rank", Type: DataTypeInt},
		{Name: "Node_id", Type: DataTypeString},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "Cache_key", Type: DataTypeString},
	}
}

func (e ClientRendezvousPlacementExchange) rendezvousPlacementResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.ShardKey)),
			metadataStringCell(string(row.TopologyGeneration)),
			metadataIntCell(row.ReplicationFactor),
			metadataIntCell(row.OwnerRank),
			metadataStringCell(string(row.Node)),
			metadataBoolCell(row.Complete),
			metadataStringCell(row.CacheKey),
		})
	}
	return rows
}

func clientRendezvousPlacementRows(placement RendezvousPlacementResult) []ClientRendezvousPlacementRow {
	rows := make([]ClientRendezvousPlacementRow, 0, len(placement.Owners))
	cacheKey := placement.PlacementCacheKey()
	for i, owner := range placement.Owners {
		rows = append(rows, ClientRendezvousPlacementRow{
			ShardKey:           placement.ShardKey,
			TopologyGeneration: placement.TopologyGeneration,
			ReplicationFactor:  placement.ReplicationFactor,
			OwnerRank:          i + 1,
			Node:               owner,
			Complete:           placement.Complete,
			CacheKey:           cacheKey,
		})
	}
	return rows
}

func cloneRendezvousPlacementInput(input RendezvousPlacementInput) RendezvousPlacementInput {
	input.Nodes = append([]NodeID(nil), input.Nodes...)
	return input
}
