package qsbridge

import "sort"

// ClientReplicaRole describes a replica's role for adapter-visible topology metadata.
type ClientReplicaRole string

const (
	// ClientReplicaRolePrimary identifies the primary replica for a shard.
	ClientReplicaRolePrimary ClientReplicaRole = "primary"
	// ClientReplicaRoleFollower identifies a follower replica for a shard.
	ClientReplicaRoleFollower ClientReplicaRole = "follower"
	// ClientReplicaRoleUnknown means the adapter did not classify replica role.
	ClientReplicaRoleUnknown ClientReplicaRole = ""
)

// ClientShardPlacement describes one adapter-supplied shard/replica placement row.
type ClientShardPlacement struct {
	Schema  string
	Table   string
	Shard   ShardID
	Replica ReplicaID
	Role    ClientReplicaRole
	Region  string
	Zone    string
	Address string
	Healthy bool
	Local   bool
}

// ClientTopologyExchange is adapter-facing shard and replica metadata.
type ClientTopologyExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Placements   []ClientShardPlacement
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientTopology returns adapter-supplied shard and replica placement metadata.
func (s PlanningService) ListClientTopology(connection ConnectionContext, placements []ClientShardPlacement, pattern string) ClientTopologyExchange {
	_ = s
	exchange := ClientTopologyExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Placements = filterClientShardPlacements(placements, pattern)
	}
	exchange.Result = exchange.topologyResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether topology metadata can be returned.
func (e ClientTopologyExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts topology diagnostics into protocol-facing errors.
func (e ClientTopologyExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking topology error, if any.
func (e ClientTopologyExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientTopologyExchange) topologyResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     topologyResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.topologyRows(),
		Final: true,
	})
}

func topologyResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Table", Type: DataTypeString},
		{Name: "Shard", Type: DataTypeString},
		{Name: "Replica", Type: DataTypeString},
		{Name: "Role", Type: DataTypeString, Nullable: true},
		{Name: "Region", Type: DataTypeString, Nullable: true},
		{Name: "Zone", Type: DataTypeString, Nullable: true},
		{Name: "Address", Type: DataTypeString, Nullable: true},
		{Name: "Healthy", Type: DataTypeBool},
		{Name: "Local", Type: DataTypeBool},
	}
}

func (e ClientTopologyExchange) topologyRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Placements))
	for _, placement := range e.Placements {
		rows = append(rows, ResultRow{
			metadataStringCell(placement.Schema),
			metadataStringCell(placement.Table),
			metadataStringCell(string(placement.Shard)),
			metadataStringCell(string(placement.Replica)),
			metadataStringCell(string(placement.Role)),
			metadataStringCell(placement.Region),
			metadataStringCell(placement.Zone),
			metadataStringCell(placement.Address),
			metadataBoolCell(placement.Healthy),
			metadataBoolCell(placement.Local),
		})
	}
	return rows
}

func filterClientShardPlacements(placements []ClientShardPlacement, pattern string) []ClientShardPlacement {
	cloned := cloneClientShardPlacements(placements)
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].Schema != cloned[j].Schema {
			return cloned[i].Schema < cloned[j].Schema
		}
		if cloned[i].Table != cloned[j].Table {
			return cloned[i].Table < cloned[j].Table
		}
		if cloned[i].Shard != cloned[j].Shard {
			return cloned[i].Shard < cloned[j].Shard
		}
		return cloned[i].Replica < cloned[j].Replica
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientShardPlacement, 0, len(cloned))
	for _, placement := range cloned {
		if catalogFieldPatternMatch(pattern, placement.Schema) ||
			catalogFieldPatternMatch(pattern, placement.Table) ||
			catalogFieldPatternMatch(pattern, string(placement.Shard)) ||
			catalogFieldPatternMatch(pattern, string(placement.Replica)) {
			filtered = append(filtered, placement)
		}
	}
	return filtered
}

func cloneClientShardPlacements(placements []ClientShardPlacement) []ClientShardPlacement {
	if len(placements) == 0 {
		return nil
	}
	return append([]ClientShardPlacement(nil), placements...)
}
