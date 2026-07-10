package qsbridge

import "testing"

func TestPlanningServiceExplainClientRendezvousPlacementReturnsOwnerRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	input := RendezvousPlacementInput{
		Nodes:              []NodeID{"node-c", "node-a", "node-b"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}

	exchange := service.ExplainClientRendezvousPlacementWithTopology(connection, input, ClusterTopologyProfile{
		Name:              "test",
		ReplicationFactor: 2,
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported rendezvous placement metadata", exchange)
	}
	if exchange.Placement.ShardKey != input.ShardKey || len(exchange.Rows) != 2 {
		t.Fatalf("placement/rows = %#v/%#v, want two owner rows", exchange.Placement, exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Shard_key" {
		t.Fatalf("schema = %#v, want rendezvous placement columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[0][3].Value != 1 {
		t.Fatalf("result = %#v, want ranked owner rows", exchange.Result)
	}
	if exchange.Rows[0].CacheKey == "" || exchange.Rows[0].TopologyGeneration != "topology-7" {
		t.Fatalf("row = %#v, want topology-aware cache key", exchange.Rows[0])
	}
}

func TestPlanningServiceExplainClientRendezvousPlacementReturnsIncompleteRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	input := RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}

	exchange := service.ExplainClientRendezvousPlacementWithTopology(connection, input, ClusterTopologyProfile{
		Name:              "test",
		ReplicationFactor: 3,
	})
	if exchange.Placement.Complete || len(exchange.Rows) != 1 || exchange.Rows[0].Complete {
		t.Fatalf("exchange = %#v, want one incomplete owner row", exchange)
	}
}

func TestPlanningServiceExplainClientRendezvousPlacementReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ExplainClientRendezvousPlacement(connection, RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceExplainClientRendezvousPlacementCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	input := RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a", "node-b"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}

	exchange := service.ExplainClientRendezvousPlacementWithTopology(connection, input, ClusterTopologyProfile{
		Name:              "test",
		ReplicationFactor: 1,
	})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Input.Nodes[0] = "mutated"
	exchange.Rows[0].Node = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ExplainClientRendezvousPlacementWithTopology(connection, input, ClusterTopologyProfile{
		Name:              "test",
		ReplicationFactor: 1,
	})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Input.Nodes[0] != "node-a" || again.Rows[0].Node == "mutated" {
		t.Fatalf("input/rows leaked mutation: %#v/%#v", again.Input, again.Rows)
	}
	if again.Result.Columns[0].Name != "Shard_key" || again.ResultSchema.Columns[0].Name != "Shard_key" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
