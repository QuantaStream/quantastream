package qsbridge

import "testing"

func TestPlanningServiceInspectClientPlanCacheKeyReturnsDeterministicParts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	key := PlanCacheKey{
		Digest:         "abc123",
		SQL:            "select * from orders",
		Schema:         "quanta",
		CatalogVersion: "catalog-v1",
		User:           "moli",
		Roles:          []RoleName{"writer", "reader"},
		SQLModes:       []SQLMode{"ansi", "strict"},
		TimeZone:       "UTC",
		Variables:      map[string]string{"b": "2", "a": "1"},
		Scope: PhysicalScope{
			Shards:    ShardSet{All: false, Shards: []ShardID{"shard-b", "shard-a"}},
			Replicas:  []ReplicaID{"replica-b", "replica-a"},
			Routing:   RoutingKey("orders"),
			Placement: PlacementLocal,
			Cache:     CacheSession,
		},
	}

	exchange := service.InspectClientPlanCacheKey(connection, key)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cache-key metadata", exchange)
	}
	if len(exchange.Parts) != 20 {
		t.Fatalf("parts = %#v, want deterministic key breakdown", exchange.Parts)
	}
	if exchange.Parts[0] != (ClientPlanCacheKeyPart{Part: "digest", Value: "abc123"}) {
		t.Fatalf("first part = %#v, want digest", exchange.Parts[0])
	}
	if exchange.Parts[5].Value != "reader" || exchange.Parts[6].Value != "writer" {
		t.Fatalf("role parts = %#v, want sorted roles", exchange.Parts[5:7])
	}
	if exchange.Parts[10] != (ClientPlanCacheKeyPart{Part: "variable", Name: "a", Value: "1"}) {
		t.Fatalf("variable part = %#v, want sorted variable a", exchange.Parts[10])
	}
	if exchange.Parts[12].Value != "false" || exchange.Parts[13].Value != "shard-a" || exchange.Parts[15].Value != "replica-a" {
		t.Fatalf("scope parts = %#v, want sorted scope details", exchange.Parts[12:16])
	}
	if len(exchange.ResultSchema.Columns) != 3 || exchange.Result.RowsReturned != uint64(len(exchange.Parts)) {
		t.Fatalf("result/schema = %#v/%#v, want cache-key rows", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[10]
	if resultRow[0].Value != "variable" || resultRow[1].Value != "a" || resultRow[2].Value != "1" {
		t.Fatalf("result row = %#v, want variable key part", resultRow)
	}
}

func TestPlanningServiceInspectClientPlanCacheKeyCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	key := PlanCacheKey{
		Digest:    "abc123",
		Roles:     []RoleName{"reader"},
		Variables: map[string]string{"sql_select_limit": "10"},
		Scope:     PhysicalScope{Shards: ShardSet{Shards: []ShardID{"shard-a"}}},
	}

	exchange := service.InspectClientPlanCacheKey(connection, key)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Key.Roles[0] = "mutated"
	exchange.Key.Variables["sql_select_limit"] = "mutated"
	exchange.Key.Scope.Shards.Shards[0] = "mutated"
	exchange.Parts[0].Value = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.InspectClientPlanCacheKey(connection, key)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Key.Roles[0] != "reader" || again.Key.Variables["sql_select_limit"] != "10" || again.Key.Scope.Shards.Shards[0] != "shard-a" {
		t.Fatalf("cache key leaked mutation: %#v", again.Key)
	}
	if again.Parts[0].Value != "abc123" {
		t.Fatalf("parts leaked mutation: %#v", again.Parts[0])
	}
	if again.Result.Columns[0].Name != "Part" || again.ResultSchema.Columns[0].Name != "Part" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "abc123" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
