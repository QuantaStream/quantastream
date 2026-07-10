package qsbridge

import "testing"

func TestPlanningServiceListClientAccessGrantsReturnsSessionGrants(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.User = "moli"
	connection.Session.Roles = []RoleName{"reader"}
	table := TableInstance{Schema: "quanta", Table: "orders"}
	policy := NewAccessPolicy(
		AccessGrant{
			PrincipalKind: AccessPrincipalUser,
			Principal:     "moli",
			Privilege:     AccessSelect,
			Table:         table,
			Fields:        []FieldRef{{Name: "o_orderkey"}, {Name: "o_orderdate"}},
		},
		AccessGrant{
			PrincipalKind: AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     AccessUpdate,
			Table:         table,
		},
		AccessGrant{
			PrincipalKind: AccessPrincipalUser,
			Principal:     "other",
			Privilege:     AccessDelete,
			Table:         table,
		},
	)

	exchange := service.ListClientAccessGrants(connection, policy)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported grant metadata", exchange)
	}
	if len(exchange.Grants) != 2 {
		t.Fatalf("grants = %#v, want user and role grants only", exchange.Grants)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want grant result metadata", exchange.Result, exchange.ResultSchema)
	}
	first := exchange.Result.Chunks[0].Rows[0]
	if first[0].Value != "role" || first[1].Value != "reader" || first[6].Value != "GRANT UPDATE ON quanta.orders TO ROLE 'reader'" {
		t.Fatalf("first row = %#v, want role grant row", first)
	}
	second := exchange.Result.Chunks[0].Rows[1]
	if second[0].Value != "user" || second[5].Value != "o_orderdate,o_orderkey" {
		t.Fatalf("second row = %#v, want sorted field grant row", second)
	}
}

func TestPlanningServiceListClientAccessGrantsReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.ListClientAccessGrants(connection, AccessPolicy{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want failed grant envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientAccessGrantsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Session.User = "moli"
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalUser,
		Principal:     "moli",
		Privilege:     AccessSelect,
		Table:         TableInstance{Schema: "quanta", Table: "orders"},
		Fields:        []FieldRef{{Name: "o_orderkey"}},
	})

	exchange := service.ListClientAccessGrants(connection, policy)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Grants[0].Fields[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientAccessGrants(connection, policy)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Grants[0].Fields[0].Name != "o_orderkey" || again.Result.Chunks[0].Rows[0][5].Value != "o_orderkey" {
		t.Fatalf("grant metadata leaked mutation: %#v/%#v", again.Grants, again.Result.Chunks)
	}
}
