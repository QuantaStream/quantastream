package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientAccessGrantsReturnsSessionCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.User = "moli"
	connection.Session.Roles = []RoleName{"reader"}
	orders := TableInstance{Schema: "quanta", Table: "orders"}
	customers := TableInstance{Schema: "quanta", Table: "customers"}
	policy := NewAccessPolicy(
		AccessGrant{
			PrincipalKind: AccessPrincipalUser,
			Principal:     "moli",
			Privilege:     AccessSelect,
			Table:         orders,
			Fields:        []FieldRef{{Name: "o_orderkey"}, {Name: "o_orderdate"}},
		},
		AccessGrant{
			PrincipalKind: AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     AccessUpdate,
			Table:         orders,
		},
		AccessGrant{
			PrincipalKind: AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     AccessInsert,
			Table:         customers,
		},
		AccessGrant{
			PrincipalKind: AccessPrincipalUser,
			Principal:     "other",
			Privilege:     AccessDelete,
			Table:         orders,
		},
	)

	exchange := service.SummarizeClientAccessGrants(connection, policy)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported grant summary", exchange)
	}
	if len(exchange.Grants) != 3 {
		t.Fatalf("grants = %#v, want session-visible grants only", exchange.Grants)
	}
	row := exchange.Row
	if row.GrantCount != 3 || row.UserGrantCount != 1 || row.RoleGrantCount != 2 {
		t.Fatalf("row = %#v, want user and role grant counts", row)
	}
	if row.SelectCount != 1 || row.InsertCount != 1 || row.UpdateCount != 1 || row.DeleteCount != 0 {
		t.Fatalf("row = %#v, want privilege counts", row)
	}
	if row.TableCount != 2 || row.FieldScopedCount != 1 || row.FieldMentionCount != 2 {
		t.Fatalf("row = %#v, want table and field grant counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[2].Value != 2 || resultRow[9].Value != 2 {
		t.Fatalf("result row = %#v, want grant summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientAccessGrantsReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientAccessGrants(connection, AccessPolicy{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless grant summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAccessGrantsCopiesMutableMetadata(t *testing.T) {
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

	exchange := service.SummarizeClientAccessGrants(connection, policy)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Grants[0].Fields[0].Name = "mutated"
	exchange.Row.GrantCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientAccessGrants(connection, policy)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Grants[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("grant metadata leaked mutation: %#v", again.Grants)
	}
	if again.Row.GrantCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Grant_count" || again.ResultSchema.Columns[0].Name != "Grant_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
