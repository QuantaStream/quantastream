package qsbridge

import "testing"

func TestStatementResultProtocolStatementResponseCarriesOKMetadata(t *testing.T) {
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)
	statement := StatementResult{
		AffectedRows: 3,
		LastInsertID: 42,
		Warnings:     1,
		Status:       "Rows matched: 3",
		SessionActions: []SessionAction{{
			Kind:  SessionActionUseSchema,
			Value: "analytics",
		}},
	}

	response := statement.ProtocolStatementResponse(profile)
	if !response.Supported() {
		t.Fatalf("diagnostics = %#v, want supported response", response.Diagnostics)
	}
	if response.AffectedRows != 3 || response.LastInsertID != 42 || response.Warnings != 1 || response.Status != "Rows matched: 3" {
		t.Fatalf("response = %#v, want OK metadata", response)
	}
	for _, flag := range []ProtocolStatusFlag{
		ProtocolStatusRowsAffected,
		ProtocolStatusLastInsertID,
		ProtocolStatusWarnings,
		ProtocolStatusSessionStateChanged,
	} {
		if !protocolStatusFlagsContain(response.Flags, flag) {
			t.Fatalf("flags = %#v, want %q", response.Flags, flag)
		}
	}
}

func TestStatementResultProtocolStatementResponseCarriesNoticeMetadata(t *testing.T) {
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	statement := StatementResult{
		Status: "Query OK",
		Notices: []StatementNotice{{
			Level:    StatementNoticeWarning,
			Code:     "1265",
			SQLState: "01000",
			Message:  "Data truncated",
		}},
	}

	response := statement.ProtocolStatementResponse(profile)
	if !response.Supported() {
		t.Fatalf("diagnostics = %#v, want supported response", response.Diagnostics)
	}
	if response.Warnings != 1 || !protocolStatusFlagsContain(response.Flags, ProtocolStatusWarnings) {
		t.Fatalf("warnings/flags = %d/%#v, want warning count and flag", response.Warnings, response.Flags)
	}
	if len(response.Notices) != 1 || response.Notices[0].Code != "1265" || response.Notices[0].SQLState != "01000" {
		t.Fatalf("notices = %#v, want copied warning detail", response.Notices)
	}
}

func TestStatementResultProtocolStatementResponseRequiresCapabilities(t *testing.T) {
	statement := StatementResult{
		Status: "Database changed",
		SessionActions: []SessionAction{{
			Kind:  SessionActionUseSchema,
			Value: "analytics",
		}},
	}

	response := statement.ProtocolStatementResponse(NewProtocolProfile(ProtocolHTTP, "http"))
	if response.Supported() {
		t.Fatalf("expected missing protocol capabilities to reject response")
	}
	if got := len(response.Diagnostics.Codes()); got != 2 {
		t.Fatalf("diagnostics = %#v, want statement and session capability diagnostics", response.Diagnostics)
	}
	if len(response.ProtocolErrors()) != 2 {
		t.Fatalf("protocol errors = %#v, want two errors", response.ProtocolErrors())
	}
}

func TestExecutionResultProtocolStatementResponseMergesResultDiagnostics(t *testing.T) {
	result := ExecutionResult{
		Statement: StatementResult{AffectedRows: 1},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad option"),
		},
	}
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	response := result.ProtocolStatementResponse(profile)
	if response.Supported() {
		t.Fatalf("expected result diagnostic to reject response")
	}
	if !containsDiagnosticCode(response.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", response.Diagnostics)
	}
}

func TestExecutionResultProtocolStatementResponseUsesResultSessionActions(t *testing.T) {
	result := ExecutionResult{
		Statement: StatementResult{Status: "Transaction started"},
		SessionActions: []SessionAction{{
			Kind: SessionActionBeginTransaction,
		}},
	}
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)

	response := result.ProtocolStatementResponse(profile)
	if !response.Supported() {
		t.Fatalf("diagnostics = %#v, want supported transaction response", response.Diagnostics)
	}
	if !protocolStatusFlagsContain(response.Flags, ProtocolStatusTransactionAction) {
		t.Fatalf("flags = %#v, want transaction action", response.Flags)
	}
	if len(response.SessionActions) != 1 || response.SessionActions[0].Kind != SessionActionBeginTransaction {
		t.Fatalf("session actions = %#v, want begin transaction", response.SessionActions)
	}
}

func TestProtocolStatementResponseCopiesMutableMetadata(t *testing.T) {
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)
	statement := StatementResult{
		Notices:        []StatementNotice{{Message: "original"}},
		SessionActions: []SessionAction{{Kind: SessionActionSetVariable, Name: "autocommit", Value: "1"}},
	}

	response := statement.ProtocolStatementResponse(profile)
	response.SessionActions[0].Value = "0"
	response.Notices[0].Message = "mutated"
	response.Profile.Capabilities[0] = ProtocolCapabilityBatchExecution
	second := statement.ProtocolStatementResponse(profile)
	if statement.SessionActions[0].Value != "1" {
		t.Fatalf("statement leaked session action mutation")
	}
	if second.Profile.Capabilities[0] != ProtocolCapabilityStatementResults {
		t.Fatalf("profile leaked mutation: %#v", second.Profile.Capabilities)
	}
	if len(second.Notices) != 1 || second.Notices[0].Message != "original" {
		t.Fatalf("notices leaked mutation: %#v", second.Notices)
	}
}

func protocolStatusFlagsContain(flags []ProtocolStatusFlag, want ProtocolStatusFlag) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
