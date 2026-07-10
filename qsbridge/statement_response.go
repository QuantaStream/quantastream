package qsbridge

// ProtocolStatusFlag is adapter-facing OK/status metadata.
type ProtocolStatusFlag string

const (
	// ProtocolStatusRowsAffected means the response carries affected-row metadata.
	ProtocolStatusRowsAffected ProtocolStatusFlag = "rows_affected"
	// ProtocolStatusLastInsertID means the response carries last-insert-id metadata.
	ProtocolStatusLastInsertID ProtocolStatusFlag = "last_insert_id"
	// ProtocolStatusWarnings means the response carries warning metadata.
	ProtocolStatusWarnings ProtocolStatusFlag = "warnings"
	// ProtocolStatusSessionStateChanged means the response carries requested session changes.
	ProtocolStatusSessionStateChanged ProtocolStatusFlag = "session_state_changed"
	// ProtocolStatusTransactionAction means the response carries a requested transaction action.
	ProtocolStatusTransactionAction ProtocolStatusFlag = "transaction_action"
)

// ProtocolStatementResponse describes a non-row statement response for an adapter.
//
// It models OK-packet-style metadata without building a wire packet or applying
// session actions. Adapters decide how to serialize and apply the response.
type ProtocolStatementResponse struct {
	Profile        ProtocolProfile
	AffectedRows   uint64
	LastInsertID   uint64
	Warnings       uint16
	Notices        []StatementNotice
	Status         string
	SessionActions []SessionAction
	Flags          []ProtocolStatusFlag
	Diagnostics    DiagnosticSet
}

// ProtocolStatementResponse maps statement metadata to a protocol response shape.
func (s StatementResult) ProtocolStatementResponse(profile ProtocolProfile) ProtocolStatementResponse {
	diagnostics := make(DiagnosticSet, 0)
	if !profile.Supports(ProtocolCapabilityStatementResults) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("statement responses are not supported by protocol profile"))
	}
	if len(s.SessionActions) > 0 && !profile.Supports(ProtocolCapabilitySessionActions) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("session actions are not supported by protocol profile"))
	}
	return ProtocolStatementResponse{
		Profile:        profile.Clone(),
		AffectedRows:   s.AffectedRows,
		LastInsertID:   s.LastInsertID,
		Warnings:       statementWarningCount(s),
		Notices:        cloneStatementNotices(s.Notices),
		Status:         s.Status,
		SessionActions: cloneSessionActions(s.SessionActions),
		Flags:          statementStatusFlags(s),
		Diagnostics:    diagnostics,
	}
}

// ProtocolStatementResponse maps an execution result to a protocol statement response shape.
func (r ExecutionResult) ProtocolStatementResponse(profile ProtocolProfile) ProtocolStatementResponse {
	response := r.Statement.ProtocolStatementResponse(profile)
	response.Diagnostics = mergeDiagnosticSets(response.Diagnostics, r.Diagnostics)
	if len(response.SessionActions) == 0 {
		response.SessionActions = cloneSessionActions(r.SessionActions)
		response.Flags = statementStatusFlags(StatementResult{
			AffectedRows:   response.AffectedRows,
			LastInsertID:   response.LastInsertID,
			Warnings:       response.Warnings,
			Notices:        response.Notices,
			Status:         response.Status,
			SessionActions: response.SessionActions,
		})
		if len(response.SessionActions) > 0 && !profile.Supports(ProtocolCapabilitySessionActions) {
			response.Diagnostics = mergeDiagnosticSets(response.Diagnostics, DiagnosticSet{
				protocolCapabilityDiagnostic("session actions are not supported by protocol profile"),
			})
		}
	}
	return response
}

// Supported reports whether the protocol can carry this statement response.
func (r ProtocolStatementResponse) Supported() bool {
	return !r.Diagnostics.BlocksNative()
}

// ProtocolErrors converts response diagnostics into protocol-facing errors.
func (r ProtocolStatementResponse) ProtocolErrors() []ProtocolError {
	return r.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking response error, if any.
func (r ProtocolStatementResponse) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics.FirstProtocolError()
}

func statementStatusFlags(result StatementResult) []ProtocolStatusFlag {
	flags := make([]ProtocolStatusFlag, 0, 5)
	if result.AffectedRows > 0 {
		flags = append(flags, ProtocolStatusRowsAffected)
	}
	if result.LastInsertID > 0 {
		flags = append(flags, ProtocolStatusLastInsertID)
	}
	if statementWarningCount(result) > 0 {
		flags = append(flags, ProtocolStatusWarnings)
	}
	if len(result.SessionActions) > 0 {
		flags = append(flags, ProtocolStatusSessionStateChanged)
	}
	if hasTransactionAction(result.SessionActions) {
		flags = append(flags, ProtocolStatusTransactionAction)
	}
	return flags
}

func hasTransactionAction(actions []SessionAction) bool {
	for _, action := range actions {
		switch action.Kind {
		case SessionActionBeginTransaction, SessionActionCommitTransaction, SessionActionRollbackTransaction:
			return true
		}
	}
	return false
}

func statementWarningCount(result StatementResult) uint16 {
	if result.Warnings > 0 || len(result.Notices) == 0 {
		return result.Warnings
	}
	if len(result.Notices) > int(^uint16(0)) {
		return ^uint16(0)
	}
	return uint16(len(result.Notices))
}

func cloneStatementNotices(notices []StatementNotice) []StatementNotice {
	if len(notices) == 0 {
		return nil
	}
	return append([]StatementNotice(nil), notices...)
}
