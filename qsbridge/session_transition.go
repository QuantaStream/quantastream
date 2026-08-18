package qsbridge

import "strings"

// SessionTransition previews session state after applying requested actions.
//
// It is metadata-only. qsbridge does not mutate live protocol sessions or own
// transaction state; adapters can inspect this transition and decide whether to
// apply it to their session store.
type SessionTransition struct {
	Before      SessionContext
	After       SessionContext
	Actions     []SessionAction
	Diagnostics DiagnosticSet
}

// PreviewSessionTransition returns the session state implied by actions.
func (s SessionContext) PreviewSessionTransition(actions []SessionAction) SessionTransition {
	transition := SessionTransition{
		Before:  s.Clone(),
		After:   s.Clone(),
		Actions: cloneSessionActions(actions),
	}
	for _, action := range actions {
		transition.After = transition.After.withSessionAction(action)
	}
	return transition
}

// SessionTransition previews this execution request's requested session changes.
func (r ExecutionRequest) SessionTransition() SessionTransition {
	return r.Bound.Prepared.Session.PreviewSessionTransition(r.SessionActions)
}

// SessionTransition previews this batch execution request's requested session changes.
func (r BatchExecutionRequest) SessionTransition() SessionTransition {
	return r.Prepared.Session.PreviewSessionTransition(r.SessionActions)
}

// SessionTransition previews this result's requested session changes.
func (r ExecutionResult) SessionTransition(before SessionContext) SessionTransition {
	return before.PreviewSessionTransition(r.SessionActions)
}

// Supported reports whether the transition has no blocking diagnostics.
func (t SessionTransition) Supported() bool {
	return !t.Diagnostics.BlocksNative()
}

func (s SessionContext) withSessionAction(action SessionAction) SessionContext {
	switch action.Kind {
	case SessionActionUseSchema:
		s.CurrentSchema = action.Value
	case SessionActionSetVariable:
		if s.Variables == nil {
			s.Variables = make(map[string]string, 1)
		}
		s.Variables[action.Name] = action.Value
	case SessionActionSetSQLMode:
		s.SQLModes = parseSQLModes(action.Value)
	case SessionActionSetTimeZone:
		s.TimeZone = action.Value
	case SessionActionResetConnection:
		s = resetClientSession(s)
	case SessionActionCreateTemporaryTable:
		table := cloneTableDefinition(action.Table)
		if table.Name == "" {
			table.Name = strings.TrimSpace(action.Name)
		}
		if table.Schema == "" {
			table.Schema = strings.TrimSpace(action.Value)
		}
		if table.Schema == "" {
			table.Schema = s.EffectiveSchema("")
		}
		if table.Name != "" {
			if s.TemporaryTables == nil {
				s.TemporaryTables = make(map[string]TableDefinition, 1)
			}
			s.TemporaryTables[temporaryTableKey(table.Schema, table.Name)] = table
		}
	case SessionActionDropTemporaryTable:
		tableName := strings.TrimSpace(action.Name)
		schemaName := strings.TrimSpace(action.Value)
		if schemaName == "" {
			schemaName = s.EffectiveSchema("")
		}
		if tableName != "" && s.TemporaryTables != nil {
			delete(s.TemporaryTables, temporaryTableKey(schemaName, tableName))
		}
	}
	return s
}

func parseSQLModes(value string) []SQLMode {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	modes := make([]SQLMode, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			modes = append(modes, SQLMode(part))
		}
	}
	return modes
}
