package qsbridge

// SessionID identifies a client session at the planning boundary.
type SessionID string

// UserName identifies the authenticated SQL user visible to planning metadata.
type UserName string

// RoleName identifies one effective SQL role visible to planning metadata.
type RoleName string

// SQLMode identifies one SQL compatibility mode enabled for a session.
type SQLMode string

// SessionContext captures protocol session state needed by planning.
//
// It is intentionally metadata-only. Authentication, authorization decisions,
// transaction state, network connection state, and runtime session storage stay
// outside qsbridge.
type SessionContext struct {
	ID                 SessionID
	User               UserName
	Roles              []RoleName
	CurrentSchema      string
	TimeZone           string
	SQLModes           []SQLMode
	Variables          map[string]string
	TemporaryTables    map[string]TableDefinition
	TemporaryTableRows map[string]TemporaryTableData
}

// Clone returns a copy whose slices and maps can be mutated independently.
func (s SessionContext) Clone() SessionContext {
	cloned := s
	cloned.Roles = append([]RoleName(nil), s.Roles...)
	cloned.SQLModes = append([]SQLMode(nil), s.SQLModes...)
	if s.Variables != nil {
		cloned.Variables = make(map[string]string, len(s.Variables))
		for key, value := range s.Variables {
			cloned.Variables[key] = value
		}
	}
	if s.TemporaryTables != nil {
		cloned.TemporaryTables = make(map[string]TableDefinition, len(s.TemporaryTables))
		for key, table := range s.TemporaryTables {
			cloned.TemporaryTables[key] = cloneTableDefinition(table)
		}
	}
	if s.TemporaryTableRows != nil {
		cloned.TemporaryTableRows = cloneTemporaryTableRowsMap(s.TemporaryTableRows)
	}
	return cloned
}

// EffectiveSchema returns the session schema or fallback when no schema is selected.
func (s SessionContext) EffectiveSchema(fallback string) string {
	if s.CurrentSchema != "" {
		return s.CurrentSchema
	}
	return fallback
}

// HasRole reports whether the role is present in the effective session roles.
func (s SessionContext) HasRole(role RoleName) bool {
	for _, current := range s.Roles {
		if current == role {
			return true
		}
	}
	return false
}

// HasSQLMode reports whether mode is enabled for the session.
func (s SessionContext) HasSQLMode(mode SQLMode) bool {
	for _, current := range s.SQLModes {
		if current == mode {
			return true
		}
	}
	return false
}
