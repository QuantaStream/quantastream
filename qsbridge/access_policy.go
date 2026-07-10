package qsbridge

// AccessPrincipalKind identifies the subject receiving an access grant.
type AccessPrincipalKind string

const (
	// AccessPrincipalUser grants directly to one authenticated user.
	AccessPrincipalUser AccessPrincipalKind = "user"
	// AccessPrincipalRole grants to sessions with one effective role.
	AccessPrincipalRole AccessPrincipalKind = "role"
)

// AccessGrant is one adapter-owned authorization policy entry.
type AccessGrant struct {
	PrincipalKind AccessPrincipalKind
	Principal     string
	Privilege     AccessPrivilege
	Table         TableInstance
	Fields        []FieldRef
}

// AccessPolicy is a small in-memory authorizer useful for adapters and tests.
//
// It is not a final RBAC engine. Persistent role storage, grant DDL, enterprise
// policy integration, and SQL privilege semantics belong outside qsbridge.
type AccessPolicy struct {
	grants []AccessGrant
}

// NewAccessPolicy creates an in-memory access policy with copied grants.
func NewAccessPolicy(grants ...AccessGrant) AccessPolicy {
	policy := AccessPolicy{}
	for _, grant := range grants {
		policy = policy.WithGrant(grant)
	}
	return policy
}

// WithGrant returns a copy of policy with grant appended.
func (p AccessPolicy) WithGrant(grant AccessGrant) AccessPolicy {
	p.grants = append(cloneAccessGrants(p.grants), cloneAccessGrant(grant))
	return p
}

// Grants returns copied policy grants.
func (p AccessPolicy) Grants() []AccessGrant {
	return cloneAccessGrants(p.grants)
}

// AuthorizeAccess implements AccessAuthorizer.
func (p AccessPolicy) AuthorizeAccess(request AuthorizationRequest) AuthorizationDecision {
	request = request.Clone()
	for _, requirement := range request.Requirements {
		if !p.allows(request.Session, requirement) {
			return request.Deny(requirement, "access denied by policy")
		}
	}
	return request.Allow()
}

func (p AccessPolicy) allows(session SessionContext, requirement AccessRequirement) bool {
	for _, grant := range p.grants {
		if !grant.matchesPrincipal(session) {
			continue
		}
		if !grant.matchesTable(requirement.Table) || grant.Privilege != requirement.Privilege {
			continue
		}
		if grant.coversFields(requirement.Fields) {
			return true
		}
	}
	return false
}

func (g AccessGrant) matchesPrincipal(session SessionContext) bool {
	switch g.PrincipalKind {
	case AccessPrincipalUser:
		return g.Principal == string(session.User)
	case AccessPrincipalRole:
		return session.HasRole(RoleName(g.Principal))
	default:
		return false
	}
}

func (g AccessGrant) matchesTable(table TableInstance) bool {
	return tableIdentityKey(g.Table) == tableIdentityKey(table)
}

func (g AccessGrant) coversFields(fields []FieldRef) bool {
	if len(fields) == 0 || len(g.Fields) == 0 {
		return true
	}
	allowed := make(map[string]struct{}, len(g.Fields))
	for _, field := range g.Fields {
		allowed[fieldIdentityKey(field)] = struct{}{}
	}
	for _, field := range fields {
		if _, ok := allowed[fieldIdentityKey(field)]; !ok {
			return false
		}
	}
	return true
}

func cloneAccessGrants(grants []AccessGrant) []AccessGrant {
	if len(grants) == 0 {
		return nil
	}
	cloned := make([]AccessGrant, 0, len(grants))
	for _, grant := range grants {
		cloned = append(cloned, cloneAccessGrant(grant))
	}
	return cloned
}

func cloneAccessGrant(grant AccessGrant) AccessGrant {
	grant.Fields = append([]FieldRef(nil), grant.Fields...)
	return grant
}

func tableIdentityKey(table TableInstance) string {
	if table.ID != "" {
		return string(table.ID)
	}
	return table.Schema + "\x00" + table.Table
}

func fieldIdentityKey(field FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}
