package qsbridge

// AccessAuthorizer is the adapter boundary for SQL authorization decisions.
//
// qsbridge derives the requirements, but it does not authenticate users, load
// roles, or enforce access. Protocol or deployment adapters own that policy.
type AccessAuthorizer interface {
	AuthorizeAccess(request AuthorizationRequest) AuthorizationDecision
}

// AuthorizationRequest is one metadata-only access check request.
type AuthorizationRequest struct {
	Session      SessionContext
	Requirements []AccessRequirement
}

// AuthorizationDecision records the result of an adapter authorization check.
type AuthorizationDecision struct {
	Allowed      bool
	Requirements []AccessRequirement
	Diagnostics  DiagnosticSet
}

// AuthorizationRequest returns the access check metadata for this planning result.
func (r PlanResult) AuthorizationRequest() AuthorizationRequest {
	return AuthorizationRequest{
		Session:      r.Session.Clone(),
		Requirements: cloneAccessRequirements(r.RequiredAccess()),
	}
}

// AuthorizationRequest returns the access check metadata for this prepared plan.
func (p PreparedPlan) AuthorizationRequest() AuthorizationRequest {
	return AuthorizationRequest{
		Session:      p.Session.Clone(),
		Requirements: cloneAccessRequirements(p.RequiredAccess()),
	}
}

// AuthorizationRequest returns the access check metadata for this execution request.
func (r ExecutionRequest) AuthorizationRequest() AuthorizationRequest {
	return AuthorizationRequest{
		Session:      r.Bound.Prepared.Session.Clone(),
		Requirements: cloneAccessRequirements(r.Access),
	}
}

// AuthorizationRequest returns the access check metadata for this batch execution request.
func (r BatchExecutionRequest) AuthorizationRequest() AuthorizationRequest {
	return AuthorizationRequest{
		Session:      r.Prepared.Session.Clone(),
		Requirements: cloneAccessRequirements(r.Access),
	}
}

// Authorize delegates an access decision to authorizer.
//
// A nil authorizer means qsbridge has no policy owner available and therefore
// returns an allow decision. Deployment adapters should pass their own policy.
func (r AuthorizationRequest) Authorize(authorizer AccessAuthorizer) AuthorizationDecision {
	if authorizer == nil {
		return r.Allow()
	}
	return authorizer.AuthorizeAccess(r.Clone())
}

// Clone returns a request whose session and requirements can be mutated independently.
func (r AuthorizationRequest) Clone() AuthorizationRequest {
	return AuthorizationRequest{
		Session:      r.Session.Clone(),
		Requirements: cloneAccessRequirements(r.Requirements),
	}
}

// Allow creates an allowed authorization decision for this request.
func (r AuthorizationRequest) Allow() AuthorizationDecision {
	return AuthorizationDecision{
		Allowed:      true,
		Requirements: cloneAccessRequirements(r.Requirements),
	}
}

// Deny creates a denied authorization decision for one requirement.
func (r AuthorizationRequest) Deny(requirement AccessRequirement, reason string) AuthorizationDecision {
	if reason == "" {
		reason = "access denied"
	}
	return AuthorizationDecision{
		Allowed:      false,
		Requirements: cloneAccessRequirements(r.Requirements),
		Diagnostics:  DiagnosticSet{AccessDeniedDiagnostic(requirement, reason)},
	}
}

// Supported reports whether authorization allows the request.
func (d AuthorizationDecision) Supported() bool {
	return d.Allowed && !d.Diagnostics.BlocksNative()
}

// AccessDeniedDiagnostic creates an access-denied diagnostic for a requirement.
func AccessDeniedDiagnostic(requirement AccessRequirement, reason string) Diagnostic {
	fields := append([]FieldRef(nil), requirement.Fields...)
	if len(fields) == 0 {
		fields = []FieldRef{{Table: requirement.Table}}
	}
	return Diagnostic{
		Code:     DiagnosticAccessDenied,
		Severity: SeverityError,
		Phase:    PhaseBind,
		Message:  reason,
		Fields:   fields,
	}
}
