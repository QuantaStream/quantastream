package qsbridge

// AuthenticationMethod names the protocol or enterprise auth mechanism in use.
type AuthenticationMethod string

const (
	// AuthenticationMethodUnknown means the adapter did not identify a mechanism.
	AuthenticationMethodUnknown AuthenticationMethod = ""
	// AuthenticationMethodMySQLPassword identifies a MySQL-compatible password flow.
	AuthenticationMethodMySQLPassword AuthenticationMethod = "mysql_password"
	// AuthenticationMethodJWT identifies a bearer-token/JWT style flow.
	AuthenticationMethodJWT AuthenticationMethod = "jwt"
	// AuthenticationMethodOAuth identifies an OAuth/OIDC style flow.
	AuthenticationMethodOAuth AuthenticationMethod = "oauth"
)

// AuthenticationPlugin names the wire/plugin identity exposed during login.
type AuthenticationPlugin string

const (
	// AuthenticationPluginCachingSHA2Password is MySQL 8's default password plugin.
	AuthenticationPluginCachingSHA2Password AuthenticationPlugin = "caching_sha2_password"
	// AuthenticationPluginMySQLNativePassword is the legacy MySQL password plugin.
	AuthenticationPluginMySQLNativePassword AuthenticationPlugin = "mysql_native_password"
	// AuthenticationPluginMySQLClearPassword is the cleartext plugin used behind secure transports.
	AuthenticationPluginMySQLClearPassword AuthenticationPlugin = "mysql_clear_password"
	// AuthenticationPluginBearerJWT is the placeholder plugin name for bearer/JWT adapters.
	AuthenticationPluginBearerJWT AuthenticationPlugin = "bearer_jwt"
	// AuthenticationPluginOpenIDConnect is the placeholder plugin name for OAuth/OIDC adapters.
	AuthenticationPluginOpenIDConnect AuthenticationPlugin = "openid_connect"
)

// DefaultAuthenticationPlugin returns the preferred plugin for a high-level method.
func DefaultAuthenticationPlugin(method AuthenticationMethod) AuthenticationPlugin {
	switch method {
	case AuthenticationMethodMySQLPassword:
		return AuthenticationPluginCachingSHA2Password
	case AuthenticationMethodJWT:
		return AuthenticationPluginBearerJWT
	case AuthenticationMethodOAuth:
		return AuthenticationPluginOpenIDConnect
	default:
		return ""
	}
}

// AuthenticationRequest carries protocol-neutral login metadata.
//
// It intentionally does not store raw password bytes or implement a handshake.
// Protocol adapters own credential exchange and pass only metadata needed by
// authentication providers.
type AuthenticationRequest struct {
	SessionID     SessionID
	User          UserName
	DefaultSchema string
	Method        AuthenticationMethod
	ClientAddress string
	Attributes    map[string]string
}

// AuthenticationPrincipal is the authenticated identity used to create sessions.
type AuthenticationPrincipal struct {
	User          UserName
	Roles         []RoleName
	DefaultSchema string
	Attributes    map[string]string
}

// AuthenticationDecision records an authentication provider decision.
type AuthenticationDecision struct {
	Authenticated bool
	Principal     AuthenticationPrincipal
	Diagnostics   DiagnosticSet
}

// Authenticator is the adapter boundary for login decisions.
type Authenticator interface {
	Authenticate(request AuthenticationRequest) AuthenticationDecision
}

// Authenticate delegates the request to authenticator.
//
// A nil authenticator returns an unauthenticated decision. Adapters should pass
// an explicit implementation rather than relying on qsbridge defaults.
func (r AuthenticationRequest) Authenticate(authenticator Authenticator) AuthenticationDecision {
	if authenticator == nil {
		return r.Deny("authentication provider is not configured")
	}
	return authenticator.Authenticate(r.Clone())
}

// Clone returns a request whose attributes can be mutated independently.
func (r AuthenticationRequest) Clone() AuthenticationRequest {
	r.Attributes = cloneStringMap(r.Attributes)
	return r
}

// Allow creates an authenticated decision for principal.
func (r AuthenticationRequest) Allow(principal AuthenticationPrincipal) AuthenticationDecision {
	if principal.User == "" {
		principal.User = r.User
	}
	if principal.DefaultSchema == "" {
		principal.DefaultSchema = r.DefaultSchema
	}
	return AuthenticationDecision{
		Authenticated: true,
		Principal:     cloneAuthenticationPrincipal(principal),
	}
}

// Deny creates an unauthenticated decision with a diagnostic.
func (r AuthenticationRequest) Deny(reason string) AuthenticationDecision {
	if reason == "" {
		reason = "authentication failed"
	}
	return AuthenticationDecision{
		Authenticated: false,
		Diagnostics: DiagnosticSet{ErrorDiagnostic(
			DiagnosticAccessDenied,
			PhaseBind,
			reason,
		)},
	}
}

// Supported reports whether the authentication decision allows session creation.
func (d AuthenticationDecision) Supported() bool {
	return d.Authenticated && !d.Diagnostics.BlocksNative()
}

// SessionContext creates planning session metadata from this decision.
func (d AuthenticationDecision) SessionContext(sessionID SessionID) SessionContext {
	if !d.Supported() {
		return SessionContext{ID: sessionID}
	}
	return SessionContext{
		ID:            sessionID,
		User:          d.Principal.User,
		Roles:         append([]RoleName(nil), d.Principal.Roles...),
		CurrentSchema: d.Principal.DefaultSchema,
		Variables:     cloneStringMap(d.Principal.Attributes),
	}
}

func cloneAuthenticationPrincipal(principal AuthenticationPrincipal) AuthenticationPrincipal {
	principal.Roles = append([]RoleName(nil), principal.Roles...)
	principal.Attributes = cloneStringMap(principal.Attributes)
	return principal
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
