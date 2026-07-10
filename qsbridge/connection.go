package qsbridge

// ClientCapability identifies behavior advertised by a connecting client.
type ClientCapability string

const (
	// ClientCapabilityMultiStatements means the client may send multiple statements per request.
	ClientCapabilityMultiStatements ClientCapability = "multi_statements"
	// ClientCapabilityPreparedStatements means the client can use prepared statement handles.
	ClientCapabilityPreparedStatements ClientCapability = "prepared_statements"
	// ClientCapabilityBatching means the client can submit batched parameter sets.
	ClientCapabilityBatching ClientCapability = "batching"
	// ClientCapabilityCompression means the client requested compressed transport.
	ClientCapabilityCompression ClientCapability = "compression"
	// ClientCapabilityTLS means the client requested encrypted transport.
	ClientCapabilityTLS ClientCapability = "tls"
	// ClientCapabilitySessionTracking means the client can consume session-state metadata.
	ClientCapabilitySessionTracking ClientCapability = "session_tracking"
)

// ClientCapabilities is an ordered set-like list of client capabilities.
type ClientCapabilities []ClientCapability

// Has reports whether capability is present.
func (c ClientCapabilities) Has(capability ClientCapability) bool {
	for _, current := range c {
		if current == capability {
			return true
		}
	}
	return false
}

// ConnectionCapabilityPolicy describes client capabilities an adapter requires or accepts.
type ConnectionCapabilityPolicy struct {
	Required ClientCapabilities
	Optional ClientCapabilities
}

// ConnectionCapabilityNegotiation records the accepted client capability set.
type ConnectionCapabilityNegotiation struct {
	Client      ClientCapabilities
	Required    ClientCapabilities
	Optional    ClientCapabilities
	Accepted    ClientCapabilities
	Diagnostics DiagnosticSet
}

// ConnectionRequest captures protocol-neutral metadata from a new connection.
//
// It is not a wire handshake. Protocol adapters own socket state, TLS
// negotiation, password exchange, and connection lifetime.
type ConnectionRequest struct {
	SessionID      SessionID
	Protocol       ProtocolProfile
	Capabilities   ClientCapabilities
	Authentication AuthenticationRequest
	Attributes     map[string]string
}

// ConnectionContext is metadata available after a connection has authenticated.
type ConnectionContext struct {
	Session      SessionContext
	Protocol     ProtocolProfile
	Capabilities ClientCapabilities
	Attributes   map[string]string
	Diagnostics  DiagnosticSet
}

// ConnectionPlanOptions supplies adapter-owned planning metadata for a connection.
type ConnectionPlanOptions struct {
	DefaultSchema  string
	CatalogVersion CatalogVersion
	Scope          PhysicalScope
	Optimization   OptimizationTrace
}

// NewConnectionRequest creates a request with copied protocol, capabilities, and attributes.
func NewConnectionRequest(sessionID SessionID, protocol ProtocolProfile, auth AuthenticationRequest, capabilities ...ClientCapability) ConnectionRequest {
	auth.SessionID = sessionID
	return ConnectionRequest{
		SessionID:      sessionID,
		Protocol:       protocol.Clone(),
		Capabilities:   append(ClientCapabilities(nil), capabilities...),
		Authentication: auth.Clone(),
		Attributes:     cloneStringMap(auth.Attributes),
	}
}

// Clone returns a request whose mutable metadata can be changed independently.
func (r ConnectionRequest) Clone() ConnectionRequest {
	r.Protocol = r.Protocol.Clone()
	r.Capabilities = append(ClientCapabilities(nil), r.Capabilities...)
	r.Authentication = r.Authentication.Clone()
	r.Attributes = cloneStringMap(r.Attributes)
	return r
}

// Supports reports whether the connecting client advertised capability.
func (r ConnectionRequest) Supports(capability ClientCapability) bool {
	return r.Capabilities.Has(capability)
}

// NegotiateCapabilities validates client capabilities against adapter policy.
func (r ConnectionRequest) NegotiateCapabilities(policy ConnectionCapabilityPolicy) ConnectionCapabilityNegotiation {
	negotiation := ConnectionCapabilityNegotiation{
		Client:   append(ClientCapabilities(nil), r.Capabilities...),
		Required: append(ClientCapabilities(nil), policy.Required...),
		Optional: append(ClientCapabilities(nil), policy.Optional...),
	}
	if len(policy.Required) == 0 && len(policy.Optional) == 0 {
		negotiation.Accepted = append(ClientCapabilities(nil), r.Capabilities...)
		return negotiation
	}
	for _, capability := range policy.Required {
		if r.Supports(capability) {
			negotiation.Accepted = appendUniqueClientCapability(negotiation.Accepted, capability)
			continue
		}
		negotiation.Diagnostics = append(negotiation.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseBind,
			"client did not advertise required capability: "+string(capability),
		))
	}
	for _, capability := range policy.Optional {
		if r.Supports(capability) {
			negotiation.Accepted = appendUniqueClientCapability(negotiation.Accepted, capability)
		}
	}
	return negotiation
}

// Authenticate delegates login and returns connection/session metadata.
func (r ConnectionRequest) Authenticate(authenticator Authenticator) ConnectionContext {
	decision := r.Authentication.Authenticate(authenticator)
	context := ConnectionContext{
		Session:      decision.SessionContext(r.SessionID),
		Protocol:     r.Protocol.Clone(),
		Capabilities: append(ClientCapabilities(nil), r.Capabilities...),
		Attributes:   cloneStringMap(r.Attributes),
		Diagnostics:  cloneDiagnosticSet(decision.Diagnostics),
	}
	if !decision.Supported() {
		return context
	}
	context.Session.ID = r.SessionID
	return context
}

// Supported reports whether the connection context is authenticated and usable.
func (c ConnectionContext) Supported() bool {
	return c.Session.User != "" && !c.Diagnostics.BlocksNative()
}

// Supports reports whether the connected client advertised capability.
func (c ConnectionContext) Supports(capability ClientCapability) bool {
	return c.Capabilities.Has(capability)
}

// Supported reports whether capability negotiation accepted the required set.
func (n ConnectionCapabilityNegotiation) Supported() bool {
	return !n.Diagnostics.BlocksNative()
}

// PlanSession returns a copy of the authenticated planning session metadata.
func (c ConnectionContext) PlanSession() SessionContext {
	return c.Session.Clone()
}

// PlanRequest creates a planning request using this authenticated connection.
func (c ConnectionContext) PlanRequest(sql string, options ConnectionPlanOptions) PlanRequest {
	session := c.PlanSession()
	return PlanRequest{
		SQL:            sql,
		DefaultSchema:  session.EffectiveSchema(options.DefaultSchema),
		CatalogVersion: options.CatalogVersion,
		Session:        session,
		Scope:          clonePhysicalScope(options.Scope),
		Optimization:   options.Optimization.Clone(),
	}
}

func appendUniqueClientCapability(capabilities ClientCapabilities, capability ClientCapability) ClientCapabilities {
	if capabilities.Has(capability) {
		return capabilities
	}
	return append(capabilities, capability)
}

func cloneConnectionCapabilityNegotiation(negotiation ConnectionCapabilityNegotiation) ConnectionCapabilityNegotiation {
	return ConnectionCapabilityNegotiation{
		Client:      append(ClientCapabilities(nil), negotiation.Client...),
		Required:    append(ClientCapabilities(nil), negotiation.Required...),
		Optional:    append(ClientCapabilities(nil), negotiation.Optional...),
		Accepted:    append(ClientCapabilities(nil), negotiation.Accepted...),
		Diagnostics: cloneDiagnosticSet(negotiation.Diagnostics),
	}
}
