package qsbridge

// ClientHandshakeStatusFlag records adapter-facing connection status metadata.
type ClientHandshakeStatusFlag string

const (
	// ClientHandshakeStatusAutocommit means new sessions default to autocommit.
	ClientHandshakeStatusAutocommit ClientHandshakeStatusFlag = "autocommit"
	// ClientHandshakeStatusSessionTracking means the server can report session changes.
	ClientHandshakeStatusSessionTracking ClientHandshakeStatusFlag = "session_tracking"
	// ClientHandshakeStatusTLSAvailable means encrypted transport can be negotiated.
	ClientHandshakeStatusTLSAvailable ClientHandshakeStatusFlag = "tls_available"
)

// ClientHandshakeOptions controls adapter-owned handshake greeting metadata.
type ClientHandshakeOptions struct {
	ServerVersion    string
	AuthPlugin       string
	CharacterSet     string
	Collation        string
	StatusFlags      []ClientHandshakeStatusFlag
	CapabilityPolicy ConnectionCapabilityPolicy
}

// ClientHandshakeGreeting is protocol-neutral server greeting metadata.
type ClientHandshakeGreeting struct {
	SessionID     SessionID
	Protocol      ProtocolProfile
	ServerVersion string
	AuthPlugin    string
	CharacterSet  string
	Collation     string
	StatusFlags   []ClientHandshakeStatusFlag
}

// ClientHandshakeExchange is adapter-facing metadata for a connection greeting.
type ClientHandshakeExchange struct {
	Request     ConnectionRequest
	Greeting    ClientHandshakeGreeting
	Negotiation ConnectionCapabilityNegotiation
	Diagnostics DiagnosticSet
}

// PrepareClientHandshake prepares metadata for a protocol greeting and capability policy.
func (s PlanningService) PrepareClientHandshake(request ConnectionRequest, options ClientHandshakeOptions) ClientHandshakeExchange {
	_ = s
	negotiation := request.NegotiateCapabilities(options.CapabilityPolicy)
	exchange := ClientHandshakeExchange{
		Request:     request.Clone(),
		Greeting:    clientHandshakeGreeting(request, options),
		Negotiation: cloneConnectionCapabilityNegotiation(negotiation),
		Diagnostics: cloneDiagnosticSet(negotiation.Diagnostics),
	}
	if request.Protocol.Kind == ProtocolUnknown {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "handshake requires a protocol profile"),
		})
	}
	if request.SessionID == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "handshake requires a session id"),
		})
	}
	return exchange
}

// Supported reports whether handshake metadata can be emitted.
func (e ClientHandshakeExchange) Supported() bool {
	return !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts handshake diagnostics into protocol-facing errors.
func (e ClientHandshakeExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking handshake error, if any.
func (e ClientHandshakeExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func clientHandshakeGreeting(request ConnectionRequest, options ClientHandshakeOptions) ClientHandshakeGreeting {
	return ClientHandshakeGreeting{
		SessionID:     request.SessionID,
		Protocol:      request.Protocol.Clone(),
		ServerVersion: defaultHandshakeString(options.ServerVersion, "quanta"),
		AuthPlugin:    defaultHandshakeString(options.AuthPlugin, string(DefaultAuthenticationPlugin(request.Authentication.Method))),
		CharacterSet:  defaultHandshakeString(options.CharacterSet, "utf8mb4"),
		Collation:     defaultHandshakeString(options.Collation, "utf8mb4_0900_ai_ci"),
		StatusFlags:   append([]ClientHandshakeStatusFlag(nil), options.StatusFlags...),
	}
}

func defaultHandshakeString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
