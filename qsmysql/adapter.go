package qsmysql

// AdapterReadiness describes whether the pure MySQL byte adapter can be mounted.
type AdapterReadiness struct {
	PacketCodec    bool
	Handshake      bool
	CommandDecoder bool
	PacketIO       bool
	Resultsets     bool
	Authentication bool
}

// ByteModelReadiness reports readiness for the current socket-free adapter slice.
func ByteModelReadiness() AdapterReadiness {
	return AdapterReadiness{
		PacketCodec:    true,
		Handshake:      true,
		CommandDecoder: true,
		Resultsets:     true,
	}
}

// PacketIOReady reports whether the adapter can accept network clients.
func (r AdapterReadiness) PacketIOReady() bool {
	return r.PacketCodec && r.Handshake && r.CommandDecoder && r.PacketIO && r.Resultsets && r.Authentication
}

// NextStep describes the first missing adapter milestone.
func (r AdapterReadiness) NextStep() string {
	switch {
	case !r.PacketCodec:
		return "implement MySQL packet codec"
	case !r.Handshake:
		return "implement MySQL handshake model"
	case !r.CommandDecoder:
		return "implement MySQL command decoder"
	case !r.PacketIO:
		return "implement MySQL socket packet IO and command loop"
	case !r.Authentication:
		return "implement MySQL-compatible authentication exchange"
	case !r.Resultsets:
		return "implement MySQL resultset and OK/ERR packet serialization"
	default:
		return ""
	}
}
