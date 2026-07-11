package qsmysql

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// CommandResponseKind identifies one MySQL command response shape.
type CommandResponseKind string

const (
	// CommandResponseQuery carries text resultset packets.
	CommandResponseQuery CommandResponseKind = "query"
	// CommandResponseOK carries an OK packet.
	CommandResponseOK CommandResponseKind = "ok"
	// CommandResponseError carries an ERR packet.
	CommandResponseError CommandResponseKind = "error"
	// CommandResponseClose asks the connection owner to close without writing packets.
	CommandResponseClose CommandResponseKind = "close"
)

// CommandResponse is the socket-free packet response to one decoded MySQL command.
type CommandResponse struct {
	Kind    CommandResponseKind
	Packets []Packet
	Close   bool
}

const (
	authMoreDataPacketHeader      = 0x01
	cachingSHA2FastAuthSuccess    = 0x03
	cachingSHA2PasswordPluginName = "caching_sha2_password"
)

// QueryResponse encodes a query result as MySQL text-result packets.
func QueryResponse(result qsbridge.ExecutionResult) (CommandResponse, error) {
	packets, err := TextResultSetPackets(result)
	if err != nil {
		return CommandResponse{}, err
	}
	return CommandResponse{Kind: CommandResponseQuery, Packets: packets}, nil
}

// StatementOKResponse encodes a statement response as a MySQL OK packet.
func StatementOKResponse(statement qsbridge.StatementResult) CommandResponse {
	return CommandResponse{Kind: CommandResponseOK, Packets: []Packet{OKPacket(1, statement)}}
}

// AuthSuccessResponse encodes the successful server side of a MySQL auth exchange.
func AuthSuccessResponse(pluginName string, capabilities CapabilityFlag) CommandResponse {
	if strings.TrimSpace(pluginName) == "" {
		pluginName = defaultAuthPluginName
	}
	if strings.EqualFold(pluginName, cachingSHA2PasswordPluginName) {
		return CommandResponse{Kind: CommandResponseOK, Packets: []Packet{
			{SequenceID: 1, Payload: []byte{authMoreDataPacketHeader, cachingSHA2FastAuthSuccess}},
			OKPacketWithCapabilities(2, qsbridge.StatementResult{}, capabilities),
		}}
	}
	return StatementOKResponse(qsbridge.StatementResult{}).WithCapabilities(capabilities)
}

// WithCapabilities reshapes OK packets for negotiated client capabilities.
func (r CommandResponse) WithCapabilities(capabilities CapabilityFlag) CommandResponse {
	if capabilities&CapabilitySessionTrack == 0 || r.Kind != CommandResponseOK {
		return r
	}
	r.Packets = clonePackets(r.Packets)
	for i := range r.Packets {
		if len(r.Packets[i].Payload) == 7 && r.Packets[i].Payload[0] == okPacketHeader {
			r.Packets[i].Payload = append(r.Packets[i].Payload, 0)
		}
	}
	return r
}

func clonePackets(packets []Packet) []Packet {
	cloned := make([]Packet, len(packets))
	for i, packet := range packets {
		cloned[i] = Packet{SequenceID: packet.SequenceID, Payload: append([]byte(nil), packet.Payload...)}
	}
	return cloned
}

// PingResponse returns the OK response for COM_PING.
func PingResponse() CommandResponse {
	return StatementOKResponse(qsbridge.StatementResult{})
}

// QuitResponse returns the close response for COM_QUIT.
func QuitResponse() CommandResponse {
	return CommandResponse{Kind: CommandResponseClose, Close: true}
}

// ErrorResponse encodes a protocol error as a MySQL ERR packet.
func ErrorResponse(protocolError qsbridge.ProtocolError) CommandResponse {
	return CommandResponse{Kind: CommandResponseError, Packets: []Packet{ERRPacket(1, protocolError)}}
}

// ErrorResponseFromError encodes an ordinary Go error as a generic MySQL ERR packet.
func ErrorResponseFromError(err error) CommandResponse {
	return ErrorResponse(ProtocolErrorFromError(err))
}
