package qsmysql

import "github.com/QuantaStream/quantastream/qsbridge"

// CommandResponseKind identifies one MySQL command response shape.
type CommandResponseKind string

const (
	// CommandResponseQuery carries text resultset packets.
	CommandResponseQuery CommandResponseKind = "query"
	// CommandResponsePrepared carries COM_STMT_PREPARE response packets.
	CommandResponsePrepared CommandResponseKind = "prepared"
	// CommandResponseOK carries an OK packet.
	CommandResponseOK CommandResponseKind = "ok"
	// CommandResponseError carries an ERR packet.
	CommandResponseError CommandResponseKind = "error"
	// CommandResponseNoResponse writes no packets, as required by COM_STMT_CLOSE.
	CommandResponseNoResponse CommandResponseKind = "no_response"
	// CommandResponseClose asks the connection owner to close without writing packets.
	CommandResponseClose CommandResponseKind = "close"
)

// CommandResponse is the socket-free packet response to one decoded MySQL command.
type CommandResponse struct {
	Kind          CommandResponseKind
	Packets       []Packet
	Close         bool
	Statement     *qsbridge.StatementResult
	ProtocolError *qsbridge.ProtocolError
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

// BinaryQueryResponse encodes a prepared statement query result as MySQL binary-result packets.
func BinaryQueryResponse(result qsbridge.ExecutionResult) (CommandResponse, error) {
	packets, err := BinaryResultSetPackets(result)
	if err != nil {
		return CommandResponse{}, err
	}
	return CommandResponse{Kind: CommandResponseQuery, Packets: packets}, nil
}

// StatementOKResponse encodes a statement response as a MySQL OK packet.
func StatementOKResponse(statement qsbridge.StatementResult) CommandResponse {
	statement = cloneStatementResult(statement)
	return CommandResponse{Kind: CommandResponseOK, Packets: []Packet{OKPacket(1, statement)}, Statement: &statement}
}

// AuthSuccessResponse encodes the successful server side of a MySQL auth exchange.
func AuthSuccessResponse(_ string, capabilities CapabilityFlag) CommandResponse {
	return StatementOKResponse(qsbridge.StatementResult{}).WithCapabilities(capabilities)
}

// WithCapabilities reshapes OK packets for negotiated client capabilities.
func (r CommandResponse) WithCapabilities(capabilities CapabilityFlag) CommandResponse {
	if r.Kind != CommandResponseOK {
		return r
	}
	if r.Statement != nil {
		sequenceID := byte(1)
		if len(r.Packets) > 0 {
			sequenceID = r.Packets[0].SequenceID
		}
		statement := cloneStatementResult(*r.Statement)
		r.Statement = &statement
		r.Packets = []Packet{OKPacketWithCapabilities(sequenceID, statement, capabilities)}
		return r
	}
	if capabilities&CapabilitySessionTrack == 0 {
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

func cloneStatementResult(statement qsbridge.StatementResult) qsbridge.StatementResult {
	statement.Notices = append([]qsbridge.StatementNotice(nil), statement.Notices...)
	statement.SessionActions = append([]qsbridge.SessionAction(nil), statement.SessionActions...)
	return statement
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

// NoResponse returns a command response that intentionally writes no packets.
func NoResponse() CommandResponse {
	return CommandResponse{Kind: CommandResponseNoResponse}
}

// ErrorResponse encodes a protocol error as a MySQL ERR packet.
func ErrorResponse(protocolError qsbridge.ProtocolError) CommandResponse {
	protocolError = cloneProtocolError(protocolError)
	return CommandResponse{Kind: CommandResponseError, Packets: []Packet{ERRPacket(1, protocolError)}, ProtocolError: &protocolError}
}

// ErrorResponseFromError encodes an ordinary Go error as a generic MySQL ERR packet.
func ErrorResponseFromError(err error) CommandResponse {
	return ErrorResponse(ProtocolErrorFromError(err))
}

func cloneProtocolError(protocolError qsbridge.ProtocolError) qsbridge.ProtocolError {
	protocolError.Diagnostic.Fields = append([]qsbridge.FieldRef(nil), protocolError.Diagnostic.Fields...)
	return protocolError
}
