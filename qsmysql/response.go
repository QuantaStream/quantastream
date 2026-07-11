package qsmysql

import "github.com/QuantaStream/quantastream/qsbridge"

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
