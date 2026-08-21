package qsmysql

import "context"

// PacketReader reads decoded MySQL packets from a transport or test stream.
type PacketReader interface {
	ReadPacket(context.Context) (Packet, error)
}

// PacketWriter writes decoded MySQL packets to a transport or test stream.
type PacketWriter interface {
	WritePacket(context.Context, Packet) error
}

// CommandHandler handles one decoded MySQL command.
type CommandHandler interface {
	HandleCommand(context.Context, Command) (CommandResponse, error)
}

// CommandLoop is a socket-free MySQL command loop over packet reader/writer interfaces.
type CommandLoop struct {
	Reader          PacketReader
	Writer          PacketWriter
	Handler         CommandHandler
	ConnectionID    uint32
	Username        string
	Database        string
	CapabilityFlags CapabilityFlag
}

// ServeNext reads, decodes, handles, and writes the response for one command packet.
func (l CommandLoop) ServeNext(ctx context.Context) (CommandResponse, error) {
	packet, err := l.Reader.ReadPacket(ctx)
	if err != nil {
		return CommandResponse{}, err
	}
	command, err := DecodeCommand(packet.Payload)
	if err != nil {
		response := ErrorResponseFromError(err)
		return response, writeResponsePackets(ctx, l.Writer, response)
	}
	command.ConnectionID = l.ConnectionID
	command.Username = l.Username
	command.Database = l.Database
	response, err := l.Handler.HandleCommand(ctx, command)
	if err != nil {
		response = ErrorResponseFromError(err)
	}
	response = response.WithCapabilities(l.CapabilityFlags)
	return response, writeResponsePackets(ctx, l.Writer, response)
}

func writeResponsePackets(ctx context.Context, writer PacketWriter, response CommandResponse) error {
	for _, packet := range response.Packets {
		if err := writer.WritePacket(ctx, packet); err != nil {
			return err
		}
	}
	return nil
}
