package qsmysql

import (
	"context"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

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
	Roles           []qsbridge.RoleName
	CapabilityFlags CapabilityFlag
	CommandLogger   CommandLogger
}

// ServeNext reads, decodes, handles, and writes the response for one command packet.
func (l CommandLoop) ServeNext(ctx context.Context) (CommandResponse, error) {
	start := time.Now()
	packet, err := l.Reader.ReadPacket(ctx)
	if err != nil {
		return CommandResponse{}, err
	}
	command, err := DecodeCommand(packet.Payload)
	if err != nil {
		response := ErrorResponseFromError(err)
		writeErr := writeResponsePackets(ctx, l.Writer, response)
		l.logCommandTrace(CommandTraceEvent{
			ConnectionID: l.ConnectionID,
			Username:     l.Username,
			Database:     l.Database,
			Kind:         CommandKindDecodeError,
			ResponseKind: response.Kind,
			Elapsed:      time.Since(start),
			Error:        commandTraceError(err, writeErr),
		})
		return response, writeErr
	}
	command.ConnectionID = l.ConnectionID
	command.Username = l.Username
	command.Database = l.Database
	command.Roles = append([]qsbridge.RoleName(nil), l.Roles...)
	response, handlerErr := l.Handler.HandleCommand(ctx, command)
	if handlerErr != nil {
		response = ErrorResponseFromError(handlerErr)
	}
	response = response.WithCapabilities(l.CapabilityFlags)
	writeErr := writeResponsePackets(ctx, l.Writer, response)
	event := command.TraceEvent()
	event.ResponseKind = response.Kind
	event.Elapsed = time.Since(start)
	event.Error = commandTraceResponseError(response, handlerErr, writeErr)
	l.logCommandTrace(event)
	return response, writeErr
}

func writeResponsePackets(ctx context.Context, writer PacketWriter, response CommandResponse) error {
	for _, packet := range response.Packets {
		if err := writer.WritePacket(ctx, packet); err != nil {
			return err
		}
	}
	return nil
}

func (l CommandLoop) logCommandTrace(event CommandTraceEvent) {
	if l.CommandLogger != nil {
		l.CommandLogger.LogCommandTrace(event)
	}
}

func commandTraceResponseError(response CommandResponse, handlerErr, writeErr error) string {
	if handlerErr != nil || writeErr != nil {
		return commandTraceError(handlerErr, writeErr)
	}
	if response.ProtocolError != nil {
		return response.ProtocolError.Message
	}
	return ""
}

func commandTraceError(errs ...error) string {
	var messages []string
	for _, err := range errs {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	return strings.Join(messages, "; ")
}
