package qsmysql

import (
	"fmt"
	"strings"
)

// CommandKind identifies a decoded MySQL command.
type CommandKind string

const (
	// CommandKindQuery is a decoded COM_QUERY command.
	CommandKindQuery CommandKind = "query"
	// CommandKindPing is a decoded COM_PING command.
	CommandKindPing CommandKind = "ping"
	// CommandKindQuit is a decoded COM_QUIT command.
	CommandKindQuit CommandKind = "quit"
)

// Command is the protocol-neutral result of decoding a MySQL command packet payload.
type Command struct {
	Kind         CommandKind
	SQL          string
	ConnectionID uint32
	Database     string
}

// DecodeCommand decodes the payload of a MySQL command packet.
func DecodeCommand(payload []byte) (Command, error) {
	if len(payload) == 0 {
		return Command{}, fmt.Errorf("mysql command payload is empty")
	}
	switch CommandByte(payload[0]) {
	case CommandQuery:
		sql := string(payload[1:])
		if strings.TrimSpace(sql) == "" {
			return Command{}, fmt.Errorf("COM_QUERY requires SQL text")
		}
		return Command{Kind: CommandKindQuery, SQL: sql}, nil
	case CommandPing:
		if len(payload) != 1 {
			return Command{}, fmt.Errorf("COM_PING payload must not include arguments")
		}
		return Command{Kind: CommandKindPing}, nil
	case CommandQuit:
		if len(payload) != 1 {
			return Command{}, fmt.Errorf("COM_QUIT payload must not include arguments")
		}
		return Command{Kind: CommandKindQuit}, nil
	default:
		return Command{}, fmt.Errorf("unsupported mysql command byte 0x%02x", payload[0])
	}
}
