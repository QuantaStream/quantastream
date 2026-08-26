package qsmysql

import (
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// CommandKind identifies a decoded MySQL command.
type CommandKind string

const (
	// CommandKindQuery is a decoded COM_QUERY command.
	CommandKindQuery CommandKind = "query"
	// CommandKindFieldList is a decoded COM_FIELD_LIST command.
	CommandKindFieldList CommandKind = "field_list"
	// CommandKindStmtPrepare is a decoded COM_STMT_PREPARE command.
	CommandKindStmtPrepare CommandKind = "stmt_prepare"
	// CommandKindStmtExecute is a decoded COM_STMT_EXECUTE command.
	CommandKindStmtExecute CommandKind = "stmt_execute"
	// CommandKindStmtSendLongData is a decoded COM_STMT_SEND_LONG_DATA command.
	CommandKindStmtSendLongData CommandKind = "stmt_send_long_data"
	// CommandKindStmtClose is a decoded COM_STMT_CLOSE command.
	CommandKindStmtClose CommandKind = "stmt_close"
	// CommandKindStmtReset is a decoded COM_STMT_RESET command.
	CommandKindStmtReset CommandKind = "stmt_reset"
	// CommandKindPing is a decoded COM_PING command.
	CommandKindPing CommandKind = "ping"
	// CommandKindQuit is a decoded COM_QUIT command.
	CommandKindQuit CommandKind = "quit"
)

// Command is the protocol-neutral result of decoding a MySQL command packet payload.
type Command struct {
	Kind         CommandKind
	SQL          string
	Table        string
	FieldPattern string
	StatementID  uint32
	Execute      PreparedExecuteCommand
	LongData     PreparedLongDataCommand
	ConnectionID uint32
	Username     string
	Database     string
	Roles        []qsbridge.RoleName
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
	case CommandFieldList:
		table, pattern, err := decodeFieldListCommand(payload)
		if err != nil {
			return Command{}, err
		}
		return Command{Kind: CommandKindFieldList, Table: table, FieldPattern: pattern}, nil
	case CommandStmtPrepare:
		sql := string(payload[1:])
		if strings.TrimSpace(sql) == "" {
			return Command{}, fmt.Errorf("COM_STMT_PREPARE requires SQL text")
		}
		return Command{Kind: CommandKindStmtPrepare, SQL: sql}, nil
	case CommandStmtExecute:
		execute, err := DecodePreparedExecuteCommand(payload)
		if err != nil {
			return Command{}, err
		}
		return Command{Kind: CommandKindStmtExecute, StatementID: execute.StatementID, Execute: execute}, nil
	case CommandStmtSendLongData:
		longData, err := DecodePreparedLongDataCommand(payload)
		if err != nil {
			return Command{}, err
		}
		return Command{Kind: CommandKindStmtSendLongData, StatementID: longData.StatementID, LongData: longData}, nil
	case CommandStmtClose:
		statementID, err := decodeStatementIDCommand(CommandStmtClose, payload)
		if err != nil {
			return Command{}, err
		}
		return Command{Kind: CommandKindStmtClose, StatementID: statementID}, nil
	case CommandStmtReset:
		statementID, err := decodeStatementIDCommand(CommandStmtReset, payload)
		if err != nil {
			return Command{}, err
		}
		return Command{Kind: CommandKindStmtReset, StatementID: statementID}, nil
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

func decodeFieldListCommand(payload []byte) (string, string, error) {
	if len(payload) < 3 {
		return "", "", fmt.Errorf("COM_FIELD_LIST requires table name and terminator")
	}
	body := payload[1:]
	terminator := -1
	for i, b := range body {
		if b == 0 {
			terminator = i
			break
		}
	}
	if terminator < 0 {
		return "", "", fmt.Errorf("COM_FIELD_LIST table name must be null-terminated")
	}
	table := strings.TrimSpace(string(body[:terminator]))
	if table == "" {
		return "", "", fmt.Errorf("COM_FIELD_LIST requires table name")
	}
	return table, string(body[terminator+1:]), nil
}

func decodeStatementIDCommand(command CommandByte, payload []byte) (uint32, error) {
	if len(payload) != 5 {
		return 0, fmt.Errorf("%s payload must include only a statement id", commandName(command))
	}
	return readUint32LE(payload[1:5]), nil
}

func commandName(command CommandByte) string {
	switch command {
	case CommandStmtSendLongData:
		return "COM_STMT_SEND_LONG_DATA"
	case CommandStmtClose:
		return "COM_STMT_CLOSE"
	case CommandStmtReset:
		return "COM_STMT_RESET"
	default:
		return fmt.Sprintf("command 0x%02x", command)
	}
}
