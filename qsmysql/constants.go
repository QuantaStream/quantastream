package qsmysql

const (
	// MaxPayloadLength is the largest payload length that fits in the MySQL packet header.
	MaxPayloadLength = 1<<24 - 1
	// PacketHeaderLength is the fixed MySQL packet header length.
	PacketHeaderLength = 4
)

// CommandByte identifies a MySQL command packet type.
type CommandByte byte

const (
	// CommandQuit is COM_QUIT.
	CommandQuit CommandByte = 0x01
	// CommandQuery is COM_QUERY.
	CommandQuery CommandByte = 0x03
	// CommandFieldList is COM_FIELD_LIST.
	CommandFieldList CommandByte = 0x04
	// CommandPing is COM_PING.
	CommandPing CommandByte = 0x0e
	// CommandStmtPrepare is COM_STMT_PREPARE.
	CommandStmtPrepare CommandByte = 0x16
	// CommandStmtExecute is COM_STMT_EXECUTE.
	CommandStmtExecute CommandByte = 0x17
	// CommandStmtSendLongData is COM_STMT_SEND_LONG_DATA.
	CommandStmtSendLongData CommandByte = 0x18
	// CommandStmtClose is COM_STMT_CLOSE.
	CommandStmtClose CommandByte = 0x19
	// CommandStmtReset is COM_STMT_RESET.
	CommandStmtReset CommandByte = 0x1a
)

// CapabilityFlag identifies one MySQL handshake capability bit.
type CapabilityFlag uint32

const (
	// CapabilityLongPassword is CLIENT_LONG_PASSWORD.
	CapabilityLongPassword CapabilityFlag = 0x00000001
	// CapabilityProtocol41 is CLIENT_PROTOCOL_41.
	CapabilityProtocol41 CapabilityFlag = 0x00000200
	// CapabilityConnectWithDB is CLIENT_CONNECT_WITH_DB.
	CapabilityConnectWithDB CapabilityFlag = 0x00000008
	// CapabilitySecureConnection is CLIENT_SECURE_CONNECTION.
	CapabilitySecureConnection CapabilityFlag = 0x00008000
	// CapabilityPluginAuth is CLIENT_PLUGIN_AUTH.
	CapabilityPluginAuth CapabilityFlag = 0x00080000
	// CapabilitySessionTrack is CLIENT_SESSION_TRACK.
	CapabilitySessionTrack CapabilityFlag = 0x00800000
)

// CharacterSet identifies a MySQL connection character set.
type CharacterSet byte

const (
	// CharacterSetUTF8MB4GeneralCI is MySQL collation utf8mb4_general_ci.
	CharacterSetUTF8MB4GeneralCI CharacterSet = 45
)

// StatusFlag identifies one MySQL server status flag.
type StatusFlag uint16

const (
	// StatusAutocommit is SERVER_STATUS_AUTOCOMMIT.
	StatusAutocommit StatusFlag = 0x0002
)
