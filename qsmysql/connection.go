package qsmysql

import "fmt"

// ConnectionState names the socket-free MySQL session state.
type ConnectionState string

const (
	// ConnectionStateNew means no greeting has been sent yet.
	ConnectionStateNew ConnectionState = "new"
	// ConnectionStateHandshakeSent means the initial server greeting was emitted.
	ConnectionStateHandshakeSent ConnectionState = "handshake_sent"
	// ConnectionStateAuthPending means the client handshake response is being evaluated.
	ConnectionStateAuthPending ConnectionState = "auth_pending"
	// ConnectionStateReady means commands can be accepted.
	ConnectionStateReady ConnectionState = "ready"
	// ConnectionStateClosing means the connection owner should close the transport.
	ConnectionStateClosing ConnectionState = "closing"
)

// Connection tracks MySQL protocol state without owning a network socket.
type Connection struct {
	ID    uint32
	State ConnectionState
}

// NewConnection returns a new socket-free MySQL connection state.
func NewConnection(id uint32) Connection {
	return Connection{ID: id, State: ConnectionStateNew}
}

// WithHandshakeSent moves a new connection to handshake-sent state.
func (c Connection) WithHandshakeSent() (Connection, error) {
	if c.State != ConnectionStateNew {
		return c, fmt.Errorf("cannot send handshake from state %s", c.State)
	}
	c.State = ConnectionStateHandshakeSent
	return c, nil
}

// WithAuthPending moves a handshaken connection to auth-pending state.
func (c Connection) WithAuthPending() (Connection, error) {
	if c.State != ConnectionStateHandshakeSent {
		return c, fmt.Errorf("cannot mark auth pending from state %s", c.State)
	}
	c.State = ConnectionStateAuthPending
	return c, nil
}

// WithReady moves a handshaken or auth-pending connection to ready state.
func (c Connection) WithReady() (Connection, error) {
	switch c.State {
	case ConnectionStateHandshakeSent, ConnectionStateAuthPending:
		c.State = ConnectionStateReady
		return c, nil
	default:
		return c, fmt.Errorf("cannot mark ready from state %s", c.State)
	}
}

// WithClosing moves a connection to closing state.
func (c Connection) WithClosing() Connection {
	c.State = ConnectionStateClosing
	return c
}

// CanAcceptCommand reports whether the connection can process COM_* packets.
func (c Connection) CanAcceptCommand() bool {
	return c.State == ConnectionStateReady
}
