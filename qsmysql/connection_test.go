package qsmysql

import "testing"

func TestConnectionStateMachineTracksHandshakeAuthAndReady(t *testing.T) {
	connection := NewConnection(100)
	if connection.CanAcceptCommand() {
		t.Fatal("new connection should not accept commands")
	}
	var err error
	connection, err = connection.WithHandshakeSent()
	if err != nil {
		t.Fatalf("WithHandshakeSent failed: %v", err)
	}
	connection, err = connection.WithAuthPending()
	if err != nil {
		t.Fatalf("WithAuthPending failed: %v", err)
	}
	connection, err = connection.WithReady()
	if err != nil {
		t.Fatalf("WithReady failed: %v", err)
	}
	if !connection.CanAcceptCommand() {
		t.Fatalf("connection = %#v, want ready", connection)
	}
	if _, err := connection.WithHandshakeSent(); err == nil {
		t.Fatal("expected invalid handshake transition to fail")
	}
	if connection.WithClosing().CanAcceptCommand() {
		t.Fatal("closing connection should not accept commands")
	}
}
