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

func TestConnectionAcceptsPermissiveHandshakeAuth(t *testing.T) {
	connection := NewConnection(101)
	var err error
	connection, err = connection.WithHandshakeSent()
	if err != nil {
		t.Fatalf("WithHandshakeSent failed: %v", err)
	}
	response := HandshakeResponse41{
		CapabilityFlags: CapabilityProtocol41 | CapabilitySecureConnection,
		Username:        "guy",
		Database:        "quanta",
		AuthPluginName:  "mysql_native_password",
	}
	connection, err = connection.AcceptHandshakeResponse(response)
	if err != nil {
		t.Fatalf("AcceptHandshakeResponse failed: %v", err)
	}
	if connection.State != ConnectionStateAuthPending || connection.Username != "guy" || connection.Database != "quanta" {
		t.Fatalf("connection = %#v", connection)
	}
	connection, authOK, err := connection.AcceptPermissiveAuth()
	if err != nil {
		t.Fatalf("AcceptPermissiveAuth failed: %v", err)
	}
	if !connection.CanAcceptCommand() || authOK.Kind != CommandResponseOK {
		t.Fatalf("connection = %#v authOK = %#v", connection, authOK)
	}
}
