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

func TestConnectionAcceptsCachingSHA2PermissiveHandshakeAuth(t *testing.T) {
	connection := NewConnection(102)
	var err error
	connection, err = connection.WithHandshakeSent()
	if err != nil {
		t.Fatalf("WithHandshakeSent failed: %v", err)
	}
	connection, err = connection.AcceptHandshakeResponse(HandshakeResponse41{
		CapabilityFlags: CapabilityProtocol41 | CapabilitySecureConnection | CapabilityPluginAuth,
		Username:        "guy",
		Database:        "quanta",
		AuthPluginName:  cachingSHA2PasswordPluginName,
	})
	if err != nil {
		t.Fatalf("AcceptHandshakeResponse failed: %v", err)
	}
	connection, authOK, err := connection.AcceptPermissiveAuth()
	if err != nil {
		t.Fatalf("AcceptPermissiveAuth failed: %v", err)
	}
	if !connection.CanAcceptCommand() || authOK.Kind != CommandResponseOK || len(authOK.Packets) != 2 {
		t.Fatalf("connection = %#v authOK = %#v", connection, authOK)
	}
	if string(authOK.Packets[0].Payload) != string([]byte{authMoreDataPacketHeader, cachingSHA2FastAuthSuccess}) {
		t.Fatalf("first auth packet = %#v, want caching_sha2 fast auth success", authOK.Packets[0])
	}
	if authOK.Packets[1].Payload[0] != okPacketHeader {
		t.Fatalf("second auth packet = %#v, want OK", authOK.Packets[1])
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
