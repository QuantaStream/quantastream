package qsmysql

import (
	"bytes"
	"context"
	"testing"
)

func TestSessionRunnerDefaultHandshakeUsesTwentyByteAuthSeed(t *testing.T) {
	runner := NewSessionRunner(SessionRunnerConfig{})
	if got := len(runner.Handshake.AuthPluginData); got != 20 {
		t.Fatalf("default auth plugin data length = %d, want 20", got)
	}
}

func TestSessionRunnerHandshakeAuthAndCommandLoop(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	stream := NewStream(&input, &output)
	handler := &testCommandHandler{}
	runner := NewSessionRunner(SessionRunnerConfig{
		ConnectionID:   77,
		AuthPluginData: []byte("12345678901234567890"),
		Stream:         stream,
		Handler:        handler,
	})
	if err := runner.SendHandshake(context.Background()); err != nil {
		t.Fatalf("SendHandshake failed: %v", err)
	}
	if runner.Connection.State != ConnectionStateHandshakeSent {
		t.Fatalf("connection = %#v", runner.Connection)
	}
	packets := readSessionTestPackets(t, output.Bytes())
	if len(packets) != 1 || packets[0].SequenceID != 0 || len(packets[0].Payload) == 0 {
		t.Fatalf("handshake packets = %#v", packets)
	}

	clientResponse, err := EncodePacket(Packet{
		SequenceID: 1,
		Payload: testHandshakeResponsePayload(
			CapabilityProtocol41|CapabilitySecureConnection|CapabilityPluginAuth|CapabilityConnectWithDB,
			"guy",
			[]byte{1, 2, 3},
			"quanta",
			"mysql_native_password",
		),
	})
	if err != nil {
		t.Fatalf("EncodePacket client response failed: %v", err)
	}
	input.Write(clientResponse)
	authOK, err := runner.AcceptHandshakeResponse(context.Background())
	if err != nil {
		t.Fatalf("AcceptHandshakeResponse failed: %v", err)
	}
	if authOK.Kind != CommandResponseOK || !runner.Connection.CanAcceptCommand() || runner.Connection.Username != "guy" {
		t.Fatalf("authOK = %#v connection=%#v", authOK, runner.Connection)
	}
	packets = readSessionTestPackets(t, output.Bytes())
	if len(packets) != 2 || packets[1].SequenceID != 2 || packets[1].Payload[0] != okPacketHeader {
		t.Fatalf("auth packets = %#v", packets)
	}

	ping, err := EncodePacket(Packet{SequenceID: 0, Payload: []byte{byte(CommandPing)}})
	if err != nil {
		t.Fatalf("EncodePacket ping failed: %v", err)
	}
	input.Write(ping)
	commandOK, err := runner.ServeNextCommand(context.Background())
	if err != nil {
		t.Fatalf("ServeNextCommand failed: %v", err)
	}
	if commandOK.Kind != CommandResponseOK || handler.got.Kind != CommandKindPing {
		t.Fatalf("commandOK = %#v handler=%#v", commandOK, handler.got)
	}
	if handler.got.ConnectionID != 77 || handler.got.Username != "guy" || handler.got.Database != "quanta" {
		t.Fatalf("handler command session metadata = %#v, want authenticated connection metadata", handler.got)
	}
	packets = readSessionTestPackets(t, output.Bytes())
	if len(packets) != 3 || packets[2].Payload[0] != okPacketHeader {
		t.Fatalf("all output packets = %#v", packets)
	}
}

func TestSessionRunnerCachingSHA2AuthWritesDirectOKPacket(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	runner := NewSessionRunner(SessionRunnerConfig{
		ConnectionID:   78,
		AuthPluginData: []byte("12345678901234567890"),
		Stream:         NewStream(&input, &output),
		Handler:        &testCommandHandler{},
	})
	if err := runner.SendHandshake(context.Background()); err != nil {
		t.Fatalf("SendHandshake failed: %v", err)
	}
	clientResponse, err := EncodePacket(Packet{
		SequenceID: 1,
		Payload: testHandshakeResponsePayload(
			CapabilityProtocol41|CapabilitySecureConnection|CapabilityPluginAuth|CapabilityConnectWithDB,
			"guy",
			[]byte{1, 2, 3},
			"quanta",
			cachingSHA2PasswordPluginName,
		),
	})
	if err != nil {
		t.Fatalf("EncodePacket client response failed: %v", err)
	}
	input.Write(clientResponse)
	authOK, err := runner.AcceptHandshakeResponse(context.Background())
	if err != nil {
		t.Fatalf("AcceptHandshakeResponse failed: %v", err)
	}
	if authOK.Kind != CommandResponseOK || !runner.Connection.CanAcceptCommand() {
		t.Fatalf("authOK = %#v connection=%#v", authOK, runner.Connection)
	}
	packets := readSessionTestPackets(t, output.Bytes())
	if len(packets) != 2 {
		t.Fatalf("auth packets = %#v, want handshake, direct OK", packets)
	}
	if packets[1].SequenceID != 2 || packets[1].Payload[0] != okPacketHeader {
		t.Fatalf("OK packet = %#v", packets[1])
	}
}

func TestSessionRunnerRejectsCommandsBeforeAuth(t *testing.T) {
	runner := NewSessionRunner(SessionRunnerConfig{
		Stream:  NewStream(bytes.NewReader(nil), &bytes.Buffer{}),
		Handler: &testCommandHandler{},
	})
	if _, err := runner.ServeNextCommand(context.Background()); err == nil {
		t.Fatalf("ServeNextCommand should reject commands before auth")
	}
}

func readSessionTestPackets(t *testing.T, data []byte) []Packet {
	t.Helper()
	reader := bytes.NewReader(data)
	stream := NewStream(reader, nil)
	var packets []Packet
	for reader.Len() > 0 {
		packet, err := stream.ReadPacket(context.Background())
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}
		packets = append(packets, packet)
	}
	return packets
}

func TestSessionRunnerRejectedAuthWritesErrorAndStaysNotReady(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	runner := NewSessionRunner(SessionRunnerConfig{
		ConnectionID:   88,
		AuthPluginData: []byte("12345678901234567890"),
		Stream:         NewStream(&input, &output),
		Handler:        &testCommandHandler{},
		Authenticator:  RejectingAuthenticator{Message: "nope"},
	})
	if err := runner.SendHandshake(context.Background()); err != nil {
		t.Fatalf("SendHandshake failed: %v", err)
	}
	clientResponse, err := EncodePacket(Packet{
		SequenceID: 1,
		Payload: testHandshakeResponsePayload(
			CapabilityProtocol41|CapabilitySecureConnection|CapabilityPluginAuth,
			"guy",
			[]byte{1, 2, 3},
			"",
			"mysql_native_password",
		),
	})
	if err != nil {
		t.Fatalf("EncodePacket client response failed: %v", err)
	}
	input.Write(clientResponse)
	authResponse, err := runner.AcceptHandshakeResponse(context.Background())
	if err != nil {
		t.Fatalf("AcceptHandshakeResponse failed: %v", err)
	}
	if authResponse.Kind != CommandResponseError || runner.Connection.CanAcceptCommand() {
		t.Fatalf("authResponse = %#v connection=%#v", authResponse, runner.Connection)
	}
	packets := readSessionTestPackets(t, output.Bytes())
	if len(packets) != 2 || packets[1].SequenceID != 2 || packets[1].Payload[0] != errPacketHeader {
		t.Fatalf("packets = %#v", packets)
	}
}

func TestSessionRunnerServeRunsUntilQuit(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	handler := &testCommandHandler{}
	runner := NewSessionRunner(SessionRunnerConfig{
		ConnectionID:   99,
		AuthPluginData: []byte("12345678901234567890"),
		Stream:         NewStream(&input, &output),
		Handler:        handler,
	})
	clientResponse, err := EncodePacket(Packet{
		SequenceID: 1,
		Payload: testHandshakeResponsePayload(
			CapabilityProtocol41|CapabilitySecureConnection|CapabilityPluginAuth,
			"guy",
			[]byte{1, 2, 3},
			"",
			"mysql_native_password",
		),
	})
	if err != nil {
		t.Fatalf("EncodePacket client response failed: %v", err)
	}
	ping, err := EncodePacket(Packet{SequenceID: 0, Payload: []byte{byte(CommandPing)}})
	if err != nil {
		t.Fatalf("EncodePacket ping failed: %v", err)
	}
	quit, err := EncodePacket(Packet{SequenceID: 0, Payload: []byte{byte(CommandQuit)}})
	if err != nil {
		t.Fatalf("EncodePacket quit failed: %v", err)
	}
	input.Write(clientResponse)
	input.Write(ping)
	input.Write(quit)
	if err := runner.Serve(context.Background()); err != nil {
		t.Fatalf("Serve failed: %v", err)
	}
	if runner.Connection.State != ConnectionStateClosing || handler.got.Kind != CommandKindQuit {
		t.Fatalf("connection=%#v handler=%#v", runner.Connection, handler.got)
	}
	packets := readSessionTestPackets(t, output.Bytes())
	if len(packets) != 3 || packets[0].SequenceID != 0 || packets[1].Payload[0] != okPacketHeader || packets[2].Payload[0] != okPacketHeader {
		t.Fatalf("packets = %#v", packets)
	}
}
