package qsmysql

import "testing"

func TestByteModelReadinessIsNotNetworkReady(t *testing.T) {
	readiness := ByteModelReadiness()
	if !readiness.PacketCodec || !readiness.Handshake || !readiness.CommandDecoder || !readiness.Resultsets || !readiness.StatementResponses || !readiness.CommandResponses || !readiness.ConnectionState || !readiness.PacketLoop {
		t.Fatalf("readiness = %#v, want byte model pieces ready", readiness)
	}
	if readiness.PacketIOReady() {
		t.Fatalf("readiness = %#v, byte model should not claim network readiness", readiness)
	}
	if readiness.NextStep() != "implement MySQL socket packet IO and command loop" {
		t.Fatalf("next step = %q", readiness.NextStep())
	}
}

func TestAdapterReadinessCanRepresentMountedAdapter(t *testing.T) {
	readiness := AdapterReadiness{
		PacketCodec:        true,
		Handshake:          true,
		CommandDecoder:     true,
		PacketIO:           true,
		Resultsets:         true,
		Authentication:     true,
		StatementResponses: true,
		CommandResponses:   true,
		ConnectionState:    true,
		PacketLoop:         true,
	}
	if !readiness.PacketIOReady() || readiness.NextStep() != "" {
		t.Fatalf("readiness = %#v", readiness)
	}
}
