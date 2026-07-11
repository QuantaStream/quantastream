package qsruntime

import (
	"bytes"
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestNativeProxyFrontDoorServesMySQLSessionUntilQuit(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	input.Write(nativeProxyTestHandshakeResponsePacket(t))
	input.Write(nativeProxyTestCommandPacket(t, qsmysql.CommandPing))
	input.Write(nativeProxyTestCommandPacket(t, qsmysql.CommandQuit))

	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})
	if err := frontDoor.ServeMySQLSession(context.Background(), qsmysql.NewStream(&input, &output), qsbridge.ExecutionOptions{}); err != nil {
		t.Fatalf("ServeMySQLSession failed: %v", err)
	}
	packets := nativeProxyReadOutputPackets(t, output.Bytes())
	if len(packets) != 3 || packets[0].SequenceID != 0 || packets[1].Payload[0] != 0x00 || packets[2].Payload[0] != 0x00 {
		t.Fatalf("packets = %#v", packets)
	}
}

func TestNativeProxyFrontDoorServesRejectedAuthAsError(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	input.Write(nativeProxyTestHandshakeResponsePacket(t))

	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{
		Authenticator: qsmysql.RejectingAuthenticator{Message: "blocked"},
	})
	if err := frontDoor.ServeMySQLSession(context.Background(), qsmysql.NewStream(&input, &output), qsbridge.ExecutionOptions{}); err != nil {
		t.Fatalf("ServeMySQLSession failed: %v", err)
	}
	packets := nativeProxyReadOutputPackets(t, output.Bytes())
	if len(packets) != 2 || packets[1].Payload[0] != 0xff {
		t.Fatalf("packets = %#v", packets)
	}
}

func nativeProxyTestHandshakeResponsePacket(t *testing.T) []byte {
	t.Helper()
	payload := make([]byte, 0, 96)
	payload = nativeProxyAppendUint32LE(payload, uint32(qsmysql.CapabilityProtocol41|qsmysql.CapabilitySecureConnection|qsmysql.CapabilityPluginAuth|qsmysql.CapabilityConnectWithDB))
	payload = nativeProxyAppendUint32LE(payload, qsmysql.MaxPayloadLength)
	payload = append(payload, byte(qsmysql.CharacterSetUTF8MB4GeneralCI))
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, []byte("guy")...)
	payload = append(payload, 0)
	payload = append(payload, 3, 1, 2, 3)
	payload = append(payload, []byte("quanta")...)
	payload = append(payload, 0)
	payload = append(payload, []byte("mysql_native_password")...)
	payload = append(payload, 0)
	encoded, err := qsmysql.EncodePacket(qsmysql.Packet{SequenceID: 1, Payload: payload})
	if err != nil {
		t.Fatalf("EncodePacket handshake response failed: %v", err)
	}
	return encoded
}

func nativeProxyTestCommandPacket(t *testing.T, command qsmysql.CommandByte) []byte {
	t.Helper()
	encoded, err := qsmysql.EncodePacket(qsmysql.Packet{SequenceID: 0, Payload: []byte{byte(command)}})
	if err != nil {
		t.Fatalf("EncodePacket command failed: %v", err)
	}
	return encoded
}

func nativeProxyReadOutputPackets(t *testing.T, data []byte) []qsmysql.Packet {
	t.Helper()
	reader := bytes.NewReader(data)
	stream := qsmysql.NewStream(reader, nil)
	var packets []qsmysql.Packet
	for reader.Len() > 0 {
		packet, err := stream.ReadPacket(context.Background())
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}
		packets = append(packets, packet)
	}
	return packets
}

func nativeProxyAppendUint32LE(out []byte, value uint32) []byte {
	return append(out, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
}
