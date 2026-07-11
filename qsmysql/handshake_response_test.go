package qsmysql

import (
	"bytes"
	"strings"
	"testing"
)

func TestDecodeHandshakeResponse41ExtractsClientFields(t *testing.T) {
	payload := testHandshakeResponsePayload(
		CapabilityProtocol41|CapabilitySecureConnection|CapabilityConnectWithDB|CapabilityPluginAuth,
		"guy",
		[]byte{1, 2, 3, 4},
		"quanta",
		"mysql_native_password",
	)
	response, err := DecodeHandshakeResponse41(payload)
	if err != nil {
		t.Fatalf("DecodeHandshakeResponse41 failed: %v", err)
	}
	if response.Username != "guy" || response.Database != "quanta" || response.AuthPluginName != "mysql_native_password" {
		t.Fatalf("response = %#v", response)
	}
	if response.CharacterSet != CharacterSetUTF8MB4GeneralCI || response.MaxPacketSize != MaxPayloadLength {
		t.Fatalf("response metadata = %#v", response)
	}
	if !bytes.Equal(response.AuthResponse, []byte{1, 2, 3, 4}) {
		t.Fatalf("auth response = %v", response.AuthResponse)
	}
}

func TestDecodeHandshakeResponse41RejectsNonProtocol41(t *testing.T) {
	payload := testHandshakeResponsePayload(CapabilitySecureConnection, "guy", nil, "", "")
	if _, err := DecodeHandshakeResponse41(payload); err == nil || !strings.Contains(err.Error(), "CLIENT_PROTOCOL_41") {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeHandshakeResponse41RejectsTruncatedAuthResponse(t *testing.T) {
	payload := testHandshakeResponsePayload(CapabilityProtocol41|CapabilitySecureConnection, "guy", []byte{1, 2, 3}, "", "")
	payload = payload[:len(payload)-2]
	if _, err := DecodeHandshakeResponse41(payload); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("err = %v", err)
	}
}

func testHandshakeResponsePayload(flags CapabilityFlag, username string, auth []byte, database string, plugin string) []byte {
	payload := make([]byte, 0, 64)
	payload = appendUint32LE(payload, uint32(flags))
	payload = appendUint32LE(payload, MaxPayloadLength)
	payload = append(payload, byte(CharacterSetUTF8MB4GeneralCI))
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, username...)
	payload = append(payload, 0)
	if flags&CapabilitySecureConnection != 0 {
		payload = append(payload, byte(len(auth)))
		payload = append(payload, auth...)
	} else {
		payload = append(payload, auth...)
		payload = append(payload, 0)
	}
	if flags&CapabilityConnectWithDB != 0 {
		payload = append(payload, database...)
		payload = append(payload, 0)
	}
	if flags&CapabilityPluginAuth != 0 {
		payload = append(payload, plugin...)
		payload = append(payload, 0)
	}
	return payload
}
