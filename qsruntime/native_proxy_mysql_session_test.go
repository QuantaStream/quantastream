package qsruntime

import (
	"bytes"
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestNativeProxyMySQLSessionConfigDefaultsTwentyByteAuthSeed(t *testing.T) {
	config := NativeProxyMySQLSessionConfig{}.WithDefaults()
	if got := len(config.AuthPluginData); got != 20 {
		t.Fatalf("default auth plugin data length = %d, want 20", got)
	}
}

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

func TestNativeProxyFrontDoorServesMySQLSessionWithConfiguredConnectionID(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	input.Write(nativeProxyTestHandshakeResponsePacket(t))
	input.Write(nativeProxyTestCommandPacket(t, qsmysql.CommandQuit))

	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})
	err := frontDoor.ServeMySQLSessionWithConfig(context.Background(), qsmysql.NewStream(&input, &output), NativeProxyMySQLSessionConfig{
		ConnectionID: 77,
	})
	if err != nil {
		t.Fatalf("ServeMySQLSessionWithConfig failed: %v", err)
	}
	packets := nativeProxyReadOutputPackets(t, output.Bytes())
	if got := nativeProxyHandshakeConnectionID(t, packets[0]); got != 77 {
		t.Fatalf("handshake connection id = %d, want 77", got)
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

func TestNativeProxyFrontDoorServesStaticAuthRoleGrant(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	input.Write(nativeProxyTestHandshakeResponsePacketWithAuth(t, "guy", []byte("secret\x00"), "quanta", "mysql_clear_password"))
	input.Write(nativeProxyTestQueryPacket(t, "select o_orderkey from orders"))
	input.Write(nativeProxyTestCommandPacket(t, qsmysql.CommandQuit))

	executed := false
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{Count: 1}, nil
	})}
	authorizer := &nativeProxyTestRecordingAuthorizer{Policy: qsbridge.NewAccessPolicy(qsbridge.AccessGrant{
		PrincipalKind: qsbridge.AccessPrincipalRole,
		Principal:     "reader",
		Privilege:     qsbridge.AccessSelect,
		Table:         qsbridge.TableInstance{Schema: "quanta", Table: "orders"},
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{
		Authenticator: qsmysql.StaticAuthenticator{Accounts: []qsmysql.StaticAccount{{
			Username:        "guy",
			Password:        "secret",
			DefaultDatabase: "quanta",
			Roles:           []qsbridge.RoleName{"reader"},
		}}},
		Server: NativeProxyServerConfig{Authorizer: authorizer},
	})

	if err := frontDoor.ServeMySQLSession(context.Background(), qsmysql.NewStream(&input, &output), qsbridge.ExecutionOptions{}); err != nil {
		t.Fatalf("ServeMySQLSession failed: %v", err)
	}
	if !executed {
		t.Fatalf("query should execute when static auth role satisfies access policy; authz requests=%#v", authorizer.Requests)
	}
	if len(authorizer.Requests) == 0 {
		t.Fatalf("authz requests = %#v, want at least one authorization check", authorizer.Requests)
	}
	for _, request := range authorizer.Requests {
		if len(request.Session.Roles) != 1 || request.Session.Roles[0] != "reader" {
			t.Fatalf("authz requests = %#v, want reader role visible", authorizer.Requests)
		}
	}
	packets := nativeProxyReadOutputPackets(t, output.Bytes())
	if len(packets) != 6 || packets[1].Payload[0] != 0x00 || packets[2].Payload[0] != 0x01 {
		t.Fatalf("packets = %#v, want handshake, auth OK, one-column query resultset", packets)
	}
}

func TestNativeProxyFrontDoorDeniesStaticAuthWithoutRoleGrant(t *testing.T) {
	var input bytes.Buffer
	var output bytes.Buffer
	input.Write(nativeProxyTestHandshakeResponsePacketWithAuth(t, "guy", []byte("secret\x00"), "quanta", "mysql_clear_password"))
	input.Write(nativeProxyTestQueryPacket(t, "select o_orderkey from orders"))
	input.Write(nativeProxyTestCommandPacket(t, qsmysql.CommandQuit))

	executed := false
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{
		Authenticator: qsmysql.StaticAuthenticator{Accounts: []qsmysql.StaticAccount{{
			Username:        "guy",
			Password:        "secret",
			DefaultDatabase: "quanta",
		}}},
		Server: NativeProxyServerConfig{Authorizer: qsbridge.NewAccessPolicy(qsbridge.AccessGrant{
			PrincipalKind: qsbridge.AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     qsbridge.AccessSelect,
			Table:         qsbridge.TableInstance{Schema: "quanta", Table: "orders"},
		})},
	})

	if err := frontDoor.ServeMySQLSession(context.Background(), qsmysql.NewStream(&input, &output), qsbridge.ExecutionOptions{}); err != nil {
		t.Fatalf("ServeMySQLSession failed: %v", err)
	}
	if executed {
		t.Fatalf("query should not execute when static auth account lacks granted role")
	}
	packets := nativeProxyReadOutputPackets(t, output.Bytes())
	if len(packets) != 3 || packets[1].Payload[0] != 0x00 || packets[2].Payload[0] != 0xff {
		t.Fatalf("packets = %#v, want handshake, auth OK, access denied", packets)
	}
}

type nativeProxyTestRecordingAuthorizer struct {
	Policy   qsbridge.AccessPolicy
	Requests []qsbridge.AuthorizationRequest
}

func (a *nativeProxyTestRecordingAuthorizer) AuthorizeAccess(request qsbridge.AuthorizationRequest) qsbridge.AuthorizationDecision {
	a.Requests = append(a.Requests, request.Clone())
	return request.Authorize(a.Policy)
}

func nativeProxyTestHandshakeResponsePacket(t *testing.T) []byte {
	return nativeProxyTestHandshakeResponsePacketWithAuth(t, "guy", []byte{1, 2, 3}, "quanta", "mysql_native_password")
}

func nativeProxyTestHandshakeResponsePacketWithAuth(t *testing.T, username string, auth []byte, database string, plugin string) []byte {
	t.Helper()
	payload := make([]byte, 0, 96)
	payload = nativeProxyAppendUint32LE(payload, uint32(qsmysql.CapabilityProtocol41|qsmysql.CapabilitySecureConnection|qsmysql.CapabilityPluginAuth|qsmysql.CapabilityConnectWithDB))
	payload = nativeProxyAppendUint32LE(payload, qsmysql.MaxPayloadLength)
	payload = append(payload, byte(qsmysql.CharacterSetUTF8MB4GeneralCI))
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, []byte(username)...)
	payload = append(payload, 0)
	payload = append(payload, byte(len(auth)))
	payload = append(payload, auth...)
	payload = append(payload, []byte(database)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(plugin)...)
	payload = append(payload, 0)
	encoded, err := qsmysql.EncodePacket(qsmysql.Packet{SequenceID: 1, Payload: payload})
	if err != nil {
		t.Fatalf("EncodePacket handshake response failed: %v", err)
	}
	return encoded
}

func nativeProxyTestQueryPacket(t *testing.T, sql string) []byte {
	t.Helper()
	payload := append([]byte{byte(qsmysql.CommandQuery)}, []byte(sql)...)
	encoded, err := qsmysql.EncodePacket(qsmysql.Packet{SequenceID: 0, Payload: payload})
	if err != nil {
		t.Fatalf("EncodePacket query failed: %v", err)
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
