package qsmysql

import (
	"bytes"
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestStaticAuthenticatorAcceptsCachingSHA2Password(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username:        "guy",
		Password:        "secret",
		DefaultDatabase: "quanta",
		Roles:           []qsbridge.RoleName{"reader"},
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: cachingSHA2PasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   cachingSHA2PasswordToken(seed, "secret"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" || decision.AuthPluginName != cachingSHA2PasswordPluginName {
		t.Fatalf("decision = %#v", decision)
	}
	if len(decision.Roles) != 1 || decision.Roles[0] != "reader" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorTreatsAuthPluginDatabaseAsMissing(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username:        "guy",
		Password:        "secret",
		DefaultDatabase: "quanta",
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		Database:       cachingSHA2PasswordPluginName,
		AuthPluginName: cachingSHA2PasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   cachingSHA2PasswordToken(seed, "secret"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v, want accepted default quanta database", decision)
	}
}

func TestStaticAuthenticatorAcceptsCachingSHA2VerifierWithoutCleartextPassword(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username:                    "guy",
		CachingSHA2PasswordVerifier: cachingSHA2Verifier("secret"),
		DefaultDatabase:             "quanta",
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: cachingSHA2PasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   cachingSHA2PasswordToken(seed, "secret"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorRejectsWrongPassword(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username: "guy",
		Password: "secret",
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: cachingSHA2PasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   cachingSHA2PasswordToken(seed, "wrong"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if decision.Accepted || decision.Failure.VendorCode != 1045 {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorAcceptsMySQLNativePassword(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username: "guy",
		Password: "secret",
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		Database:       "quanta",
		AuthPluginName: mysqlNativePasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   mysqlNativePasswordToken(seed, "secret"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorAcceptsMySQLNativeVerifierWithoutCleartextPassword(t *testing.T) {
	seed := []byte("12345678901234567890")
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username:                    "guy",
		MySQLNativePasswordVerifier: mysqlNativeVerifier("secret"),
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		Database:       "quanta",
		AuthPluginName: mysqlNativePasswordPluginName,
		AuthPluginData: seed,
		AuthResponse:   mysqlNativePasswordToken(seed, "secret"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorAcceptsClearPassword(t *testing.T) {
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username: "guy",
		Password: "secret",
	}}}
	decision, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: mysqlClearPasswordPluginName,
		AuthResponse:   []byte("secret\x00"),
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestStaticAuthenticatorAcceptsEmptyPasswordOnlyWithEmptyAuthResponse(t *testing.T) {
	authenticator := StaticAuthenticator{Accounts: []StaticAccount{{
		Username: "guy",
	}}}
	accepted, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: cachingSHA2PasswordPluginName,
	})
	if err != nil {
		t.Fatalf("Authenticate empty failed: %v", err)
	}
	rejected, err := authenticator.Authenticate(context.Background(), AuthRequest{
		Username:       "guy",
		AuthPluginName: cachingSHA2PasswordPluginName,
		AuthResponse:   []byte{1},
	})
	if err != nil {
		t.Fatalf("Authenticate non-empty failed: %v", err)
	}
	if !accepted.Accepted || rejected.Accepted {
		t.Fatalf("accepted=%#v rejected=%#v", accepted, rejected)
	}
}

func TestSessionRunnerStaticAuthenticatorUsesHandshakeSeed(t *testing.T) {
	seed := []byte("12345678901234567890")
	var input bytes.Buffer
	var output bytes.Buffer
	runner := NewSessionRunner(SessionRunnerConfig{
		ConnectionID:   91,
		AuthPluginData: seed,
		Stream:         NewStream(&input, &output),
		Handler:        &testCommandHandler{},
		Authenticator: StaticAuthenticator{Accounts: []StaticAccount{{
			Username:        "guy",
			Password:        "secret",
			DefaultDatabase: "quanta",
			Roles:           []qsbridge.RoleName{"reader"},
		}}},
	})
	if err := runner.SendHandshake(context.Background()); err != nil {
		t.Fatalf("SendHandshake failed: %v", err)
	}
	clientResponse, err := EncodePacket(Packet{
		SequenceID: 1,
		Payload: testHandshakeResponsePayload(
			CapabilityProtocol41|CapabilitySecureConnection|CapabilityPluginAuth,
			"guy",
			cachingSHA2PasswordToken(seed, "secret"),
			"",
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
	if authOK.Kind != CommandResponseOK || !runner.Connection.CanAcceptCommand() || runner.Connection.Database != "quanta" {
		t.Fatalf("authOK = %#v connection=%#v", authOK, runner.Connection)
	}
	if len(runner.Connection.Roles) != 1 || runner.Connection.Roles[0] != "reader" {
		t.Fatalf("authOK = %#v connection=%#v", authOK, runner.Connection)
	}
}
