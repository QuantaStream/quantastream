package qsmysql

import (
	"context"
	"testing"
)

func TestPermissiveAuthenticatorUsesConfiguredDefaultDatabase(t *testing.T) {
	decision, err := (PermissiveAuthenticator{DefaultDatabase: "quanta"}).Authenticate(context.Background(), AuthRequest{
		Username:       "root",
		AuthPluginName: cachingSHA2PasswordPluginName,
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v, want accepted default quanta database", decision)
	}
}

func TestPermissiveAuthenticatorTreatsAuthPluginDatabaseAsMissing(t *testing.T) {
	decision, err := (PermissiveAuthenticator{DefaultDatabase: "quanta"}).Authenticate(context.Background(), AuthRequest{
		Username:       "root",
		Database:       cachingSHA2PasswordPluginName,
		AuthPluginName: cachingSHA2PasswordPluginName,
	})
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}
	if !decision.Accepted || decision.Database != "quanta" {
		t.Fatalf("decision = %#v, want accepted default quanta database", decision)
	}
}
