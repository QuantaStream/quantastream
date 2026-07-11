package main

import (
	"strings"
	"testing"
)

func TestMySQLReferenceConfigDefaultsDriverAndRequiresDSN(t *testing.T) {
	config := mysqlReferenceConfig{}.withDefaults()
	if config.Driver != defaultMySQLReferenceDriver {
		t.Fatalf("driver = %q, want %q", config.Driver, defaultMySQLReferenceDriver)
	}
	if err := config.validate(); err == nil {
		t.Fatal("missing DSN should fail validation")
	}
}

func TestNewMySQLReferenceEngineReportsUnknownDriverWithoutDependency(t *testing.T) {
	_, _, err := newMySQLReferenceEngine(mysqlReferenceConfig{Driver: "definitely_missing_driver", DSN: "user:pass@tcp(localhost:3306)/test"})
	if err == nil || !strings.Contains(err.Error(), "unknown driver") {
		t.Fatalf("err = %v, want unknown driver", err)
	}
}
