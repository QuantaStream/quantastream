package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectRuntimeConfigAppliesServicePortDefault(t *testing.T) {
	config := NewDirectRuntimeConfig("/tmp/quanta", "127.0.0.1:8500", 0, 0)

	withDefaults := config.WithDefaults()
	if withDefaults.ServicePort != DefaultDirectServicePort {
		t.Fatalf("service port = %d, want %d", withDefaults.ServicePort, DefaultDirectServicePort)
	}
	if config.ServicePort != 0 {
		t.Fatalf("source config was mutated: %#v", config)
	}
}

func TestDirectRuntimeConfigValidatesNegativeValues(t *testing.T) {
	config := NewDirectRuntimeConfig("", "", -1, -2)

	diagnostics := config.Validate()
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected invalid execution option diagnostics")
	}
	codes := diagnostics.Codes()
	if len(codes) != 2 {
		t.Fatalf("diagnostic count = %d, want 2", len(codes))
	}
	for _, code := range codes {
		if code != qsbridge.DiagnosticInvalidExecutionOption {
			t.Fatalf("diagnostic code = %q, want %q", code, qsbridge.DiagnosticInvalidExecutionOption)
		}
	}
}

func TestDirectRuntimeConfigAllowsLegacySessionPoolDefault(t *testing.T) {
	config := NewDirectRuntimeConfig("", "", DefaultDirectServicePort, 0)

	if diagnostics := config.Validate(); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
}

func TestDirectRuntimeConfigReturnsQuantaSourceArgs(t *testing.T) {
	config := NewDirectRuntimeConfig("/data/quanta", "127.0.0.1:8500", 0, 8)

	args := config.QuantaSourceArgs()
	if args.BaseDir != "/data/quanta" {
		t.Fatalf("base dir = %q, want /data/quanta", args.BaseDir)
	}
	if args.ConsulAddress != "127.0.0.1:8500" {
		t.Fatalf("consul address = %q, want 127.0.0.1:8500", args.ConsulAddress)
	}
	if args.ServicePort != DefaultDirectServicePort {
		t.Fatalf("service port = %d, want %d", args.ServicePort, DefaultDirectServicePort)
	}
	if args.SessionPoolSize != 8 {
		t.Fatalf("session pool size = %d, want 8", args.SessionPoolSize)
	}
}
