package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRunStatusPrintsInaboxStandardSkeleton(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-status"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{
		"mode=inabox-standard",
		"mysql=127.0.0.1:4000",
		"local_node_ready=false",
		"streaming_risk=",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

func TestRunStatusPrintsNativeGRPCWhenConfigured(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-status", "-native-grpc-bind", "0.0.0.0", "-native-grpc-port", "4100"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "native_grpc=0.0.0.0:4100") {
		t.Fatalf("stdout missing native gRPC address:\n%s", stdout.String())
	}
}

func TestRunRejectsUnsupportedMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-mode", "distributed"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("unsupported mode exited successfully")
	}
	if !strings.Contains(stderr.String(), "unsupported mode") {
		t.Fatalf("stderr = %q, want unsupported mode", stderr.String())
	}
}

func TestRunStartsInaboxStandardListenerUntilContextCanceled(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeCommandTestSchema(t, configDir, "sample")
	port := reserveCommandTestPort(t)
	address := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runWithContext(ctx, []string{
			"-bind", "127.0.0.1",
			"-mysql-port", strconv.Itoa(port),
			"-config-dir", configDir,
			"-data-dir", filepath.Join(root, "data"),
		}, &stdout, &stderr)
	}()

	conn := dialCommandTestListener(t, done, address)
	_ = conn.Close()
	cancel()

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, stdout = %s stderr = %s", code, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("server did not stop after context cancellation; stdout = %s stderr = %s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "listening="+address) {
		t.Fatalf("stdout missing listener address %q:\n%s", address, stdout.String())
	}
}

func reserveCommandTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func dialCommandTestListener(t *testing.T, done <-chan int, address string) net.Conn {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case code := <-done:
			t.Fatalf("server exited before accepting connections with code %d", code)
		case <-deadline:
			t.Fatalf("timed out waiting for listener %s", address)
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				return conn
			}
		}
	}
}

func writeCommandTestSchema(t *testing.T, configDir, table string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	schema := `tableName: sample
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: IntBSI
  type: Integer
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}
