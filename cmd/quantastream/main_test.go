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

	"github.com/QuantaStream/quantastream/core"
)

func TestRunStatusPrintsInaboxStandardSkeleton(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{
		"mode=inabox-standard",
		"mysql=127.0.0.1:4000",
		"local_node_ready=false",
		"auth=permissive",
		"bsi_pk_authority_manifest=missing",
		"streaming_risk=",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

func TestRunRejectsOmittedAuthMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-status"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("omitted auth mode exited successfully: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "mysql auth mode is required") {
		t.Fatalf("stderr = %q, want required auth guidance", stderr.String())
	}
}

func TestRunRejectsPermissiveAuthOnNetworkBind(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-bind", "0.0.0.0", "-status"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("network permissive auth exited successfully: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "requires a loopback bind") {
		t.Fatalf("stderr = %q, want loopback guidance", stderr.String())
	}
}

func TestRunStatusPrintsStaticAuthWithoutPassword(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-auth-mode", "static", "-auth-user", "bench", "-auth-password", "secret"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{"auth=static", "auth_user=bench"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "secret") {
		t.Fatalf("output leaked password material:\n%s", output)
	}
}

func TestRunStatusPrintsStaticAuthAccountFile(t *testing.T) {
	accountFile := filepath.Join(t.TempDir(), "accounts.yaml")
	if err := os.WriteFile(accountFile, []byte("accounts:\n  - username: bench\n"), 0o600); err != nil {
		t.Fatalf("write account file: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-auth-mode", "static", "-auth-account-file", accountFile}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{"auth=static", "auth_account_file=" + accountFile} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "auth_user=") {
		t.Fatalf("output should not print one auth user when account file is configured:\n%s", output)
	}
}

func TestRunStatusPrintsAccessPolicyFile(t *testing.T) {
	policyFile := writeCommandTestAccessPolicyFile(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-access-policy-file", policyFile}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{"authorization=static_policy", "access_policy_file=" + policyFile} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

func TestRunRejectsInvalidAccessPolicyFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-access-policy-file", filepath.Join(t.TempDir(), "missing.yaml")}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("missing policy file exited successfully; stdout=%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "configure access policy") {
		t.Fatalf("stderr = %q, want access policy error", stderr.String())
	}
}

func TestRunRejectsUnsupportedAuthMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-auth-mode", "jwt"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("unsupported auth mode exited successfully; stdout=%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "unsupported mysql auth mode") {
		t.Fatalf("stderr = %q, want auth mode error", stderr.String())
	}
}

func TestRunStatusPrintsNativeGRPCWhenConfigured(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-status", "-native-grpc-bind", "0.0.0.0", "-native-grpc-port", "4100"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "native_grpc=0.0.0.0:4100") {
		t.Fatalf("stdout missing native gRPC address:\n%s", stdout.String())
	}
}

func TestRunRejectsUnsupportedMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive", "-mode", "distributed"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("unsupported mode exited successfully")
	}
	if !strings.Contains(stderr.String(), "unsupported mode") {
		t.Fatalf("stderr = %q, want unsupported mode", stderr.String())
	}
}

func TestRunPrintsBSIPrimaryKeyAuthorityManifest(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeCommandTestSchema(t, filepath.Join(dataDir, "config"), "sample")

	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive",
		"-data-dir", dataDir,
		"-print-bsi-pk-authority-manifest",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{
		"version: 1",
		"source: quantastream-cli",
		"table: sample",
		"primary_key: id",
		"encoding_version:",
		"fields:",
		"clean: true",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "listening=") {
		t.Fatalf("manifest print should not start listener:\n%s", output)
	}
}

func TestRunWritesBSIPrimaryKeyAuthorityManifest(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeCommandTestSchema(t, filepath.Join(dataDir, "config"), "sample")

	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive",
		"-data-dir", dataDir,
		"-write-bsi-pk-authority-manifest",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "bsi_pk_authority_manifest_written=") {
		t.Fatalf("stdout missing manifest write path:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_entries=1") {
		t.Fatalf("stdout missing manifest entry count:\n%s", output)
	}
	data, err := os.ReadFile(core.BSIPrimaryKeyAuthorityManifestPath(dataDir))
	if err != nil {
		t.Fatalf("read written manifest: %v", err)
	}
	for _, want := range []string{
		"source: quantastream-cli",
		"table: sample",
		"primary_key: id",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("written manifest missing %q:\n%s", want, string(data))
		}
	}
}

func TestRunRejectsConflictingBSIPrimaryKeyAuthorityManifestActions(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-auth-mode", "permissive",
		"-print-bsi-pk-authority-manifest",
		"-write-bsi-pk-authority-manifest",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit code = 0, want failure")
	}
	if !strings.Contains(stderr.String(), "cannot both be set") {
		t.Fatalf("stderr = %q, want conflict message", stderr.String())
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
			"-auth-mode", "permissive",
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

func writeCommandTestAccessPolicyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	if err := os.WriteFile(path, []byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: orders
`), 0o600); err != nil {
		t.Fatalf("write access policy file: %v", err)
	}
	return path
}
