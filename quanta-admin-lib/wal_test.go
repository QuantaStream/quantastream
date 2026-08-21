package admin

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestWALCommandsValidateAndPlan(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := core.OpenLocalWALWithOptions(walPath, core.LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "put-1",
		Kind:        core.LocalWALRecordKindPutRow,
		Table:       "customer",
		Payload:     json.RawMessage(`{"c_custkey":1}`),
	}); err != nil {
		t.Fatalf("Append put-1 returned error: %v", err)
	}
	if _, _, err := wal.CommitBoundary(context.Background(), core.LocalWALRecord{
		OperationID: "commit-2",
		Kind:        core.LocalWALRecordKindCommit,
	}, func() error { return nil }); err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "put-3",
		Kind:        core.LocalWALRecordKindPutRow,
		Table:       "orders",
		Payload:     json.RawMessage(`{"o_orderkey":10}`),
	}); err != nil {
		t.Fatalf("Append put-3 returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "commit-4",
		Kind:        core.LocalWALRecordKindCommit,
	}); err != nil {
		t.Fatalf("Append commit-4 returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "put-5",
		Kind:        core.LocalWALRecordKindPutRow,
		Table:       "lineitem",
		Payload:     json.RawMessage(`{"l_orderkey":10}`),
	}); err != nil {
		t.Fatalf("Append put-5 returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}

	validateOutput, err := captureAdminWALStdout(t, func() error {
		return (&WALValidateCmd{Path: walPath}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("WALValidateCmd.Run returned error: %v", err)
	}
	assertAdminWALOutputContains(t, validateOutput,
		"wal_valid=",
		"wal_records=5",
		"wal_last_lsn=5",
		"wal_checkpoint_exists=true",
		"wal_checkpoint_lsn=2",
	)

	planOutput, err := captureAdminWALStdout(t, func() error {
		return (&WALPlanCmd{Path: walPath}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("WALPlanCmd.Run returned error: %v", err)
	}
	assertAdminWALOutputContains(t, planOutput,
		"wal_plan=",
		"wal_records=5",
		"wal_checkpointed_records=2",
		"wal_replay_records=2",
		"wal_pending_records=1",
		"wal_replay_commit_boundaries=1",
		"wal_needs_replay=true",
		"wal_has_pending_tail=true",
	)
}

func captureAdminWALStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = orig }()
	runErr := fn()
	closeErr := writer.Close()
	os.Stdout = orig
	data, readErr := io.ReadAll(reader)
	if err := reader.Close(); err != nil && readErr == nil {
		readErr = err
	}
	if readErr != nil {
		t.Fatalf("read captured stdout: %v", readErr)
	}
	if closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	return string(data), runErr
}

func assertAdminWALOutputContains(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("WAL command output missing %q:\n%s", want, output)
		}
	}
}
