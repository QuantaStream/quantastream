package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalWALAppendReadReopenAndReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	firstAt := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	first, err := wal.Append(context.Background(), LocalWALRecord{
		Timestamp:   firstAt,
		Transaction: "txn-1",
		Statement:   "stmt-1",
		OperationID: "op-1",
		Kind:        "put_row",
		Table:       "customer",
		Payload:     json.RawMessage(`{"c_custkey":1}`),
	})
	if err != nil {
		t.Fatalf("Append first returned error: %v", err)
	}
	second, err := wal.Append(context.Background(), LocalWALRecord{
		Transaction: "txn-1",
		Statement:   "stmt-2",
		OperationID: "op-2",
		Kind:        "commit",
	})
	if err != nil {
		t.Fatalf("Append second returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if first.LSN != 1 || second.LSN != 2 {
		t.Fatalf("LSNs = %d/%d, want 1/2", first.LSN, second.LSN)
	}
	if !first.Timestamp.Equal(firstAt) {
		t.Fatalf("timestamp = %s, want %s", first.Timestamp, firstAt)
	}
	if string(second.Payload) != "{}" {
		t.Fatalf("default payload = %s, want {}", string(second.Payload))
	}

	records, err := ReadLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ReadLocalWAL returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records length = %d, want 2", len(records))
	}
	if records[0].OperationID != "op-1" || records[1].OperationID != "op-2" {
		t.Fatalf("operation ids = %s/%s", records[0].OperationID, records[1].OperationID)
	}

	wal, err = OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("reopen WAL returned error: %v", err)
	}
	third, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "op-3",
		Kind:        "put_row",
		Table:       "orders",
		Payload:     json.RawMessage(`{"o_orderkey":10}`),
	})
	if err != nil {
		t.Fatalf("Append third returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close reopened WAL returned error: %v", err)
	}
	if third.LSN != 3 {
		t.Fatalf("third LSN = %d, want 3", third.LSN)
	}

	summary, err := ValidateLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ValidateLocalWAL returned error: %v", err)
	}
	if summary.RecordCount != 3 || summary.LastLSN != 3 || summary.ByteCount == 0 {
		t.Fatalf("summary = %+v, want three records ending at LSN 3", summary)
	}

	var replayed []string
	if err := ReplayLocalWAL(walPath, func(record LocalWALRecord) error {
		replayed = append(replayed, record.OperationID)
		return nil
	}); err != nil {
		t.Fatalf("ReplayLocalWAL returned error: %v", err)
	}
	if strings.Join(replayed, ",") != "op-1,op-2,op-3" {
		t.Fatalf("replayed operations = %v", replayed)
	}
}

func TestLocalWALCommitBoundaryAdvancesCheckpointAfterCommit(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	commitSawRecord := false
	record, checkpoint, err := wal.CommitBoundary(context.Background(), LocalWALRecord{
		OperationID: "commit-1",
		Kind:        "commit",
	}, func() error {
		records, err := ReadLocalWAL(walPath)
		if err != nil {
			t.Fatalf("ReadLocalWAL during commit returned error: %v", err)
		}
		commitSawRecord = len(records) == 1 && records[0].OperationID == "commit-1"
		return nil
	})
	if err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !commitSawRecord {
		t.Fatalf("storage commit callback did not see durable commit record")
	}
	if record.LSN != 1 || checkpoint.LastCommittedLSN != 1 || checkpoint.OperationID != "commit-1" {
		t.Fatalf("record/checkpoint = %+v/%+v, want LSN 1 commit-1", record, checkpoint)
	}
	loaded, found, err := LoadLocalWALCheckpoint(walPath + ".checkpoint.json")
	if err != nil {
		t.Fatalf("LoadLocalWALCheckpoint returned error: %v", err)
	}
	if !found || loaded.LastCommittedLSN != 1 || loaded.OperationID != "commit-1" {
		t.Fatalf("loaded checkpoint found=%t checkpoint=%+v", found, loaded)
	}
	summary, err := ValidateLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ValidateLocalWAL returned error: %v", err)
	}
	if !summary.CheckpointExists || summary.CheckpointLSN != 1 {
		t.Fatalf("summary = %+v, want checkpoint LSN 1", summary)
	}
}

func TestLocalWALCommitBoundaryDoesNotCheckpointFailedCommit(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	commitErr := os.ErrPermission
	_, _, err = wal.CommitBoundary(context.Background(), LocalWALRecord{
		OperationID: "commit-failed",
		Kind:        "commit",
	}, func() error {
		return commitErr
	})
	if err == nil || !strings.Contains(err.Error(), commitErr.Error()) {
		t.Fatalf("CommitBoundary error = %v, want storage commit error", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	records, err := ReadLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ReadLocalWAL returned error: %v", err)
	}
	if len(records) != 1 || records[0].OperationID != "commit-failed" {
		t.Fatalf("records = %+v, want failed commit record retained", records)
	}
	_, found, err := LoadLocalWALCheckpoint(walPath + ".checkpoint.json")
	if err != nil {
		t.Fatalf("LoadLocalWALCheckpoint returned error: %v", err)
	}
	if found {
		t.Fatalf("checkpoint should not exist after failed storage commit")
	}
}

func TestPlanLocalWALRecoverySeparatesCheckpointReplayAndPendingTail(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "put-1",
		Kind:        LocalWALRecordKindPutRow,
		Table:       "customer",
		Payload:     json.RawMessage(`{"c_custkey":1}`),
	}); err != nil {
		t.Fatalf("Append put-1 returned error: %v", err)
	}
	if _, _, err := wal.CommitBoundary(context.Background(), LocalWALRecord{
		OperationID: "commit-2",
		Kind:        LocalWALRecordKindCommit,
	}, func() error { return nil }); err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "put-3",
		Kind:        LocalWALRecordKindPutRow,
		Table:       "orders",
		Payload:     json.RawMessage(`{"o_orderkey":10}`),
	}); err != nil {
		t.Fatalf("Append put-3 returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "commit-4",
		Kind:        LocalWALRecordKindCommit,
	}); err != nil {
		t.Fatalf("Append commit-4 returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "put-5",
		Kind:        LocalWALRecordKindPutRow,
		Table:       "lineitem",
		Payload:     json.RawMessage(`{"l_orderkey":10}`),
	}); err != nil {
		t.Fatalf("Append put-5 returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	plan, err := PlanLocalWALRecovery(walPath)
	if err != nil {
		t.Fatalf("PlanLocalWALRecovery returned error: %v", err)
	}
	if !plan.CheckpointExists || plan.CheckpointLSN != 2 || plan.LastLSN != 5 {
		t.Fatalf("checkpoint/tail = exists:%t checkpoint:%d last:%d, want true/2/5", plan.CheckpointExists, plan.CheckpointLSN, plan.LastLSN)
	}
	if plan.CheckpointedRecordCount != 2 || plan.RecordCount != 5 {
		t.Fatalf("record counts = checkpointed:%d total:%d, want 2/5", plan.CheckpointedRecordCount, plan.RecordCount)
	}
	if got := operationIDs(plan.ReplayRecords); got != "put-3,commit-4" {
		t.Fatalf("replay operations = %s, want put-3,commit-4", got)
	}
	if got := operationIDs(plan.PendingRecords); got != "put-5" {
		t.Fatalf("pending operations = %s, want put-5", got)
	}
	if !plan.NeedsReplay() || !plan.HasPendingTail() || plan.ReplayRecordCount() != 2 || plan.PendingRecordCount() != 1 || plan.ReplayCommitBoundaryCount != 1 {
		t.Fatalf("plan summary = %+v", plan)
	}
	var replayed []string
	if err := ReplayLocalWALRecoveryPlan(plan, func(record LocalWALRecord) error {
		replayed = append(replayed, record.OperationID)
		return nil
	}); err != nil {
		t.Fatalf("ReplayLocalWALRecoveryPlan returned error: %v", err)
	}
	if strings.Join(replayed, ",") != "put-3,commit-4" {
		t.Fatalf("replayed operations = %v, want put-3,commit-4", replayed)
	}
}

func TestPlanLocalWALRecoveryTreatsTailWithoutCommitAsPending(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "put-1",
		Kind:        LocalWALRecordKindPutRow,
		Table:       "customer",
		Payload:     json.RawMessage(`{"c_custkey":1}`),
	}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	plan, err := PlanLocalWALRecovery(walPath)
	if err != nil {
		t.Fatalf("PlanLocalWALRecovery returned error: %v", err)
	}
	if plan.NeedsReplay() || !plan.HasPendingTail() {
		t.Fatalf("plan replay/pending = %t/%t, want false/true", plan.NeedsReplay(), plan.HasPendingTail())
	}
	if got := operationIDs(plan.PendingRecords); got != "put-1" {
		t.Fatalf("pending operations = %s, want put-1", got)
	}
}

func TestPlanLocalWALRecoveryRejectsCheckpointAheadOfTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "storage.wal")
	if err := WriteLocalWALCheckpoint(walPath+".checkpoint.json", LocalWALCheckpoint{
		WALPath:          walPath,
		LastCommittedLSN: 9,
		OperationID:      "commit-9",
	}); err != nil {
		t.Fatalf("WriteLocalWALCheckpoint returned error: %v", err)
	}
	_, err := PlanLocalWALRecovery(walPath)
	if err == nil || !strings.Contains(err.Error(), "ahead of WAL tail") {
		t.Fatalf("PlanLocalWALRecovery error = %v, want checkpoint ahead rejection", err)
	}
}

func TestLocalWALCheckpointDetectsChecksumMismatch(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "storage.wal.checkpoint.json")
	if err := WriteLocalWALCheckpoint(checkpointPath, LocalWALCheckpoint{
		WALPath:          "/tmp/storage.wal",
		LastCommittedLSN: 10,
		OperationID:      "commit-10",
	}); err != nil {
		t.Fatalf("WriteLocalWALCheckpoint returned error: %v", err)
	}
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	data = []byte(strings.Replace(string(data), `"last_committed_lsn": 10`, `"last_committed_lsn": 11`, 1))
	if err := os.WriteFile(checkpointPath, data, 0644); err != nil {
		t.Fatalf("corrupt checkpoint: %v", err)
	}
	_, _, err = LoadLocalWALCheckpoint(checkpointPath)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("LoadLocalWALCheckpoint error = %v, want checksum mismatch", err)
	}
}

func TestLocalWALRejectsInvalidRecord(t *testing.T) {
	wal, err := OpenLocalWALWithOptions(filepath.Join(t.TempDir(), "storage.wal"), LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	defer wal.Close()

	_, err = wal.Append(context.Background(), LocalWALRecord{
		Kind:    "put_row",
		Payload: json.RawMessage(`{}`),
	})
	if err == nil || !strings.Contains(err.Error(), "operation_id") {
		t.Fatalf("Append error = %v, want operation_id rejection", err)
	}

	_, err = wal.Append(context.Background(), LocalWALRecord{
		OperationID: "op-bad-json",
		Kind:        "put_row",
		Payload:     json.RawMessage(`{bad`),
	})
	if err == nil || !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("Append error = %v, want JSON rejection", err)
	}
}

func TestLocalWALDetectsChecksumMismatch(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "storage.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, err := wal.Append(context.Background(), LocalWALRecord{
		OperationID: "op-1",
		Kind:        "put_row",
		Payload:     json.RawMessage(`{"value":1}`),
	}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	data = []byte(strings.Replace(string(data), `"value":1`, `"value":2`, 1))
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("corrupt WAL: %v", err)
	}

	_, err = ReadLocalWAL(walPath)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("ReadLocalWAL error = %v, want checksum mismatch", err)
	}
}

func TestLocalWALMissingFileIsEmpty(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "missing.wal")
	records, err := ReadLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ReadLocalWAL missing returned error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("records length = %d, want 0", len(records))
	}
	summary, err := ValidateLocalWAL(walPath)
	if err != nil {
		t.Fatalf("ValidateLocalWAL missing returned error: %v", err)
	}
	if summary != (LocalWALSummary{}) {
		t.Fatalf("summary = %+v, want zero value", summary)
	}
}

func operationIDs(records []LocalWALRecord) string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.OperationID)
	}
	return strings.Join(ids, ",")
}
