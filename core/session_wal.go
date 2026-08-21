package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	LocalWALRecordKindPutRow    = "put_row"
	LocalWALRecordKindUpdateRow = "update_row"
	LocalWALRecordKindCommit    = "commit"
)

type SessionWriteAheadLog interface {
	Append(context.Context, LocalWALRecord) (LocalWALRecord, error)
}

type SessionWriteAheadLogCommitBoundary interface {
	CommitBoundary(context.Context, LocalWALRecord, func() error) (LocalWALRecord, LocalWALCheckpoint, error)
}

type putRowWALPayload struct {
	Table                 string         `json:"table"`
	ProvidedColumnID      uint64         `json:"provided_column_id,omitempty"`
	IgnoreSourcePath      bool           `json:"ignore_source_path,omitempty"`
	UseNerdCapitalization bool           `json:"use_nerd_capitalization,omitempty"`
	PrimaryKeyMode        PrimaryKeyMode `json:"primary_key_mode"`
	Options               PutRowOptions  `json:"options"`
	Row                   interface{}    `json:"row"`
}

type updateRowWALPayload struct {
	Table         string                 `json:"table"`
	ColumnID      uint64                 `json:"column_id"`
	TimePartition time.Time              `json:"time_partition"`
	Values        map[string]interface{} `json:"values"`
}

func (s *Session) SetWriteAheadLog(wal SessionWriteAheadLog) {
	if s == nil {
		return
	}
	s.writeAheadLog = wal
}

func ReplayLocalWALRecoveryPlanToSessionPool(ctx context.Context, pool *SessionPool, wal *LocalWAL, plan LocalWALRecoveryPlan, resolverFactory SessionPrimaryKeyResolverFactory) (LocalWALReplaySummary, error) {
	if pool == nil {
		return LocalWALReplaySummary{}, fmt.Errorf("WAL replay session pool is required")
	}
	summary := LocalWALReplaySummary{
		ReplayRecordCount:  len(plan.ReplayRecords),
		PendingRecordCount: len(plan.PendingRecords),
		CheckpointLSN:      plan.CheckpointLSN,
	}
	if len(plan.ReplayRecords) == 0 {
		return summary, nil
	}
	previousWAL := pool.WriteAheadLog()
	pool.SetWriteAheadLog(nil)
	defer pool.SetWriteAheadLog(previousWAL)

	sessions := make(map[string]*Session)
	releaseAll := func() {
		for table, session := range sessions {
			pool.Return(table, session)
			delete(sessions, table)
		}
	}
	defer releaseAll()
	borrow := func(table string) (*Session, error) {
		table = strings.TrimSpace(table)
		if table == "" {
			return nil, fmt.Errorf("WAL replay record table is required")
		}
		if session := sessions[table]; session != nil {
			return session, nil
		}
		session, err := pool.Borrow(table)
		if err != nil {
			return nil, fmt.Errorf("borrow WAL replay session table=%s: %w", table, err)
		}
		session.SetWriteAheadLog(nil)
		if resolverFactory != nil {
			session.SetPrimaryKeyResolver(resolverFactory(session))
		}
		sessions[table] = session
		return session, nil
	}
	commitBoundary := func(record LocalWALRecord) error {
		for table, session := range sessions {
			if err := session.Flush(); err != nil {
				return fmt.Errorf("flush WAL replay session table=%s at lsn=%d: %w", table, record.LSN, err)
			}
		}
		for table, session := range sessions {
			if err := session.Commit(); err != nil {
				return fmt.Errorf("commit WAL replay session table=%s at lsn=%d: %w", table, record.LSN, err)
			}
		}
		if wal != nil {
			checkpoint, err := wal.CheckpointCommittedRecord(ctx, record)
			if err != nil {
				return fmt.Errorf("advance WAL replay checkpoint lsn=%d: %w", record.LSN, err)
			}
			summary.CheckpointAdvanced = checkpoint.LastCommittedLSN > summary.CheckpointLSN
			summary.CheckpointLSN = checkpoint.LastCommittedLSN
		}
		summary.CommitBoundaryCount++
		summary.LastReplayedLSN = record.LSN
		releaseAll()
		return nil
	}

	for _, record := range plan.ReplayRecords {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		switch record.Kind {
		case LocalWALRecordKindPutRow:
			var payload putRowWALPayload
			if err := decodeLocalWALPayload(record.Payload, &payload); err != nil {
				return summary, fmt.Errorf("parse WAL put_row payload lsn=%d operation_id=%s: %w", record.LSN, record.OperationID, err)
			}
			payload.Row = normalizeLocalWALJSONValue(payload.Row)
			table := strings.TrimSpace(payload.Table)
			if table == "" {
				table = strings.TrimSpace(record.Table)
			}
			session, err := borrow(table)
			if err != nil {
				return summary, err
			}
			options := payload.Options
			if payload.PrimaryKeyMode != "" {
				options.PrimaryKeyMode = payload.PrimaryKeyMode.normalize()
			} else {
				options.PrimaryKeyMode = options.PrimaryKeyMode.normalize()
			}
			if _, err := session.PutRowWithOptions(table, payload.Row, payload.ProvidedColumnID, payload.IgnoreSourcePath, payload.UseNerdCapitalization, options); err != nil {
				return summary, fmt.Errorf("replay WAL put_row table=%s lsn=%d operation_id=%s: %w", table, record.LSN, record.OperationID, err)
			}
			summary.PutRowCount++
			summary.LastReplayedLSN = record.LSN
		case LocalWALRecordKindUpdateRow:
			var payload updateRowWALPayload
			if err := decodeLocalWALPayload(record.Payload, &payload); err != nil {
				return summary, fmt.Errorf("parse WAL update_row payload lsn=%d operation_id=%s: %w", record.LSN, record.OperationID, err)
			}
			for field, value := range payload.Values {
				payload.Values[field] = normalizeLocalWALJSONValue(value)
			}
			table := strings.TrimSpace(payload.Table)
			if table == "" {
				table = strings.TrimSpace(record.Table)
			}
			session, err := borrow(table)
			if err != nil {
				return summary, err
			}
			values := make(map[string]*qsbridge.ResultCell, len(payload.Values))
			for field, value := range payload.Values {
				values[field] = &qsbridge.ResultCell{Value: value}
			}
			if err := session.UpdateRow(table, payload.ColumnID, values, payload.TimePartition); err != nil {
				return summary, fmt.Errorf("replay WAL update_row table=%s lsn=%d operation_id=%s: %w", table, record.LSN, record.OperationID, err)
			}
			summary.UpdateRowCount++
			summary.LastReplayedLSN = record.LSN
		case LocalWALRecordKindCommit:
			if err := commitBoundary(record); err != nil {
				return summary, err
			}
		default:
			return summary, fmt.Errorf("unsupported WAL replay record kind %q at lsn=%d", record.Kind, record.LSN)
		}
	}
	return summary, nil
}

func decodeLocalWALPayload(data json.RawMessage, dst interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(dst)
}

func normalizeLocalWALJSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		text := v.String()
		if !strings.ContainsAny(text, ".eE") {
			if i, err := v.Int64(); err == nil {
				return i
			}
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return text
	case map[string]interface{}:
		for key, child := range v {
			v[key] = normalizeLocalWALJSONValue(child)
		}
		return v
	case []interface{}:
		for i, child := range v {
			v[i] = normalizeLocalWALJSONValue(child)
		}
		return v
	default:
		return value
	}
}

func (s *Session) appendPutRowWAL(req putRowRequest) error {
	if s == nil || s.writeAheadLog == nil {
		return nil
	}
	payload := putRowWALPayload{
		Table:                 req.tableName,
		ProvidedColumnID:      req.providedColID,
		IgnoreSourcePath:      req.ignoreSourcePath,
		UseNerdCapitalization: req.useNerdCapitalization,
		PrimaryKeyMode:        req.primaryKeyMode.normalize(),
		Options:               req.options,
		Row:                   req.row,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal put row WAL payload table=%s: %w", req.tableName, err)
	}
	operationID := strings.TrimSpace(req.options.EventID)
	if operationID == "" {
		operationID = s.nextWALOperationID(LocalWALRecordKindPutRow, req.tableName)
	} else {
		operationID = LocalWALRecordKindPutRow + ":" + operationID
	}
	_, err = s.writeAheadLog.Append(context.Background(), LocalWALRecord{
		OperationID: operationID,
		Kind:        LocalWALRecordKindPutRow,
		Table:       req.tableName,
		Payload:     json.RawMessage(payloadBytes),
	})
	if err != nil {
		return fmt.Errorf("append put row WAL record table=%s: %w", req.tableName, err)
	}
	return nil
}

func (s *Session) appendUpdateRowWAL(table string, columnID uint64, values map[string]*qsbridge.ResultCell, partition time.Time) error {
	if s == nil || s.writeAheadLog == nil {
		return nil
	}
	payloadValues := make(map[string]interface{}, len(values))
	for field, cell := range values {
		if cell == nil {
			payloadValues[field] = nil
			continue
		}
		payloadValues[field] = cell.Value
	}
	payloadBytes, err := json.Marshal(updateRowWALPayload{
		Table:         table,
		ColumnID:      columnID,
		TimePartition: partition.UTC(),
		Values:        payloadValues,
	})
	if err != nil {
		return fmt.Errorf("marshal update row WAL payload table=%s column_id=%d: %w", table, columnID, err)
	}
	_, err = s.writeAheadLog.Append(context.Background(), LocalWALRecord{
		OperationID: s.nextWALOperationID(LocalWALRecordKindUpdateRow, table),
		Kind:        LocalWALRecordKindUpdateRow,
		Table:       table,
		Payload:     json.RawMessage(payloadBytes),
	})
	if err != nil {
		return fmt.Errorf("append update row WAL record table=%s column_id=%d: %w", table, columnID, err)
	}
	return nil
}

func (s *Session) commitWithWAL(commitStorage func() error) error {
	if s == nil || s.writeAheadLog == nil {
		return commitStorage()
	}
	record := s.nextCommitWALRecord()
	if boundary, ok := s.writeAheadLog.(SessionWriteAheadLogCommitBoundary); ok {
		_, _, err := boundary.CommitBoundary(context.Background(), record, commitStorage)
		if err != nil {
			return fmt.Errorf("commit with WAL boundary: %w", err)
		}
		return nil
	}
	if _, err := s.writeAheadLog.Append(context.Background(), record); err != nil {
		return fmt.Errorf("append commit WAL record: %w", err)
	}
	return commitStorage()
}

func (s *Session) appendCommitWAL() (LocalWALRecord, error) {
	if s == nil || s.writeAheadLog == nil {
		return LocalWALRecord{}, nil
	}
	record, err := s.writeAheadLog.Append(context.Background(), s.nextCommitWALRecord())
	if err != nil {
		return LocalWALRecord{}, fmt.Errorf("append commit WAL record: %w", err)
	}
	return record, nil
}

func (s *Session) nextCommitWALRecord() LocalWALRecord {
	return LocalWALRecord{
		OperationID: s.nextWALOperationID(LocalWALRecordKindCommit, ""),
		Kind:        LocalWALRecordKindCommit,
		Payload:     json.RawMessage(`{}`),
	}
}

func (s *Session) nextWALOperationID(kind, table string) string {
	s.walOperationSeq++
	if strings.TrimSpace(table) == "" {
		return fmt.Sprintf("%s:%d", kind, s.walOperationSeq)
	}
	return fmt.Sprintf("%s:%s:%d", kind, table, s.walOperationSeq)
}
