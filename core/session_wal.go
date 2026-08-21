package core

import (
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
