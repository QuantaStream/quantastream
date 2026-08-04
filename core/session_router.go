package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stvp/rendezvous"
	"golang.org/x/sync/errgroup"
)

// IngestRecord is a transport-neutral row mutation routed through a single
// session owner.
type IngestRecord struct {
	TableName    string
	Data         map[string]interface{}
	ShardKey     string
	EventID      string
	Source       string
	EventTime    time.Time
	SourceOffset string
	PayloadHash  uint64
	DedupTTL     time.Duration
}

// SessionRouterConfig configures deterministic fanout across session workers.
type SessionRouterConfig struct {
	TableCache     *TableCacheStruct
	BasePath       string
	Conn           *shared.Conn
	ShardCount     int
	ChannelSize    int
	FlushInterval  time.Duration
	OnSessionOpen  func()
	OnSessionClose func()
	OnPutRowResult func(shardID string, record IngestRecord, result PutRowResult)
	OnFlushProfile func(shardID string, tableName string, profile shared.BatchBufferFlushProfile)
	OnProcessed    func()
	OnError        func(error)
}

// SessionRouter owns non-threadsafe Session objects behind worker channels.
type SessionRouter struct {
	cfg           SessionRouterConfig
	hashTable     *rendezvous.Table
	shardChannels map[string]chan IngestRecord
	sessionCache  sync.Map
	eg            errgroup.Group
	closeOnce     sync.Once
}

// NewSessionRouter creates session workers and deterministic shard routing.
func NewSessionRouter(cfg SessionRouterConfig) (*SessionRouter, error) {
	if cfg.TableCache == nil {
		return nil, fmt.Errorf("table cache is required")
	}
	if cfg.Conn == nil {
		return nil, fmt.Errorf("connection is required")
	}
	if cfg.ShardCount <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 100000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}

	router := &SessionRouter{
		cfg:           cfg,
		shardChannels: make(map[string]chan IngestRecord),
	}
	shardIDs := make([]string, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		shardID := fmt.Sprintf("shard%v", i)
		shardIDs[i] = shardID
		router.shardChannels[shardID] = make(chan IngestRecord, cfg.ChannelSize)
	}
	router.hashTable = rendezvous.New(shardIDs)
	for _, shardID := range shardIDs {
		router.startWorker(shardID, router.shardChannels[shardID])
	}
	return router, nil
}

// Enqueue routes a record to the session worker selected by its shard key.
func (r *SessionRouter) Enqueue(record IngestRecord) error {
	if record.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if record.ShardKey == "" {
		return fmt.Errorf("shard key is required")
	}
	shard := r.hashTable.GetN(1, record.ShardKey)
	ch, ok := r.shardChannels[shard[0]]
	if !ok {
		return fmt.Errorf("cannot locate channel for shard key %v", record.ShardKey)
	}
	ch <- record
	return nil
}

// Close drains workers and closes all owned sessions.
func (r *SessionRouter) Close() error {
	r.closeOnce.Do(func() {
		for _, ch := range r.shardChannels {
			close(ch)
		}
	})
	return r.eg.Wait()
}

func (r *SessionRouter) startWorker(shardID string, ch <-chan IngestRecord) {
	r.eg.Go(func() error {
		var shardTableKeys sync.Map
		for {
			select {
			case record, open := <-ch:
				if !open {
					return r.closeWorkerSessions(&shardTableKeys)
				}
				if err := r.putRecord(shardID, record, &shardTableKeys); err != nil {
					if r.cfg.OnError != nil {
						r.cfg.OnError(err)
					}
					return err
				}
			default:
				if err := r.flushIdleSessions(shardID, &shardTableKeys); err != nil {
					if r.cfg.OnError != nil {
						r.cfg.OnError(err)
					}
					return err
				}
				time.Sleep(r.cfg.FlushInterval)
			}
		}
	})
}

func (r *SessionRouter) putRecord(shardID string, record IngestRecord, shardTableKeys *sync.Map) error {
	shardTableKey := fmt.Sprintf("%v+%v", shardID, record.TableName)
	conn, ok := r.sessionCache.Load(shardTableKey)
	if !ok {
		session, err := OpenSession(r.cfg.TableCache, r.cfg.BasePath, record.TableName, true, r.cfg.Conn)
		if err != nil {
			return err
		}
		conn = session
		r.sessionCache.Store(shardTableKey, session)
		shardTableKeys.Store(shardTableKey, session)
		if r.cfg.OnSessionOpen != nil {
			r.cfg.OnSessionOpen()
		}
	}
	options, err := record.PutRowOptionsWithPayloadHash()
	if err != nil {
		return fmt.Errorf("ERROR in PutRow payload hash, shard %s - %v", shardID, err)
	}
	record.PayloadHash = options.PayloadHash
	result, err := conn.(*Session).PutRowWithOptions(record.TableName, record.Data, 0, false, false, options)
	if err != nil {
		return fmt.Errorf("ERROR in PutRow, shard %s - %v", shardID, err)
	}
	r.publishPutRowResult(shardID, record, result)
	if r.cfg.OnProcessed != nil {
		r.cfg.OnProcessed()
	}
	return nil
}

func (r *SessionRouter) publishPutRowResult(shardID string, record IngestRecord, result PutRowResult) {
	if r.cfg.OnPutRowResult != nil {
		r.cfg.OnPutRowResult(shardID, record, result)
	}
}

func (r *SessionRouter) publishFlushProfile(shardID string, tableName string, profile shared.BatchBufferFlushProfile) {
	if r.cfg.OnFlushProfile == nil || !batchBufferFlushProfileHasActivity(profile) {
		return
	}
	r.cfg.OnFlushProfile(shardID, tableName, profile)
}

// PutRowOptions returns optional streaming metadata for the state-changing
// load boundary. Empty fields preserve the current PutRow behavior.
func (r IngestRecord) PutRowOptions() PutRowOptions {
	options, _ := r.PutRowOptionsWithPayloadHash()
	return options
}

// PutRowOptionsWithPayloadHash returns optional streaming metadata and computes
// a deterministic payload hash when one was not provided.
func (r IngestRecord) PutRowOptionsWithPayloadHash() (PutRowOptions, error) {
	payloadHash := r.PayloadHash
	if payloadHash == 0 && r.Data != nil {
		var err error
		payloadHash, err = HashIngestPayload(r.Data)
		if err != nil {
			return PutRowOptions{}, err
		}
	}
	return PutRowOptions{
		EventID:      r.EventID,
		Source:       r.Source,
		EventTime:    r.EventTime,
		SourceOffset: r.SourceOffset,
		PayloadHash:  payloadHash,
		DedupTTL:     r.DedupTTL,
	}, nil
}

func (r *SessionRouter) flushIdleSessions(shardID string, shardTableKeys *sync.Map) error {
	var firstErr error
	shardTableKeys.Range(func(k, v interface{}) bool {
		session := v.(*Session)
		if session.IsFlushing() {
			return true
		}
		tableName := tableNameFromShardTableKey(fmt.Sprint(k))
		if time.Since(session.BatchBuffer.FlushedAt) > 2*r.cfg.FlushInterval {
			before := session.LastFlushProfile()
			if err := session.CloseSession(); err != nil {
				r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
				firstErr = err
				return false
			}
			r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
			shardTableKeys.Delete(k)
			r.sessionCache.Delete(k)
			if r.cfg.OnSessionClose != nil {
				r.cfg.OnSessionClose()
			}
		} else if time.Since(session.BatchBuffer.ModifiedAt) > r.cfg.FlushInterval {
			before := session.LastFlushProfile()
			if err := session.Flush(); err != nil {
				r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
				firstErr = err
				return false
			}
			r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
		}
		return true
	})
	return firstErr
}

func (r *SessionRouter) closeWorkerSessions(shardTableKeys *sync.Map) error {
	var firstErr error
	shardTableKeys.Range(func(k, v interface{}) bool {
		session := v.(*Session)
		before := session.LastFlushProfile()
		if err := session.CloseSession(); err != nil {
			key := fmt.Sprint(k)
			r.publishNewFlushProfile(tableShardFromShardTableKey(key), tableNameFromShardTableKey(key), before,
				session.LastFlushProfile())
			firstErr = err
			return false
		}
		key := fmt.Sprint(k)
		r.publishNewFlushProfile(tableShardFromShardTableKey(key), tableNameFromShardTableKey(key), before,
			session.LastFlushProfile())
		shardTableKeys.Delete(k)
		r.sessionCache.Delete(k)
		if r.cfg.OnSessionClose != nil {
			r.cfg.OnSessionClose()
		}
		return true
	})
	return firstErr
}

func (r *SessionRouter) publishNewFlushProfile(shardID string, tableName string, before, after shared.BatchBufferFlushProfile) {
	if !batchBufferFlushProfileHasActivity(after) {
		return
	}
	if !before.FinishedAt.IsZero() && !after.FinishedAt.After(before.FinishedAt) {
		return
	}
	r.publishFlushProfile(shardID, tableName, after)
}

func batchBufferFlushProfileHasActivity(profile shared.BatchBufferFlushProfile) bool {
	return profile.PartitionStringEntryCount > 0 ||
		profile.BitmapSetEntryCount > 0 ||
		profile.BitmapClearEntryCount > 0 ||
		profile.BSIValueEntryCount > 0 ||
		profile.BSIClearValueEntryCount > 0 ||
		profile.TotalElapsed > 0 ||
		profile.Error != ""
}

func tableShardFromShardTableKey(key string) string {
	shardID, _ := splitShardTableKey(key)
	return shardID
}

func tableNameFromShardTableKey(key string) string {
	_, tableName := splitShardTableKey(key)
	return tableName
}

func splitShardTableKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '+' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}
