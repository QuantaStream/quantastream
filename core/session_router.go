package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stvp/rendezvous"
	"golang.org/x/sync/errgroup"
)

// IngestRecord is a transport-neutral row mutation routed through a single
// session owner.
type IngestRecord struct {
	TableName      string
	Data           map[string]interface{}
	ShardKey       string
	BuildShardKey  string
	EventID        string
	Source         string
	EventTime      time.Time
	SourceOffset   string
	PayloadHash    uint64
	DedupTTL       time.Duration
	PrimaryKeyMode PrimaryKeyMode
}

// SessionRouterConfig configures deterministic fanout across session workers.
type SessionRouterConfig struct {
	TableCache                *TableCacheStruct
	BasePath                  string
	Conn                      *shared.Conn
	ShardCount                int
	ChannelSize               int
	FlushInterval             time.Duration
	PrimaryKeyMode            PrimaryKeyMode
	PrimaryKeyResolverFactory SessionPrimaryKeyResolverFactory
	CommitOnClose             bool
	OnSessionOpen             func()
	OnSessionClose            func()
	OnPutRowResult            func(shardID string, record IngestRecord, result PutRowResult)
	OnFlushProfile            func(shardID string, tableName string, profile shared.BatchBufferFlushProfile)
	OnDrainProfile            func(profile RouterDrainWorkerProfile)
	OnProcessed               func()
	OnError                   func(error)
}

// SessionPrimaryKeyResolverFactory creates an optional resolver for
// router-owned sessions.
type SessionPrimaryKeyResolverFactory func(*Session) PrimaryKeyResolver

// SessionRouter owns non-threadsafe Session objects behind worker channels.
type SessionRouter struct {
	cfg           SessionRouterConfig
	hashTable     *rendezvous.Table
	shardChannels map[string]chan sessionRouterMessage
	sessionCache  sync.Map
	eg            errgroup.Group
	closeOnce     sync.Once
	commitOnce    sync.Once
	commitErr     error
}

type sessionRouterMessage struct {
	record  IngestRecord
	command *sessionRouterCommand
}

type sessionRouterCommandKind string

const (
	sessionRouterCommandFlush sessionRouterCommandKind = "flush"
)

type sessionRouterCommand struct {
	kind  sessionRouterCommandKind
	reply chan sessionRouterCommandResult
}

type sessionRouterCommandResult struct {
	shardID      string
	sessionCount int
	flushCount   int
	err          error
}

// RouterFlushResult summarizes an explicit router flush operation.
type RouterFlushResult struct {
	ShardCount   int           `json:"shard_count"`
	SessionCount int           `json:"session_count"`
	FlushCount   int           `json:"flush_count"`
	ErrorCount   int           `json:"error_count"`
	Elapsed      time.Duration `json:"elapsed_nanos"`
}

// RouterCommitResult summarizes an explicit backend commit operation.
type RouterCommitResult struct {
	CommitCount int           `json:"commit_count"`
	Elapsed     time.Duration `json:"elapsed_nanos"`
}

// SessionRouterStats is a point-in-time view of router queue/session pressure.
type SessionRouterStats struct {
	ShardCount       int                           `json:"shard_count"`
	ChannelSize      int                           `json:"channel_size"`
	PrimaryKeyMode   PrimaryKeyMode                `json:"primary_key_mode"`
	TotalQueued      int                           `json:"total_queued"`
	TotalCapacity    int                           `json:"total_capacity"`
	OpenSessionCount int                           `json:"open_session_count"`
	Shards           map[string]SessionRouterShard `json:"shards,omitempty"`
}

// SessionRouterShard is the queue state for one router worker.
type SessionRouterShard struct {
	Queued   int `json:"queued"`
	Capacity int `json:"capacity"`
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
	cfg.PrimaryKeyMode = cfg.PrimaryKeyMode.normalize()

	router := &SessionRouter{
		cfg:           cfg,
		shardChannels: make(map[string]chan sessionRouterMessage),
	}
	shardIDs := make([]string, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		shardID := fmt.Sprintf("shard%v", i)
		shardIDs[i] = shardID
		router.shardChannels[shardID] = make(chan sessionRouterMessage, cfg.ChannelSize)
	}
	router.hashTable = rendezvous.New(shardIDs)
	for _, shardID := range shardIDs {
		router.startWorker(shardID, router.shardChannels[shardID])
	}
	return router, nil
}

// Snapshot returns queue and session ownership state for instrumentation.
func (r *SessionRouter) Snapshot() SessionRouterStats {
	if r == nil {
		return SessionRouterStats{}
	}
	stats := SessionRouterStats{
		ShardCount:     len(r.shardChannels),
		ChannelSize:    r.cfg.ChannelSize,
		PrimaryKeyMode: r.cfg.PrimaryKeyMode,
		Shards:         map[string]SessionRouterShard{},
	}
	for shardID, ch := range r.shardChannels {
		shard := SessionRouterShard{
			Queued:   len(ch),
			Capacity: cap(ch),
		}
		stats.Shards[shardID] = shard
		stats.TotalQueued += shard.Queued
		stats.TotalCapacity += shard.Capacity
	}
	r.sessionCache.Range(func(_, _ interface{}) bool {
		stats.OpenSessionCount++
		return true
	})
	return stats
}

// Enqueue routes a record to the session worker selected by its shard key.
func (r *SessionRouter) Enqueue(record IngestRecord) error {
	if record.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	routeKey := record.RouteShardKey()
	if routeKey == "" {
		return fmt.Errorf("shard key or build shard key is required")
	}
	shard := r.hashTable.GetN(1, routeKey)
	ch, ok := r.shardChannels[shard[0]]
	if !ok {
		return fmt.Errorf("cannot locate channel for route shard key %v", routeKey)
	}
	ch <- sessionRouterMessage{record: record}
	return nil
}

// Flush asks every worker to flush its owned sessions after processing any
// records already queued ahead of the flush marker.
func (r *SessionRouter) Flush(ctx context.Context) (RouterFlushResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	result := RouterFlushResult{ShardCount: len(r.shardChannels)}
	replies := make([]chan sessionRouterCommandResult, 0, len(r.shardChannels))
	for _, ch := range r.shardChannels {
		reply := make(chan sessionRouterCommandResult, 1)
		message := sessionRouterMessage{command: &sessionRouterCommand{
			kind:  sessionRouterCommandFlush,
			reply: reply,
		}}
		select {
		case ch <- message:
			replies = append(replies, reply)
		case <-ctx.Done():
			result.Elapsed = time.Since(startedAt)
			return result, ctx.Err()
		}
	}
	var firstErr error
	for _, reply := range replies {
		select {
		case shard := <-reply:
			result.SessionCount += shard.sessionCount
			result.FlushCount += shard.flushCount
			if shard.err != nil {
				result.ErrorCount++
				if firstErr == nil {
					firstErr = shard.err
				}
			}
		case <-ctx.Done():
			result.Elapsed = time.Since(startedAt)
			return result, ctx.Err()
		}
	}
	result.Elapsed = time.Since(startedAt)
	return result, firstErr
}

// Commit asks the backend nodes to persist their current storage state.
func (r *SessionRouter) Commit(ctx context.Context) (RouterCommitResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	select {
	case <-ctx.Done():
		return RouterCommitResult{Elapsed: time.Since(startedAt)}, ctx.Err()
	default:
	}
	err := shared.NewBitmapIndex(r.cfg.Conn).CommitWithContext(ctx)
	result := RouterCommitResult{Elapsed: time.Since(startedAt)}
	if err == nil {
		result.CommitCount = 1
	}
	return result, err
}

// Close drains workers and closes all owned sessions.
func (r *SessionRouter) Close() error {
	r.closeOnce.Do(func() {
		for _, ch := range r.shardChannels {
			close(ch)
		}
	})
	if err := r.eg.Wait(); err != nil {
		return err
	}
	if r.cfg.CommitOnClose {
		r.commitOnce.Do(func() {
			r.commitErr = shared.NewBitmapIndex(r.cfg.Conn).Commit()
		})
		return r.commitErr
	}
	return nil
}

func (r *SessionRouter) startWorker(shardID string, ch <-chan sessionRouterMessage) {
	r.eg.Go(func() error {
		var shardTableKeys sync.Map
		ticker := time.NewTicker(r.cfg.FlushInterval)
		defer ticker.Stop()
		for {
			select {
			case message, open := <-ch:
				if !open {
					drainStartedAt := time.Now()
					sessionCount, err := r.closeWorkerSessions(&shardTableKeys)
					profile := RouterDrainWorkerProfile{
						ShardID:      shardID,
						SessionCount: sessionCount,
						Elapsed:      time.Since(drainStartedAt),
					}
					if err != nil {
						profile.Error = err.Error()
					}
					r.publishDrainProfile(profile)
					return err
				}
				if message.command != nil {
					r.handleWorkerCommand(shardID, message.command, &shardTableKeys)
					continue
				}
				if err := r.putRecord(shardID, message.record, &shardTableKeys); err != nil {
					if r.cfg.OnError != nil {
						r.cfg.OnError(err)
					}
					return err
				}
			case <-ticker.C:
				if err := r.flushIdleSessions(shardID, &shardTableKeys); err != nil {
					if r.cfg.OnError != nil {
						r.cfg.OnError(err)
					}
					return err
				}
			}
		}
	})
}

func (r *SessionRouter) handleWorkerCommand(shardID string, command *sessionRouterCommand, shardTableKeys *sync.Map) {
	if command == nil || command.reply == nil {
		return
	}
	result := sessionRouterCommandResult{shardID: shardID}
	switch command.kind {
	case sessionRouterCommandFlush:
		result.sessionCount, result.flushCount, result.err = r.flushWorkerSessions(shardID, shardTableKeys)
	default:
		result.err = fmt.Errorf("unknown session router command %q", command.kind)
	}
	command.reply <- result
}

func (r *SessionRouter) putRecord(shardID string, record IngestRecord, shardTableKeys *sync.Map) error {
	shardTableKey := fmt.Sprintf("%v+%v", shardID, record.TableName)
	conn, ok := r.sessionCache.Load(shardTableKey)
	if !ok {
		session, err := OpenSession(r.cfg.TableCache, r.cfg.BasePath, record.TableName, true, r.cfg.Conn)
		if err != nil {
			return err
		}
		r.configureSessionResolver(session)
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
	if options.PrimaryKeyMode == "" {
		options.PrimaryKeyMode = r.cfg.PrimaryKeyMode
	} else {
		options.PrimaryKeyMode = options.PrimaryKeyMode.normalize()
	}
	record.PayloadHash = options.PayloadHash
	record.PrimaryKeyMode = options.PrimaryKeyMode
	session := conn.(*Session)
	result, err := session.PutRowWithOptions(record.TableName, record.Data, 0, false, false, options)
	if err != nil {
		return fmt.Errorf("ERROR in PutRow, shard %s - %v", shardID, err)
	}
	r.publishPutRowResult(shardID, record, result)
	if r.cfg.OnProcessed != nil {
		r.cfg.OnProcessed()
	}
	if err := r.flushActiveSession(shardID, record.TableName, session); err != nil {
		return err
	}
	return nil
}

func (r *SessionRouter) configureSessionResolver(session *Session) {
	if r.cfg.PrimaryKeyResolverFactory == nil || session == nil {
		return
	}
	session.SetPrimaryKeyResolver(r.cfg.PrimaryKeyResolverFactory(session))
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

func (r *SessionRouter) publishDrainProfile(profile RouterDrainWorkerProfile) {
	if r.cfg.OnDrainProfile == nil {
		return
	}
	r.cfg.OnDrainProfile(profile)
}

// PutRowOptions returns optional streaming metadata for the state-changing
// load boundary. Empty fields preserve the current PutRow behavior.
func (r IngestRecord) PutRowOptions() PutRowOptions {
	options, _ := r.PutRowOptionsWithPayloadHash()
	return options
}

// RouteShardKey returns the physical router key. BuildShardKey is optional and
// lets loaders colocate work by build/persistence shard without changing the
// logical record shard key used by upstream envelopes and dedup policy.
func (r IngestRecord) RouteShardKey() string {
	if key := strings.TrimSpace(r.BuildShardKey); key != "" {
		return key
	}
	return strings.TrimSpace(r.ShardKey)
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
		EventID:        r.EventID,
		Source:         r.Source,
		EventTime:      r.EventTime,
		SourceOffset:   r.SourceOffset,
		PayloadHash:    payloadHash,
		DedupTTL:       r.DedupTTL,
		PrimaryKeyMode: r.PrimaryKeyMode,
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

func (r *SessionRouter) flushWorkerSessions(shardID string, shardTableKeys *sync.Map) (int, int, error) {
	var firstErr error
	sessionCount := 0
	flushCount := 0
	shardTableKeys.Range(func(k, v interface{}) bool {
		sessionCount++
		session := v.(*Session)
		if session.IsFlushing() {
			return true
		}
		tableName := tableNameFromShardTableKey(fmt.Sprint(k))
		before := session.LastFlushProfile()
		if err := session.Flush(); err != nil {
			r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
			firstErr = err
			return false
		}
		after := session.LastFlushProfile()
		if batchBufferFlushProfileHasActivity(after) && (before.FinishedAt.IsZero() || after.FinishedAt.After(before.FinishedAt)) {
			flushCount++
		}
		r.publishNewFlushProfile(shardID, tableName, before, after)
		return true
	})
	return sessionCount, flushCount, firstErr
}

func (r *SessionRouter) flushActiveSession(shardID, tableName string, session *Session) error {
	if session == nil || session.BatchBuffer == nil || session.IsFlushing() {
		return nil
	}
	if session.BatchBuffer.IsEmpty() || time.Since(session.BatchBuffer.FlushedAt) <= r.cfg.FlushInterval {
		return nil
	}
	before := session.LastFlushProfile()
	if err := session.Flush(); err != nil {
		r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
		return err
	}
	r.publishNewFlushProfile(shardID, tableName, before, session.LastFlushProfile())
	return nil
}

func (r *SessionRouter) closeWorkerSessions(shardTableKeys *sync.Map) (int, error) {
	var firstErr error
	sessionCount := 0
	shardTableKeys.Range(func(k, v interface{}) bool {
		sessionCount++
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
	return sessionCount, firstErr
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
