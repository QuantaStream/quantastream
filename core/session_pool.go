package core

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/QuantaStream/quantastream/shared"
	u "github.com/araddon/gou"
)

// ErrPoolDrained - Special case error indicates that the pool is exhausted
var ErrPoolDrained = errors.New("session pool drained")

// SessionPool - Session pool encapsulates a Quanta session.
type SessionPool struct {
	AppHost          *shared.Conn
	baseDir          string
	sessPoolMap      map[string]*sessionPoolEntry
	tableGenerations map[string]uint64
	sessPoolLock     sync.RWMutex
	semaphores       chan struct{}
	poolSize         int
	maxUsed          int32

	TableCache *TableCacheStruct

	closed bool // when semaphores is closed, race warning
}

// SessionPool - Pool of Quanta connections
type sessionPoolEntry struct {
	pool       chan *Session
	generation uint64
}

// NewSessionPool - Construct a session pool to constrain resource consumption.
func NewSessionPool(tableCache *TableCacheStruct, appHost *shared.Conn, baseDir string, poolSize int) *SessionPool {

	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}

	p := &SessionPool{AppHost: appHost, baseDir: baseDir,
		sessPoolMap: make(map[string]*sessionPoolEntry), tableGenerations: make(map[string]uint64),
		semaphores: make(chan struct{}, poolSize), poolSize: poolSize}
	p.TableCache = tableCache
	for i := 0; i < poolSize; i++ {
		p.semaphores <- struct{}{}
	}
	return p
}

func (m *SessionPool) newSessionPoolEntry(tableName string) *sessionPoolEntry {
	return &sessionPoolEntry{pool: make(chan *Session, m.poolSize), generation: m.tableGenerationLocked(tableName)}
}

func (m *SessionPool) getPoolByTableName(tableName string) *sessionPoolEntry {

	var cp *sessionPoolEntry
	var found bool
	if cp, found = m.sessPoolMap[tableName]; !found {
		cp = m.newSessionPoolEntry(tableName)
		m.sessPoolMap[tableName] = cp
	}
	return cp
}

// Borrow - Get a pooled connection.
func (m *SessionPool) Borrow(tableName string) (*Session, error) {

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	cp := m.getPoolByTableName(tableName)
	select {
	case <-m.semaphores:
		max := atomic.LoadInt32(&m.maxUsed)
		used := int32(m.poolSize - len(m.semaphores))
		if used > max {
			atomic.StoreInt32(&m.maxUsed, used)
		}
		select {
		case r := <-cp.pool:
			r.poolGeneration = cp.generation
			return r, nil
		default:
			conn, err := m.NewSession(tableName)
			if err != nil {
				return nil, fmt.Errorf("borrowSession %v", err)
			}
			conn.poolGeneration = cp.generation
			return conn, nil
		}
	default:
		return nil, ErrPoolDrained
	}
}

// Return - Return a connection to the pool.
func (m *SessionPool) Return(tableName string, conn *Session) {
	_, _ = m.ReturnWithProfile(tableName, conn)
}

// ReturnWithProfile returns a connection to the pool and reports the flush
// profile observed while releasing the session.
func (m *SessionPool) ReturnWithProfile(tableName string, conn *Session) (shared.BatchBufferFlushProfile, error) {
	if conn == nil {
		return shared.BatchBufferFlushProfile{}, nil
	}
	batchEmpty := conn.BatchBuffer == nil || conn.BatchBuffer.IsEmpty()

	m.sessPoolLock.Lock()
	if m.closed {
		m.sessPoolLock.Unlock()
		err := conn.CloseSession()
		m.returnSemaphore()
		return sessionReturnFlushProfile(conn, batchEmpty), err
	}
	stale := m.sessionGenerationStaleLocked(tableName, conn.poolGeneration)
	m.sessPoolLock.Unlock()

	if stale {
		err := conn.CloseSession()
		m.returnSemaphore()
		return sessionReturnFlushProfile(conn, batchEmpty), err
	}

	if err := conn.Flush(); err != nil {
		profile := sessionReturnFlushProfile(conn, batchEmpty)
		conn.CloseSession()
		m.returnSemaphore()
		return profile, err
	}
	profile := sessionReturnFlushProfile(conn, batchEmpty)

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	if m.closed {
		conn.CloseSession()
		return profile, nil
	}
	if m.sessionGenerationStaleLocked(tableName, conn.poolGeneration) {
		conn.CloseSession()
		m.returnSemaphoreLocked()
		return profile, nil
	}
	cp := m.getPoolByTableName(tableName)
	conn.poolGeneration = cp.generation
	select {
	case m.semaphores <- struct{}{}:
		select {
		case cp.pool <- conn:
		default:
			conn.CloseSession()
		}
	default: //Don't block
	}
	return profile, nil
}

func sessionReturnFlushProfile(conn *Session, batchEmpty bool) shared.BatchBufferFlushProfile {
	if conn == nil || batchEmpty {
		return shared.BatchBufferFlushProfile{}
	}
	return conn.LastFlushProfile()
}

// InvalidateTable closes pooled sessions and cached metadata for a table after a schema change.
func (m *SessionPool) InvalidateTable(tableName string) {
	canonical := sessionPoolTableKey(tableName)
	if canonical == "" {
		return
	}
	var stale []*Session

	m.sessPoolLock.Lock()
	if m.closed {
		m.sessPoolLock.Unlock()
		return
	}
	m.tableGenerations[canonical] = m.tableGenerations[canonical] + 1
	for key, entry := range m.sessPoolMap {
		if sessionPoolTableKey(key) != canonical {
			continue
		}
		delete(m.sessPoolMap, key)
		for {
			select {
			case session := <-entry.pool:
				if session != nil {
					stale = append(stale, session)
				}
			default:
				goto drained
			}
		}
	drained:
	}
	m.sessPoolLock.Unlock()

	for _, session := range stale {
		session.CloseSession()
	}
	m.invalidateTableCache(tableName)
}

func (m *SessionPool) invalidateTableCache(tableName string) {
	if m.TableCache == nil {
		return
	}
	canonical := sessionPoolTableKey(tableName)
	m.TableCache.TableCacheLock.Lock()
	defer m.TableCache.TableCacheLock.Unlock()
	for key, table := range m.TableCache.TableCache {
		if sessionPoolTableKey(key) == canonical || (table != nil && sessionPoolTableKey(table.Name) == canonical) {
			delete(m.TableCache.TableCache, key)
		}
	}
}

func (m *SessionPool) tableGenerationLocked(tableName string) uint64 {
	if m.tableGenerations == nil {
		m.tableGenerations = make(map[string]uint64)
	}
	return m.tableGenerations[sessionPoolTableKey(tableName)]
}

func (m *SessionPool) sessionGenerationStaleLocked(tableName string, generation uint64) bool {
	return generation != m.tableGenerationLocked(tableName)
}

func (m *SessionPool) returnSemaphore() {
	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	m.returnSemaphoreLocked()
}

func (m *SessionPool) returnSemaphoreLocked() {
	if m.closed {
		return
	}
	select {
	case m.semaphores <- struct{}{}:
	default:
	}
}

func sessionPoolTableKey(tableName string) string {
	return strings.ToLower(strings.TrimSpace(tableName))
}

// NewSession - Construct a new session.
func (m *SessionPool) NewSession(tableName string) (*Session, error) {

	conn, err := OpenSession(m.TableCache, m.baseDir, tableName, false, m.AppHost)
	if err != nil {
		u.Errorf("error opening quanta connection - %v", err)
		return nil, err
	}
	return conn, nil
}

// Shutdown - Terminate and destroy the pool.
func (m *SessionPool) Shutdown() {

	for _, v := range m.sessPoolMap {
		close(v.pool)
		for x := range v.pool {
			x.CloseSession()
		}
	}
	m.closed = true
	close(m.semaphores)
}

// Recover from network event.  Purge session and optionally recover unflushed buffers.
func (m *SessionPool) Recover(unflushedCh chan *shared.BatchBuffer) {

	if m.closed {
		return
	}

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()

	for _, v := range m.sessPoolMap {
		// Drain and close bad sessions
	loop:
		for {
			select {
			case r := <-v.pool:
				if unflushedCh != nil && !r.BatchBuffer.IsEmpty() {
					unflushedCh <- r.BatchBuffer
				}
				// Push a replacement semaphore
				select {
				case m.semaphores <- struct{}{}:
				default:
					continue
				}
			default:
				break loop
			}
		}
	}
}

// Lock - Prevent operations while a table maintenance event is in progress
func (m *SessionPool) Lock() {
	m.sessPoolLock.Lock()
}

// Unlock - Allow operations after a table maintenance event is complete
func (m *SessionPool) Unlock() {
	m.sessPoolLock.Unlock()
}

// Metrics - Return pool size and usage.
func (m *SessionPool) Metrics() (poolSize, inUse, pooled, maxUsed int) {

	poolSize = m.poolSize
	inUse = poolSize - len(m.semaphores)
	for _, v := range m.sessPoolMap {
		pooled = pooled + len(v.pool)
	}
	maxUsed = int(atomic.LoadInt32(&m.maxUsed))
	return
}
