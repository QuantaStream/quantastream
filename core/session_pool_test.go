package core

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
)

func TestSessionPoolInvalidateTableDrainsPooledSessionsAndTableCache(t *testing.T) {
	cache := NewTableCacheStruct()
	cache.TableCache["customers_qa"] = &Table{BasicTable: &shared.BasicTable{Name: "customers_qa"}}
	pool := NewSessionPool(cache, nil, "", 2)
	pool.sessPoolLock.Lock()
	entry := pool.getPoolByTableName("customers_qa")
	entry.pool <- &Session{poolGeneration: entry.generation}
	pool.sessPoolLock.Unlock()

	pool.InvalidateTable("CUSTOMERS_QA")

	_, _, pooled, _ := pool.Metrics()
	if pooled != 0 {
		t.Fatalf("pooled sessions = %d, want 0", pooled)
	}
	if _, ok := cache.TableCache["customers_qa"]; ok {
		t.Fatalf("customers_qa table cache entry survived invalidation")
	}
}

func TestSessionPoolInvalidateTableClosesBorrowedSessionOnReturn(t *testing.T) {
	cache := NewTableCacheStruct()
	cache.TableCache["orders"] = &Table{BasicTable: &shared.BasicTable{Name: "orders"}}
	pool := NewSessionPool(cache, nil, "", 1)
	pool.sessPoolLock.Lock()
	entry := pool.getPoolByTableName("orders")
	borrowed := &Session{poolGeneration: entry.generation}
	pool.sessPoolLock.Unlock()
	<-pool.semaphores

	pool.InvalidateTable("orders")
	pool.Return("orders", borrowed)

	_, inUse, pooled, _ := pool.Metrics()
	if inUse != 0 || pooled != 0 {
		t.Fatalf("metrics after stale return = inUse %d pooled %d, want 0/0", inUse, pooled)
	}
}

func TestSessionPoolReturnWithProfileDoesNotReuseStaleFlushProfile(t *testing.T) {
	pool := NewSessionPool(NewTableCacheStruct(), nil, "", 1)
	pool.sessPoolLock.Lock()
	entry := pool.getPoolByTableName("orders")
	session := &Session{
		BatchBuffer:      shared.NewBatchBuffer(nil, nil, 10),
		poolGeneration:   entry.generation,
		lastFlushProfile: shared.BatchBufferFlushProfile{StartedAt: time.Now(), BSIValueEntryCount: 7},
	}
	pool.sessPoolLock.Unlock()
	<-pool.semaphores

	profile, err := pool.ReturnWithProfile("orders", session)
	if err != nil {
		t.Fatalf("ReturnWithProfile returned error: %v", err)
	}
	if !profile.StartedAt.IsZero() {
		t.Fatalf("profile.StartedAt = %v, want zero profile for empty fresh release", profile.StartedAt)
	}
}
