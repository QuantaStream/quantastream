package qsloader

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLoadSelectorTablesUsesConfiguredAllowlist(t *testing.T) {
	cache := core.NewTableCacheStruct()
	tables, err := loadSelectorTables(cache, Config{
		ConfigDir: "../tpc-h-benchmark/config",
		Database:  "quanta",
		Tables:    []string{"orders", "lineitem"},
	})
	if err != nil {
		t.Fatalf("loadSelectorTables() error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(tables))
	}
	if tables[0].Name != "lineitem" || tables[1].Name != "orders" {
		t.Fatalf("tables = %s/%s, want lineitem/orders sorted", tables[0].Name, tables[1].Name)
	}
	for _, table := range tables {
		if table.Selector == "" {
			t.Fatalf("table %s selector empty", table.Name)
		}
	}
}

func TestPayloadDataMapPrefersDataWrapper(t *testing.T) {
	data := map[string]interface{}{"id": int64(7)}
	payload := map[string]interface{}{"type": "orders", "data": data}
	if got := payloadDataMap(payload); got["id"] != int64(7) {
		t.Fatalf("payloadDataMap() = %#v, want data wrapper", got)
	}
}

func TestStatsEndpointReturnsLoaderSnapshot(t *testing.T) {
	putProfile := &core.RouterPutRowProfile{}
	putProfile.Observe("shard0", core.IngestRecord{TableName: "orders"}, core.PutRowResult{
		TableName:       "orders",
		Inserted:        true,
		LogicalRowCount: 1,
		TotalElapsed:    2 * time.Millisecond,
	})
	flushProfile := &core.RouterFlushProfile{}
	flushProfile.Observe("shard0", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:        3 * time.Millisecond,
		BitmapSetEntryCount: 4,
	})
	router := &core.SessionRouter{
		// Constructed directly so this test does not open worker goroutines.
	}
	server := &Server{
		config: Config{
			ListenAddress:        "127.0.0.1:8088",
			ConnectionMode:       shared.LoaderConnectionStandardNative,
			NativeGRPCAddr:       "127.0.0.1:4100",
			Workers:              1,
			ChannelSize:          100,
			FlushInterval:        time.Second,
			CommitOnClose:        true,
			PhysicalBuildRouting: true,
		},
		tables: []*core.Table{
			{BasicTable: &shared.BasicTable{Name: "orders"}},
			{BasicTable: &shared.BasicTable{Name: "lineitem"}},
		},
		router:       router,
		putProfile:   putProfile,
		flushProfile: flushProfile,
		drainProfile: &core.RouterDrainProfile{},
		startedAt:    time.Now().Add(-time.Second).UTC(),
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stats", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("/stats status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var stats StatsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode /stats: %v", err)
	}
	if stats.Status != "ok" {
		t.Fatalf("status = %q, want ok", stats.Status)
	}
	if stats.Config.NativeGRPCAddr != "127.0.0.1:4100" || !stats.Config.PhysicalBuildRouting {
		t.Fatalf("config = %+v, want native grpc and physical routing", stats.Config)
	}
	if !stats.Config.CommitOnClose {
		t.Fatalf("commit_on_close = false, want true")
	}
	if len(stats.Tables) != 2 || stats.Tables[0] != "lineitem" || stats.Tables[1] != "orders" {
		t.Fatalf("tables = %v, want sorted lineitem/orders", stats.Tables)
	}
	if stats.PutRow.RecordCount != 1 || stats.PutRow.InsertedCount != 1 {
		t.Fatalf("putrow = %+v, want one inserted record", stats.PutRow)
	}
	if stats.Flush.FlushCount != 1 || stats.Flush.BitmapSetEntryCount != 4 {
		t.Fatalf("flush = %+v, want one bitmap flush", stats.Flush)
	}
	if stats.Runtime.Goroutines == 0 {
		t.Fatalf("runtime goroutines = 0, want positive")
	}
}
