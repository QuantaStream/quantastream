package qsloader

import (
	"testing"

	"github.com/QuantaStream/quantastream/core"
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
