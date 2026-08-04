package qsinabox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsfixture"
	"github.com/QuantaStream/quantastream/shared"
)

func BenchmarkStandardProcessNativeGRPCRouterTPCHNestedIngest(b *testing.B) {
	orderCount := positiveIntEnv("QUANTASTREAM_TPCH_INGEST_BENCH_ORDERS", 100)
	lineitemsPerOrder := positiveIntEnv("QUANTASTREAM_TPCH_INGEST_BENCH_LINEITEMS", 4)
	shardCount := positiveIntEnv("QUANTASTREAM_TPCH_INGEST_BENCH_SHARDS", 1)
	profileName := stringEnv("QUANTASTREAM_TPCH_INGEST_BENCH_PROFILE", "standard-native-tpch-ingest")
	reportPath := strings.TrimSpace(os.Getenv("QUANTASTREAM_TPCH_INGEST_BENCH_REPORT"))
	primaryKeyMode := core.PrimaryKeyMode(stringEnv("QUANTASTREAM_TPCH_INGEST_BENCH_PK_MODE", "verify_existing")).Normalize()

	root := b.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTPCHNestedSchemas(b, configDir)
	config := StandardConfig{
		BindAddress:    "127.0.0.1",
		MySQLPort:      reserveStandardTestPort(b),
		NativeGRPCPort: reserveStandardTestPort(b),
		ConfigDir:      configDir,
		DataDir:        filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		b.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		b.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.NativeNode == nil {
		b.Fatalf("NativeNode = nil, want native gRPC listener")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	remoteConn, err := shared.NewLoaderConnection(dialCtx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-native-tpch-ingest-benchmark",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		b.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	putRowProfile := &core.RouterPutRowProfile{}
	drainProfile := &core.RouterDrainProfile{}
	flushProfile := &core.RouterFlushProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:     core.NewTableCacheStruct(),
		BasePath:       process.Backend.ConfigBaseDir(config),
		Conn:           remoteConn,
		ShardCount:     shardCount,
		ChannelSize:    orderCount,
		FlushInterval:  10 * time.Millisecond,
		PrimaryKeyMode: primaryKeyMode,
		OnPutRowResult: putRowProfile.Callback(),
		OnDrainProfile: drainProfile.Callback(),
		OnFlushProfile: flushProfile.Callback(),
	})
	if err != nil {
		b.Fatalf("NewSessionRouter() error = %v", err)
	}

	tables := qsfixture.TPCHOrderLineitemSelectorTables()
	startedAt := time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC)
	baseOrderKey := int64(1001)
	envelopeBatches := make([][]core.IngestEnvelope, b.N)
	for i := 0; i < b.N; i++ {
		fixture, err := qsfixture.NewTPCHOrderLineitemEnvelopeFixture(qsfixture.TPCHOrderLineitemEnvelopeOptions{
			OrderCount:        orderCount,
			LineitemsPerOrder: lineitemsPerOrder,
			BaseOrderKey:      baseOrderKey + int64(i*orderCount),
			StartedAt:         startedAt.Add(time.Duration(i*orderCount) * time.Second),
		})
		if err != nil {
			b.Fatalf("TPCH fixture error = %v", err)
		}
		envelopeBatches[i] = fixture.Envelopes
	}

	b.ResetTimer()
	benchmarkStartedAt := time.Now()
	enqueueStartedAt := time.Now()
	for _, envelopes := range envelopeBatches {
		for _, envelope := range envelopes {
			route, routeDiagnostics, err := core.RouteSelectedIngestEnvelope(router, envelope, core.IngestEnvelopeRouteOptions{
				Tables: tables,
			})
			if routeDiagnostics.BlocksNative() {
				b.Fatalf("route diagnostics = %#v, want none", routeDiagnostics)
			}
			if err != nil {
				b.Fatalf("RouteSelectedIngestEnvelope(%s) error = %v", envelope.EventID, err)
			}
			if !route.Enqueued {
				b.Fatalf("RouteSelectedIngestEnvelope(%s) did not enqueue", envelope.EventID)
			}
		}
	}
	enqueueElapsed := time.Since(enqueueStartedAt)
	drainStartedAt := time.Now()
	if err := router.Close(); err != nil {
		b.Fatalf("router Close() error = %v", err)
	}
	drainElapsed := time.Since(drainStartedAt)
	benchmarkElapsed := time.Since(benchmarkStartedAt)
	b.StopTimer()

	totalOrders := b.N * orderCount
	totalLineitems := totalOrders * lineitemsPerOrder
	totalLogicalRows := totalOrders + totalLineitems
	putSnapshot := putRowProfile.Snapshot()
	drainSnapshot := drainProfile.Snapshot()
	flushSnapshot := flushProfile.Snapshot()
	if putSnapshot.RecordCount != totalOrders || putSnapshot.InsertedCount != totalOrders {
		b.Fatalf("put profile = %+v, want %d inserted order records", putSnapshot, totalOrders)
	}
	if putSnapshot.ChildRowCount != totalLineitems || putSnapshot.LogicalRowCount != totalLogicalRows {
		b.Fatalf("put profile = %+v, want %d children and %d logical rows",
			putSnapshot, totalLineitems, totalLogicalRows)
	}
	if flushSnapshot.FlushCount == 0 || flushSnapshot.BSIValueEntryCount == 0 || flushSnapshot.PartitionStringEntryCount == 0 {
		b.Fatalf("flush profile = %+v, want native write activity", flushSnapshot)
	}
	if drainSnapshot.WorkerCount != shardCount {
		b.Fatalf("drain profile = %+v, want %d worker observations", drainSnapshot, shardCount)
	}

	metrics := qsfixture.NativeIngestBenchmarkMetrics(benchmarkElapsed, enqueueElapsed, drainElapsed,
		totalOrders, totalLineitems, totalLogicalRows, putSnapshot, drainSnapshot, flushSnapshot, b.N)
	reportTPCHIngestBenchmarkMetrics(b, metrics)
	report := qsfixture.BuildNativeIngestBenchmarkReport(qsfixture.NativeIngestBenchmarkReportRequest{
		Profile:           profileName,
		Mode:              StandardMode,
		OrderCount:        orderCount,
		LineitemsPerOrder: lineitemsPerOrder,
		ShardCount:        shardCount,
		RunCount:          b.N,
		PrimaryKeyMode:    string(primaryKeyMode),
		Elapsed:           benchmarkElapsed,
		EnqueueElapsed:    enqueueElapsed,
		DrainElapsed:      drainElapsed,
		PutRow:            putSnapshot,
		Drain:             drainSnapshot,
		Flush:             flushSnapshot,
		Metrics:           metrics,
	})
	if err := qsfixture.WriteNativeIngestBenchmarkReport(reportPath, report); err != nil {
		b.Fatalf("write benchmark report: %v", err)
	}
	requireStandardProcessScalarString(b, process, "select count(*) from orders", fmt.Sprint(totalOrders))
	requireStandardProcessScalarString(b, process, "select count(*) from lineitem", fmt.Sprint(totalLineitems))
	requireStandardProcessScalarString(b, process, `
select count(*) as joined_lineitems
from orders as o
inner join lineitem as l on l.l_orderkey = o.o_orderkey`, fmt.Sprint(totalLineitems))

	cancel()
	process.NativeNode.Close()
	select {
	case err := <-done:
		if err != nil {
			b.Fatalf("native gRPC server exited with error %v", err)
		}
	case <-time.After(5 * time.Second):
		b.Fatalf("native gRPC server did not stop")
	}
}

func positiveIntEnv(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func stringEnv(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func reportTPCHIngestBenchmarkMetrics(b *testing.B, metrics map[string]float64) {
	b.Helper()
	b.ReportMetric(metrics["orders_per_second"], "orders/s")
	b.ReportMetric(metrics["lineitems_per_second"], "lineitems/s")
	b.ReportMetric(metrics["logical_rows_per_second"], "logical_rows/s")
	b.ReportMetric(metrics["logical_rows_per_order"], "logical_rows/order")
	b.ReportMetric(metrics["enqueue_microseconds_per_order"], "enqueue_us/order")
	b.ReportMetric(metrics["drain_microseconds_per_order"], "drain_us/order")
	b.ReportMetric(metrics["drain_wall_percent"], "drain_wall_percent")
	b.ReportMetric(metrics["drain_worker_max_microseconds"], "drain_worker_max_us")
	b.ReportMetric(metrics["drain_worker_sum_to_wall_percent"], "drain_worker_sum_to_wall_percent")
	b.ReportMetric(metrics["drain_worker_max_to_avg_percent"], "drain_worker_max_to_avg_percent")
	b.ReportMetric(metrics["drain_sessions_per_worker"], "drain_sessions/worker")
	b.ReportMetric(metrics["put_microseconds_per_order"], "put_us/order")
	b.ReportMetric(metrics["flush_microseconds_per_order"], "flush_us/order")
	b.ReportMetric(metrics["flush_microseconds_per_flush"], "flush_us/flush")
	b.ReportMetric(metrics["flush_summed_to_drain_percent"], "flush_summed_to_drain_percent")
	b.ReportMetric(metrics["flush_shard_max_to_avg_percent"], "flush_shard_max_to_avg_percent")
	b.ReportMetric(metrics["flushes_per_operation"], "flushes/op")
	b.ReportMetric(metrics["bsi_entries_per_logical_row"], "bsi_entries/logical_row")
	b.ReportMetric(metrics["kv_entries_per_logical_row"], "kv_entries/logical_row")
	b.ReportMetric(metrics["put_stage_normalize_microseconds_per_order"], "put_stage_normalize_us/order")
	b.ReportMetric(metrics["put_stage_identity_microseconds_per_order"], "put_stage_identity_us/order")
	b.ReportMetric(metrics["put_stage_primary_key_microseconds_per_order"], "put_stage_pk_us/order")
	b.ReportMetric(metrics["put_stage_alternate_keys_microseconds_per_order"], "put_stage_alt_keys_us/order")
	b.ReportMetric(metrics["put_stage_child_expansion_microseconds_per_order"], "put_stage_child_expansion_us/order")
	b.ReportMetric(metrics["put_stage_child_traversal_microseconds_per_order"], "put_stage_child_traversal_us/order")
	b.ReportMetric(metrics["put_stage_child_recursive_write_microseconds_per_order"], "put_stage_child_recursive_write_us/order")
	b.ReportMetric(metrics["put_stage_parent_relation_microseconds_per_order"], "put_stage_parent_relation_us/order")
	b.ReportMetric(metrics["put_stage_attribute_mapping_microseconds_per_order"], "put_stage_attribute_mapping_us/order")
	b.ReportMetric(metrics["primary_key_resolves_per_logical_row"], "pk_resolves/logical_row")
	b.ReportMetric(metrics["primary_key_total_microseconds_per_resolve"], "pk_total_us/resolve")
	b.ReportMetric(metrics["primary_key_local_cache_hit_percent"], "pk_local_cache_hit_percent")
	b.ReportMetric(metrics["primary_key_assume_new_percent"], "pk_assume_new_percent")
	b.ReportMetric(metrics["primary_key_skipped_kv_lookup_percent"], "pk_skipped_kv_lookup_percent")
	b.ReportMetric(metrics["primary_key_kv_hit_percent"], "pk_kv_hit_percent")
	b.ReportMetric(metrics["primary_key_kv_lookup_microseconds_per_lookup"], "pk_kv_lookup_us/lookup")
	b.ReportMetric(metrics["primary_key_allocation_microseconds_per_allocation"], "pk_alloc_us/allocation")
	b.ReportMetric(metrics["primary_key_batch_cache_write_microseconds_per_write"], "pk_batch_write_us/write")
}
