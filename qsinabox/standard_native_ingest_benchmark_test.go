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
	flushProfile := &core.RouterFlushProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:     core.NewTableCacheStruct(),
		BasePath:       process.Backend.ConfigBaseDir(config),
		Conn:           remoteConn,
		ShardCount:     shardCount,
		ChannelSize:    orderCount,
		FlushInterval:  10 * time.Millisecond,
		OnPutRowResult: putRowProfile.Callback(),
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
	if err := router.Close(); err != nil {
		b.Fatalf("router Close() error = %v", err)
	}
	benchmarkElapsed := time.Since(benchmarkStartedAt)
	b.StopTimer()

	totalOrders := b.N * orderCount
	totalLineitems := totalOrders * lineitemsPerOrder
	totalLogicalRows := totalOrders + totalLineitems
	putSnapshot := putRowProfile.Snapshot()
	flushSnapshot := flushProfile.Snapshot()
	if putSnapshot.RecordCount != totalOrders || putSnapshot.InsertedCount != totalOrders {
		b.Fatalf("put profile = %+v, want %d inserted order records", putSnapshot, totalOrders)
	}
	if flushSnapshot.FlushCount == 0 || flushSnapshot.BSIValueEntryCount == 0 || flushSnapshot.PartitionStringEntryCount == 0 {
		b.Fatalf("flush profile = %+v, want native write activity", flushSnapshot)
	}

	reportTPCHIngestBenchmarkMetrics(b, benchmarkElapsed, totalOrders, totalLineitems, totalLogicalRows, putSnapshot, flushSnapshot)
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

func reportTPCHIngestBenchmarkMetrics(
	b *testing.B,
	elapsed time.Duration,
	totalOrders int,
	totalLineitems int,
	totalLogicalRows int,
	putSnapshot core.RouterPutRowProfileSummary,
	flushSnapshot core.RouterFlushProfileSummary,
) {
	b.Helper()
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		return
	}
	b.ReportMetric(float64(totalOrders)/elapsedSeconds, "orders/s")
	b.ReportMetric(float64(totalLineitems)/elapsedSeconds, "lineitems/s")
	b.ReportMetric(float64(totalLogicalRows)/elapsedSeconds, "logical_rows/s")
	b.ReportMetric(float64(totalLogicalRows)/float64(maxInt(1, totalOrders)), "logical_rows/order")
	b.ReportMetric(float64(putSnapshot.TotalElapsed.Microseconds())/float64(maxInt(1, totalOrders)), "put_us/order")
	b.ReportMetric(float64(flushSnapshot.TotalElapsed.Microseconds())/float64(maxInt(1, flushSnapshot.FlushCount)), "flush_us/flush")
	b.ReportMetric(float64(flushSnapshot.FlushCount)/float64(maxInt(1, b.N)), "flushes/op")
	b.ReportMetric(float64(flushSnapshot.BSIValueEntryCount)/float64(maxInt(1, totalLogicalRows)), "bsi_entries/logical_row")
	b.ReportMetric(float64(flushSnapshot.PartitionStringEntryCount)/float64(maxInt(1, totalLogicalRows)), "kv_entries/logical_row")
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
