package qsinabox

import (
	"context"
	"encoding/json"
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

	metrics := tpchIngestBenchmarkMetrics(benchmarkElapsed, totalOrders, totalLineitems, totalLogicalRows, putSnapshot, flushSnapshot, b.N)
	reportTPCHIngestBenchmarkMetrics(b, metrics)
	report := buildTPCHNativeIngestBenchmarkReport(tpchNativeIngestBenchmarkReportRequest{
		Profile:           profileName,
		Mode:              StandardMode,
		OrderCount:        orderCount,
		LineitemsPerOrder: lineitemsPerOrder,
		ShardCount:        shardCount,
		RunCount:          b.N,
		Elapsed:           benchmarkElapsed,
		PutRow:            putSnapshot,
		Flush:             flushSnapshot,
		Metrics:           metrics,
	})
	if err := writeTPCHNativeIngestBenchmarkReport(reportPath, report); err != nil {
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

func TestBuildTPCHNativeIngestBenchmarkReportCapturesProfiles(t *testing.T) {
	report := buildTPCHNativeIngestBenchmarkReport(tpchNativeIngestBenchmarkReportRequest{
		Profile:           "unit-profile",
		Mode:              StandardMode,
		OrderCount:        2,
		LineitemsPerOrder: 3,
		ShardCount:        1,
		RunCount:          4,
		Elapsed:           10 * time.Millisecond,
		PutRow: core.RouterPutRowProfileSummary{
			RecordCount:   8,
			InsertedCount: 8,
			TotalElapsed:  5 * time.Millisecond,
		},
		Flush: core.RouterFlushProfileSummary{
			FlushCount:                2,
			TotalElapsed:              3 * time.Millisecond,
			PartitionStringEntryCount: 8,
			BSIValueEntryCount:        24,
		},
		Metrics: map[string]float64{
			"orders_per_second": 800,
		},
	})
	if report.Profile != "unit-profile" || report.Config.OrderCount != 2 || report.Counts.TotalLogicalRows != 32 {
		t.Fatalf("report = %+v, want captured config/counts", report)
	}
	if report.PutRow.RecordCount != 8 || report.Flush.FlushCount != 2 {
		t.Fatalf("report profiles = %+v/%+v, want captured summaries", report.PutRow, report.Flush)
	}

	path := filepath.Join(t.TempDir(), "profiles", "ingest.json")
	if err := writeTPCHNativeIngestBenchmarkReport(path, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var decoded tpchNativeIngestBenchmarkReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if decoded.Profile != report.Profile || decoded.Counts.TotalLineitems != report.Counts.TotalLineitems {
		t.Fatalf("decoded report = %+v, want %+v", decoded, report)
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

type tpchNativeIngestBenchmarkReportRequest struct {
	Profile           string
	Mode              string
	OrderCount        int
	LineitemsPerOrder int
	ShardCount        int
	RunCount          int
	Elapsed           time.Duration
	PutRow            core.RouterPutRowProfileSummary
	Flush             core.RouterFlushProfileSummary
	Metrics           map[string]float64
}

type tpchNativeIngestBenchmarkReport struct {
	Version     int                              `json:"version"`
	Profile     string                           `json:"profile"`
	Mode        string                           `json:"mode"`
	GeneratedAt time.Time                        `json:"generated_at"`
	Config      tpchNativeIngestBenchmarkConfig  `json:"config"`
	Counts      tpchNativeIngestBenchmarkCounts  `json:"counts"`
	Timings     tpchNativeIngestBenchmarkTimings `json:"timings"`
	Metrics     map[string]float64               `json:"metrics"`
	PutRow      core.RouterPutRowProfileSummary  `json:"put_row"`
	Flush       core.RouterFlushProfileSummary   `json:"flush"`
}

type tpchNativeIngestBenchmarkConfig struct {
	OrderCount        int `json:"order_count"`
	LineitemsPerOrder int `json:"lineitems_per_order"`
	ShardCount        int `json:"shard_count"`
	RunCount          int `json:"run_count"`
}

type tpchNativeIngestBenchmarkCounts struct {
	TotalOrders      int `json:"total_orders"`
	TotalLineitems   int `json:"total_lineitems"`
	TotalLogicalRows int `json:"total_logical_rows"`
}

type tpchNativeIngestBenchmarkTimings struct {
	Elapsed      string `json:"elapsed"`
	ElapsedNanos int64  `json:"elapsed_nanos"`
}

func buildTPCHNativeIngestBenchmarkReport(request tpchNativeIngestBenchmarkReportRequest) tpchNativeIngestBenchmarkReport {
	totalOrders := request.OrderCount * request.RunCount
	totalLineitems := totalOrders * request.LineitemsPerOrder
	return tpchNativeIngestBenchmarkReport{
		Version:     1,
		Profile:     request.Profile,
		Mode:        request.Mode,
		GeneratedAt: time.Now().UTC(),
		Config: tpchNativeIngestBenchmarkConfig{
			OrderCount:        request.OrderCount,
			LineitemsPerOrder: request.LineitemsPerOrder,
			ShardCount:        request.ShardCount,
			RunCount:          request.RunCount,
		},
		Counts: tpchNativeIngestBenchmarkCounts{
			TotalOrders:      totalOrders,
			TotalLineitems:   totalLineitems,
			TotalLogicalRows: totalOrders + totalLineitems,
		},
		Timings: tpchNativeIngestBenchmarkTimings{
			Elapsed:      request.Elapsed.String(),
			ElapsedNanos: request.Elapsed.Nanoseconds(),
		},
		Metrics: copyFloatMetrics(request.Metrics),
		PutRow:  request.PutRow,
		Flush:   request.Flush,
	}
}

func writeTPCHNativeIngestBenchmarkReport(path string, report tpchNativeIngestBenchmarkReport) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0644)
}

func tpchIngestBenchmarkMetrics(
	elapsed time.Duration,
	totalOrders int,
	totalLineitems int,
	totalLogicalRows int,
	putSnapshot core.RouterPutRowProfileSummary,
	flushSnapshot core.RouterFlushProfileSummary,
	runCount int,
) map[string]float64 {
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		return map[string]float64{}
	}
	return map[string]float64{
		"orders_per_second":            float64(totalOrders) / elapsedSeconds,
		"lineitems_per_second":         float64(totalLineitems) / elapsedSeconds,
		"logical_rows_per_second":      float64(totalLogicalRows) / elapsedSeconds,
		"logical_rows_per_order":       float64(totalLogicalRows) / float64(maxInt(1, totalOrders)),
		"put_microseconds_per_order":   float64(putSnapshot.TotalElapsed.Microseconds()) / float64(maxInt(1, totalOrders)),
		"flush_microseconds_per_flush": float64(flushSnapshot.TotalElapsed.Microseconds()) / float64(maxInt(1, flushSnapshot.FlushCount)),
		"flushes_per_operation":        float64(flushSnapshot.FlushCount) / float64(maxInt(1, runCount)),
		"bsi_entries_per_logical_row":  float64(flushSnapshot.BSIValueEntryCount) / float64(maxInt(1, totalLogicalRows)),
		"kv_entries_per_logical_row":   float64(flushSnapshot.PartitionStringEntryCount) / float64(maxInt(1, totalLogicalRows)),
	}
}

func reportTPCHIngestBenchmarkMetrics(b *testing.B, metrics map[string]float64) {
	b.Helper()
	b.ReportMetric(metrics["orders_per_second"], "orders/s")
	b.ReportMetric(metrics["lineitems_per_second"], "lineitems/s")
	b.ReportMetric(metrics["logical_rows_per_second"], "logical_rows/s")
	b.ReportMetric(metrics["logical_rows_per_order"], "logical_rows/order")
	b.ReportMetric(metrics["put_microseconds_per_order"], "put_us/order")
	b.ReportMetric(metrics["flush_microseconds_per_flush"], "flush_us/flush")
	b.ReportMetric(metrics["flushes_per_operation"], "flushes/op")
	b.ReportMetric(metrics["bsi_entries_per_logical_row"], "bsi_entries/logical_row")
	b.ReportMetric(metrics["kv_entries_per_logical_row"], "kv_entries/logical_row")
}

func copyFloatMetrics(src map[string]float64) map[string]float64 {
	if src == nil {
		return nil
	}
	dst := make(map[string]float64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
