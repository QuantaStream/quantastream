package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type streamEvent struct {
	Mode         string                 `json:"mode"`
	EventID      string                 `json:"event_id"`
	Source       string                 `json:"source"`
	EventTime    string                 `json:"event_time"`
	SourceOffset string                 `json:"source_offset"`
	ShardKey     string                 `json:"shard_key"`
	Payload      map[string]interface{} `json:"payload"`
}

type ingestResponse struct {
	Accepted int      `json:"accepted"`
	Failed   int      `json:"failed"`
	Errors   []string `json:"errors,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("tpch-stream-producer", flag.ContinueOnError)
	flags.SetOutput(stderr)

	target := flags.String("target", "http://127.0.0.1:8088/ingest/json", "quantastream-loader JSON endpoint")
	orders := flags.Int("orders", 10, "number of synthetic orders to emit")
	lineitems := flags.Int("lineitems", 4, "lineitems per order")
	batchSize := flags.Int("batch-size", 25, "events per HTTP POST")
	baseOrderKey := flags.Int64("base-order-key", 100000001, "first synthetic order key")
	source := flags.String("source", "tpch-stream-producer", "event source")
	startedAtRaw := flags.String("started-at", "1995-03-15T12:00:00Z", "first event timestamp")
	interval := flags.Duration("interval", 0, "sleep interval between generated orders")
	customerCount := flags.Int64("customer-count", 1500, "existing customer key domain size")
	partCount := flags.Int64("part-count", 2000, "existing part key domain size")
	supplierCount := flags.Int64("supplier-count", 100, "existing supplier key domain size")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *orders < 1 || *lineitems < 1 || *batchSize < 1 || *customerCount < 1 || *partCount < 1 || *supplierCount < 1 {
		fmt.Fprintln(stderr, "orders, lineitems, batch-size, customer-count, part-count, and supplier-count must be positive")
		return 2
	}
	startedAt, err := time.Parse(time.RFC3339Nano, *startedAtRaw)
	if err != nil {
		fmt.Fprintf(stderr, "parse -started-at: %v\n", err)
		return 2
	}
	client := &http.Client{Timeout: 30 * time.Second}
	batch := make([]streamEvent, 0, *batchSize)
	sent := 0

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if err := postBatch(client, *target, batch); err != nil {
			fmt.Fprintf(stderr, "post batch: %v\n", err)
			return false
		}
		sent += len(batch)
		batch = batch[:0]
		return true
	}

	for i := 0; i < *orders; i++ {
		orderKey := *baseOrderKey + int64(i)
		eventTime := startedAt.Add(time.Duration(i) * time.Second).UTC()
		shardKey := fmt.Sprintf("tpch.order.%d", orderKey)
		order := orderEvent(orderKey, i, *source, shardKey, eventTime, *customerCount)
		batch = append(batch, order)
		for line := 1; line <= *lineitems; line++ {
			batch = append(batch, lineitemEvent(orderKey, i, line, *source, shardKey, eventTime, *partCount, *supplierCount))
			if len(batch) >= *batchSize && !flush() {
				return 1
			}
		}
		if len(batch) >= *batchSize && !flush() {
			return 1
		}
		if *interval > 0 {
			time.Sleep(*interval)
		}
	}
	if !flush() {
		return 1
	}
	fmt.Fprintf(stdout, "tpch stream producer complete target=%s orders=%d lineitems_per_order=%d events=%d\n",
		*target, *orders, *lineitems, sent)
	return 0
}

func postBatch(client *http.Client, target string, events []streamEvent) error {
	body, err := json.Marshal(map[string]interface{}{"events": events})
	if err != nil {
		return err
	}
	resp, err := client.Post(target, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("loader returned %s: %s", resp.Status, string(data))
	}
	var result ingestResponse
	if len(data) > 0 {
		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("decode loader response: %w", err)
		}
	}
	if result.Failed > 0 {
		detail := ""
		if len(result.Errors) > 0 {
			detail = fmt.Sprintf(": %s", result.Errors[0])
		}
		return fmt.Errorf("loader accepted %d events and failed %d%s", result.Accepted, result.Failed, detail)
	}
	return nil
}

func orderEvent(orderKey int64, orderIndex int, source, shardKey string, eventTime time.Time, customerCount int64) streamEvent {
	customerKey := int64(1)
	if customerCount > 0 {
		customerKey = int64(orderIndex)%customerCount + 1
	}
	return streamEvent{
		Mode:         "stream",
		EventID:      fmt.Sprintf("tpch.orders.%d", orderKey),
		Source:       source,
		EventTime:    eventTime.Format(time.RFC3339Nano),
		SourceOffset: fmt.Sprintf("orders:%d", orderIndex+1),
		ShardKey:     shardKey,
		Payload: map[string]interface{}{
			"type": "orders",
			"data": map[string]interface{}{
				"o_orderkey":      orderKey,
				"o_custkey":       customerKey,
				"o_orderstatus":   "O",
				"o_totalprice":    100.00 + float64(orderIndex)*10.25,
				"o_orderdate":     eventTime.Format("2006-01-02"),
				"o_orderpriority": "1-URGENT",
				"o_clerk":         fmt.Sprintf("Clerk#%09d", orderIndex+1),
				"o_shippriority":  0,
				"o_comment":       fmt.Sprintf("synthetic streaming order %d", orderKey),
			},
		},
	}
}

func lineitemEvent(orderKey int64, orderIndex, lineNumber int, source, shardKey string, eventTime time.Time,
	partCount, supplierCount int64) streamEvent {

	partKey := int64(orderIndex+lineNumber)%partCount + 1
	supplierKey := int64(lineNumber-1)%supplierCount + 1
	shipDate := eventTime.AddDate(0, 0, lineNumber)
	return streamEvent{
		Mode:         "stream",
		EventID:      fmt.Sprintf("tpch.lineitem.%d.%d", orderKey, lineNumber),
		Source:       source,
		EventTime:    eventTime.Format(time.RFC3339Nano),
		SourceOffset: fmt.Sprintf("lineitem:%d:%d", orderIndex+1, lineNumber),
		ShardKey:     shardKey,
		Payload: map[string]interface{}{
			"type": "lineitem",
			"data": map[string]interface{}{
				"l_orderkey":      orderKey,
				"l_partkey":       partKey,
				"l_suppkey":       supplierKey,
				"l_linenumber":    lineNumber,
				"l_quantity":      1 + lineNumber,
				"l_extendedprice": float64(25 * lineNumber),
				"l_discount":      float64(lineNumber) / 100,
				"l_tax":           0.02,
				"l_returnflag":    "N",
				"l_linestatus":    "O",
				"l_shipdate":      shipDate.Format("2006-01-02"),
				"l_commitdate":    shipDate.AddDate(0, 0, -1).Format("2006-01-02"),
				"l_receiptdate":   shipDate.AddDate(0, 0, 2).Format("2006-01-02"),
				"l_shipinstruct":  "DELIVER IN PERSON",
				"l_shipmode":      "MAIL",
				"l_comment":       fmt.Sprintf("synthetic streaming lineitem %d-%d", orderKey, lineNumber),
			},
		},
	}
}
