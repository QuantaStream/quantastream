package qsfixture

import (
	"fmt"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

const (
	tpchFixtureSource = "tpch-fixture"
	tpchFixtureKind   = "tpch_order_lineitem"
)

// TPCHOrderLineitemEnvelopeOptions controls deterministic TPC-H shaped ingest
// envelope generation.
type TPCHOrderLineitemEnvelopeOptions struct {
	OrderCount        int
	LineitemsPerOrder int
	BaseOrderKey      int64
	StartedAt         time.Time
}

// TPCHOrderLineitemEnvelopeFixture contains deterministic streaming envelopes
// and selector tables for the order/lineitem shape.
type TPCHOrderLineitemEnvelopeFixture struct {
	Tables    []*core.Table
	Envelopes []core.IngestEnvelope
}

// NewTPCHOrderLineitemEnvelopeFixture builds a tiny deterministic stream-like
// fixture using TPC-H field names and a nested order->lineitems payload.
func NewTPCHOrderLineitemEnvelopeFixture(options TPCHOrderLineitemEnvelopeOptions) (TPCHOrderLineitemEnvelopeFixture, error) {
	if options.OrderCount <= 0 {
		options.OrderCount = 1
	}
	if options.LineitemsPerOrder <= 0 {
		options.LineitemsPerOrder = 1
	}
	if options.BaseOrderKey <= 0 {
		options.BaseOrderKey = 1001
	}
	if options.StartedAt.IsZero() {
		options.StartedAt = time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC)
	}
	fixture := TPCHOrderLineitemEnvelopeFixture{
		Tables:    TPCHOrderLineitemSelectorTables(),
		Envelopes: make([]core.IngestEnvelope, 0, options.OrderCount),
	}
	for i := 0; i < options.OrderCount; i++ {
		orderKey := options.BaseOrderKey + int64(i)
		eventTime := options.StartedAt.Add(time.Duration(i) * time.Second)
		envelope, err := core.NewStreamIngestEnvelope(core.StreamIngestEnvelopeRequest{
			EventID:      fmt.Sprintf("tpch.orders.%d", orderKey),
			Source:       tpchFixtureSource,
			EventTime:    eventTime,
			SourceOffset: fmt.Sprintf("orders:%d", i+1),
			Payload:      tpchOrderPayload(orderKey, i, options.LineitemsPerOrder, eventTime),
			Headers: map[string]interface{}{
				"fixture":     tpchFixtureKind,
				"order_index": i,
			},
		})
		if err != nil {
			return TPCHOrderLineitemEnvelopeFixture{}, err
		}
		fixture.Envelopes = append(fixture.Envelopes, envelope)
	}
	return fixture, nil
}

// TPCHOrderLineitemSelectorTables returns selector-only table metadata for the
// tiny fixture. Full catalog metadata remains owned by schema.yaml files.
func TPCHOrderLineitemSelectorTables() []*core.Table {
	return []*core.Table{
		{
			BasicTable: &shared.BasicTable{
				Name:       "orders",
				PrimaryKey: "o_orderkey",
				Selector:   `type = "orders"`,
			},
		},
		{
			BasicTable: &shared.BasicTable{
				Name:       "lineitem",
				PrimaryKey: "l_orderkey+l_linenumber",
				Selector:   `type = "lineitem"`,
			},
		},
	}
}

func tpchOrderPayload(orderKey int64, orderIndex, lineitemsPerOrder int, eventTime time.Time) map[string]interface{} {
	data := map[string]interface{}{
		"o_orderkey":      orderKey,
		"o_custkey":       int64(501 + orderIndex%3),
		"o_orderstatus":   "O",
		"o_totalprice":    100.00 + float64(orderIndex)*10.25,
		"o_orderdate":     eventTime,
		"o_orderpriority": "1-URGENT",
		"o_clerk":         fmt.Sprintf("Clerk#%09d", orderIndex+1),
		"o_shippriority":  int64(0),
		"o_comment":       fmt.Sprintf("deterministic streaming order %d", orderKey),
	}
	lineitems := make([]interface{}, 0, lineitemsPerOrder)
	for line := 1; line <= lineitemsPerOrder; line++ {
		lineitems = append(lineitems, tpchLineitemPayload(orderKey, orderIndex, line, eventTime))
	}
	data["lineitems"] = lineitems
	return map[string]interface{}{
		"type": "orders",
		"data": data,
	}
}

func tpchLineitemPayload(orderKey int64, orderIndex, lineNumber int, eventTime time.Time) map[string]interface{} {
	shipDate := eventTime.AddDate(0, 0, lineNumber)
	return map[string]interface{}{
		"type":            "lineitem",
		"l_orderkey":      orderKey,
		"l_partkey":       int64(200 + orderIndex + lineNumber),
		"l_suppkey":       int64(300 + lineNumber),
		"l_linenumber":    int64(lineNumber),
		"l_quantity":      int64(1 + lineNumber),
		"l_extendedprice": float64(25 * lineNumber),
		"l_discount":      float64(lineNumber) / 100,
		"l_tax":           0.02,
		"l_returnflag":    "N",
		"l_linestatus":    "O",
		"l_shipdate":      shipDate,
		"l_commitdate":    shipDate.AddDate(0, 0, -1),
		"l_receiptdate":   shipDate.AddDate(0, 0, 2),
		"l_shipinstruct":  "DELIVER IN PERSON",
		"l_shipmode":      "MAIL",
		"l_comment":       fmt.Sprintf("deterministic lineitem %d-%d", orderKey, lineNumber),
	}
}
