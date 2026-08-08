package main

import (
	"testing"
	"time"
)

func TestTPCHStreamProducerUsesSharedShardKeyForOrderAndLines(t *testing.T) {
	eventTime := time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC)
	order := orderEvent(100000001, 0, "test-source", "tpch.order.100000001", eventTime, 1500)
	line := lineitemEvent(100000001, 0, 1, "test-source", "tpch.order.100000001", eventTime, 2000, 100)

	if order.ShardKey != line.ShardKey {
		t.Fatalf("shard keys = %q/%q, want shared order shard", order.ShardKey, line.ShardKey)
	}
	if order.Payload["type"] != "orders" || line.Payload["type"] != "lineitem" {
		t.Fatalf("payload types = %#v/%#v", order.Payload["type"], line.Payload["type"])
	}
	orderData := order.Payload["data"].(map[string]interface{})
	lineData := line.Payload["data"].(map[string]interface{})
	if orderData["o_orderkey"] != int64(100000001) {
		t.Fatalf("order key = %#v", orderData["o_orderkey"])
	}
	if lineData["l_orderkey"] != int64(100000001) || lineData["l_suppkey"] != int64(1) {
		t.Fatalf("line data = %#v", lineData)
	}
}
