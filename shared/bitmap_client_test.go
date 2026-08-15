package shared

import (
	"context"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stvp/rendezvous"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestBatchMutateRequiresBitmapClients(t *testing.T) {
	index := NewBitmapIndex(NewDefaultConnection("empty-clients"))
	batch := map[string]map[string]map[uint64]map[int64]*Bitmap{
		"orders": {
			"o_orderkey": {
				1: {
					time.Unix(0, 0).UnixNano(): NewBitmap(roaring64.BitmapOf(1), false),
				},
			},
		},
	}

	err := index.BatchMutate(batch, false)
	if err == nil || !strings.Contains(err.Error(), "no bitmap clients available") {
		t.Fatalf("expected no-client batch mutate error, got %v", err)
	}
}

func TestBatchSetValueRequiresBitmapClients(t *testing.T) {
	index := NewBitmapIndex(NewDefaultConnection("empty-clients"))
	values := roaring64.NewDefaultBSI()
	values.SetBigValue(1, big.NewInt(10))
	batch := map[string]map[string]map[int64]*roaring64.BSI{
		"orders": {
			"o_orderkey": {
				time.Unix(0, 0).UnixNano(): values,
			},
		},
	}

	err := index.BatchSetValue(batch)
	if err == nil || !strings.Contains(err.Error(), "no bitmap clients available") {
		t.Fatalf("expected no-client batch set value error, got %v", err)
	}
}

func TestSplitBSIItemBatchPreEncodesReplicatedItems(t *testing.T) {
	conn := NewDefaultConnection("bsi-item-batch")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeMap = map[string]int{"node-0": 0, "node-1": 1, "node-2": 2}
	conn.HashTable = rendezvous.New(conn.ids)
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	values := roaring64.NewDefaultBSI()
	values.SetBigValue(1, big.NewInt(10))
	index := NewBitmapIndex(conn)
	_, batches, err := index.splitBSIItemBatch(map[string]map[string]map[int64]*roaring64.BSI{
		"orders": {
			"o_orderkey": {
				time.Unix(0, 0).UnixNano(): values,
			},
		},
	})
	if err != nil {
		t.Fatalf("splitBSIItemBatch returned error: %v", err)
	}

	var routed []*pb.IndexKVPair
	for _, batch := range batches {
		routed = append(routed, batch...)
	}
	if len(routed) != conn.Replicas {
		t.Fatalf("routed item count = %d, want %d replicas", len(routed), conn.Replicas)
	}
	for _, item := range routed {
		if item.IndexPath != "orders/o_orderkey" || item.Time != time.Unix(0, 0).UnixNano() {
			t.Fatalf("routed item = %+v", item)
		}
		if len(item.Value) == 0 || len(item.Value[0]) == 0 {
			t.Fatalf("routed item has empty BSI payload")
		}
	}
	if &routed[0].Value[0][0] != &routed[1].Value[0][0] {
		t.Fatalf("replicated routed BSI items should share one encoded payload")
	}
}

func TestActiveClientsSnapshotKeepsNodesWithMissingCachedStatus(t *testing.T) {
	conn := NewDefaultConnection("missing-status")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeStatusMap.Store("node-2", &pb.StatusMessage{NodeState: "Active"})

	index := NewBitmapIndex(conn)
	snapshot := index.activeClientsSnapshot()
	if got, want := len(snapshot), 3; got != want {
		t.Fatalf("active client snapshot len = %d, want %d", got, want)
	}
	for i, client := range snapshot {
		if client.index != i {
			t.Fatalf("snapshot[%d].index = %d, want %d", i, client.index, i)
		}
	}
}

func TestClientTargetHandlesMembershipShrink(t *testing.T) {
	conn := NewDefaultConnection("membership-shrink")
	conn.clientConn = []*grpc.ClientConn{{}, {}}

	target := conn.ClientTarget(2)
	if !strings.Contains(target, "index=2 unavailable clients=2") {
		t.Fatalf("target = %q, want unavailable index detail", target)
	}
}

func TestRelationshipAlignedValueSumClientsRoutesExactValueShard(t *testing.T) {
	conn := NewDefaultConnection("aligned-sum-route")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeMap = map[string]int{"node-0": 0, "node-1": 1, "node-2": 2}
	conn.HashTable = rendezvous.New(conn.ids)
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	index := NewBitmapIndex(conn)
	shardTime := time.Date(1995, time.March, 16, 0, 0, 0, 0, time.UTC).UnixNano()
	clients, err := index.relationshipAlignedValueSumClients("lineitem", "l_extendedprice", shardTime, shardTime)
	if err != nil {
		t.Fatalf("relationshipAlignedValueSumClients() error = %v", err)
	}
	if got, want := len(clients), 1; got != want {
		t.Fatalf("client count = %d, want %d", got, want)
	}
	routeKey := "lineitem/l_extendedprice/" + formatShardTime(time.Unix(0, shardTime))
	ownerID := conn.HashTable.GetN(conn.Replicas, routeKey)[0]
	if got, want := clients[0].index, conn.nodeMap[ownerID]; got != want {
		t.Fatalf("routed client index = %d, want owner %d for %s", got, want, routeKey)
	}
}

func TestRelationshipAlignedValueSumClientsBroadcastsBroadRangesAndExpressions(t *testing.T) {
	conn := NewDefaultConnection("aligned-sum-broadcast")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeMap = map[string]int{"node-0": 0, "node-1": 1, "node-2": 2}
	conn.HashTable = rendezvous.New(conn.ids)
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	index := NewBitmapIndex(conn)
	from := time.Date(1995, time.March, 16, 0, 0, 0, 0, time.UTC).UnixNano()
	to := time.Date(1995, time.March, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	clients, err := index.relationshipAlignedValueSumClients("lineitem", "l_extendedprice", from, to)
	if err != nil {
		t.Fatalf("relationshipAlignedValueSumClients() broad range error = %v", err)
	}
	if got, want := len(clients), 3; got != want {
		t.Fatalf("broad range client count = %d, want %d", got, want)
	}

	clients, err = index.relationshipAlignedValueSumClients("lineitem", "discounted_revenue(l_extendedprice,l_discount)", from, from)
	if err != nil {
		t.Fatalf("relationshipAlignedValueSumClients() expression error = %v", err)
	}
	if got, want := len(clients), 3; got != want {
		t.Fatalf("expression client count = %d, want %d", got, want)
	}
}

func TestBatchMutateItemsNodeUsesUnaryChunks(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &batchMutateItemsBitmapIndexServer{}
	pb.RegisterBitmapIndexServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	items := make([]*pb.IndexKVPair, bitmapBatchMutateItemsChunkSize+1)
	for i := range items {
		items[i] = &pb.IndexKVPair{
			IndexPath: "orders/o_orderkey",
			Key:       ToBytes(int64(1)),
			Value:     [][]byte{[]byte("payload")},
			Time:      int64(i),
		}
	}

	index := NewBitmapIndex(NewDefaultConnection("batch-mutate-items"))
	profile, err := index.batchMutateItemsNodeProfile(pb.NewBitmapIndexClient(conn), items)
	if err != nil {
		t.Fatalf("batchMutateItemsNodeProfile() error = %v", err)
	}
	if profile.Items != len(items) || profile.SendElapsed <= 0 || profile.TotalElapsed <= 0 {
		t.Fatalf("profile = %+v, want item count and non-zero elapsed timings", profile)
	}
	if got, want := fake.batchSizes(), []int{bitmapBatchMutateItemsChunkSize, 1}; !equalIntSlices(got, want) {
		t.Fatalf("BatchMutateItems batch sizes = %v, want %v", got, want)
	}
	if got := fake.streamCallCount(); got != 0 {
		t.Fatalf("BatchMutate stream calls = %d, want 0", got)
	}
}

func TestBitmapIndexCompareBSIFieldsFansOutToActiveClients(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &compareFanoutBitmapIndexServer{}
	pb.RegisterBitmapIndexServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conns := make([]*grpc.ClientConn, 3)
	for i := range conns {
		conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
		if err != nil {
			t.Fatalf("dial bufnet: %v", err)
		}
		conns[i] = conn
		t.Cleanup(func() {
			_ = conn.Close()
		})
	}

	conn := NewDefaultConnection("compare-fanout")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = conns
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	index := NewBitmapIndex(conn)
	matches, err := index.CompareBSIFields("lineitem", "l_receiptdate", "l_commitdate", 10, 20, roaring64.BitmapOf(1, 2, 3), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFields() error = %v", err)
	}
	if got, want := fake.callCount(), 3; got != want {
		t.Fatalf("compare fanout calls = %d, want %d", got, want)
	}
	if got, want := matches.GetCardinality(), uint64(3); got != want {
		t.Fatalf("matches cardinality = %d, want %d", got, want)
	}

	matches, stats, err := index.CompareBSIFieldsWithStats("lineitem", "l_receiptdate", "l_commitdate", 10, 20, roaring64.BitmapOf(1, 2, 3), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFieldsWithStats() error = %v", err)
	}
	if got, want := fake.callCount(), 6; got != want {
		t.Fatalf("compare fanout calls after stats call = %d, want %d", got, want)
	}
	if got, want := matches.GetCardinality(), uint64(3); got != want {
		t.Fatalf("stats matches cardinality = %d, want %d", got, want)
	}
	if stats.Nodes != 3 || stats.OutputRows != 3 {
		t.Fatalf("stats nodes/output = %d/%d, want 3/3", stats.Nodes, stats.OutputRows)
	}
	if stats.Left.ShardsVisited != 3 || stats.Right.ShardsRetained != 6 {
		t.Fatalf("stats projections = left visited %d right retained %d, want 3/6", stats.Left.ShardsVisited, stats.Right.ShardsRetained)
	}
	if stats.CompareElapsed != 6*time.Millisecond {
		t.Fatalf("compare elapsed = %s, want 6ms", stats.CompareElapsed)
	}
}

func TestBitmapIndexRelationshipReverseArtifactFansOutToActiveClients(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &reverseArtifactFanoutBitmapIndexServer{}
	pb.RegisterBitmapIndexServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conns := make([]*grpc.ClientConn, 3)
	for i := range conns {
		conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
		if err != nil {
			t.Fatalf("dial bufnet: %v", err)
		}
		conns[i] = conn
		t.Cleanup(func() {
			_ = conn.Close()
		})
	}

	conn := NewDefaultConnection("reverse-artifact-fanout")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = conns
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	index := NewBitmapIndex(conn)
	rownums, parentValues, stats, ok, err := index.RelationshipReverseArtifactCandidateValues("lineitem", "l_suppkey", []int64{7})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateValues() error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactCandidateValues() ok = false, want true")
	}
	if got, want := fake.candidateCallCount(), 3; got != want {
		t.Fatalf("candidate fanout calls = %d, want %d", got, want)
	}
	if got, want := fake.omitRownumsCandidateCallCount(), 3; got != want {
		t.Fatalf("omit rownums candidate calls = %d, want %d", got, want)
	}
	if got, want := rownums, []uint64{10, 20, 30}; !equalUint64Slices(got, want) {
		t.Fatalf("rownums = %v, want %v", got, want)
	}
	for _, rownum := range rownums {
		if parentValues[rownum] != 7 {
			t.Fatalf("parent value for row %d = %d, want 7", rownum, parentValues[rownum])
		}
	}
	if stats.Nodes != 3 || stats.SourceValues != 1 || stats.TargetRows != 3 || stats.LookupElapsed != 6*time.Millisecond {
		t.Fatalf("stats = %+v, want merged node stats", stats)
	}

	rowOnly, rowOnlyStats, ok, err := index.RelationshipReverseArtifactCandidateRowsForRowsUnordered("lineitem", "l_suppkey", []int64{7}, []uint64{10, 20, 30})
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactCandidateRowsForRowsUnordered() error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactCandidateRowsForRowsUnordered() ok = false, want true")
	}
	if got, want := fake.candidateCallCount(), 6; got != want {
		t.Fatalf("candidate fanout calls after row-only read = %d, want %d", got, want)
	}
	if got, want := fake.omitParentValuesCandidateCallCount(), 3; got != want {
		t.Fatalf("omit parent values candidate calls = %d, want %d", got, want)
	}
	if got, want := fake.omitRownumsCandidateCallCount(), 3; got != want {
		t.Fatalf("omit rownums candidate calls after row-only read = %d, want %d", got, want)
	}
	if got, want := rowOnly, []uint64{40, 50, 60}; !sameUint64Set(got, want) {
		t.Fatalf("row-only rownums = %v, want %v", got, want)
	}
	if rowOnlyStats.Nodes != 3 || rowOnlyStats.SourceValues != 1 || rowOnlyStats.TargetRows != 3 {
		t.Fatalf("row-only stats = %+v, want merged node stats", rowOnlyStats)
	}

	artifactStats, ok, err := index.RelationshipReverseArtifactStats("lineitem", "l_suppkey")
	if err != nil {
		t.Fatalf("RelationshipReverseArtifactStats() error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipReverseArtifactStats() ok = false, want true")
	}
	if got, want := fake.statsCallCount(), 3; got != want {
		t.Fatalf("stats fanout calls = %d, want %d", got, want)
	}
	if artifactStats.Nodes != 3 || artifactStats.Rows != 300 || artifactStats.Values != 30 {
		t.Fatalf("artifact stats = %+v, want merged stats", artifactStats)
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesDedupesUnorderedRows(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			Rownums: []uint64{30, 10, 20, 10},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
				{Rownum: 10, ParentValue: 7},
				{Rownum: 20, ParentValue: 8},
			},
			Stats: &pb.RelationshipReverseArtifactStats{
				TargetRows: 4,
			},
			Ok: true,
		},
		{
			Rownums: []uint64{20, 40, 30},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 20, ParentValue: 8},
				{Rownum: 40, ParentValue: 9},
				{Rownum: 30, ParentValue: 7},
			},
			Stats: &pb.RelationshipReverseArtifactStats{
				TargetRows: 3,
			},
			Ok: true,
		},
	}

	rownums, _, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7, 8, 9}, false, false, true)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if got, want := rownums, []uint64{30, 10, 20, 40}; !equalUint64Slices(got, want) {
		t.Fatalf("rownums = %v, want first-seen unique order %v", got, want)
	}
	if got, want := stats.TargetRows, uint64(4); got != want {
		t.Fatalf("target rows = %d, want deduped count %d", got, want)
	}
	if got, want := stats.RownumEntries, uint64(7); got != want {
		t.Fatalf("rownum entries = %d, want raw count %d", got, want)
	}
	if got, want := stats.DuplicateRownumEntries, uint64(3); got != want {
		t.Fatalf("duplicate rownum entries = %d, want duplicate count %d", got, want)
	}
	if got, want := stats.ParentValueEntries, uint64(6); got != want {
		t.Fatalf("parent value entries = %d, want raw count %d", got, want)
	}
	if got, want := stats.DuplicateParentValueEntries, uint64(2); got != want {
		t.Fatalf("duplicate parent value entries = %d, want duplicate count %d", got, want)
	}
	wantParentValues := map[uint64]int64{10: 7, 20: 8, 30: 7, 40: 9}
	for rownum, want := range wantParentValues {
		if got := parentValues[rownum]; got != want {
			t.Fatalf("parent value for row %d = %d, want %d", rownum, got, want)
		}
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesDerivesOrderedRowsFromParentValues(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			Rownums: []uint64{30, 10},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
				{Rownum: 10, ParentValue: 7},
			},
			Ok: true,
		},
		{
			Rownums: []uint64{20, 40},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 20, ParentValue: 8},
				{Rownum: 40, ParentValue: 9},
			},
			Ok: true,
		},
	}

	rownums, _, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7, 8, 9}, true, true, true)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if got, want := rownums, []uint64{10, 20, 30, 40}; !equalUint64Slices(got, want) {
		t.Fatalf("rownums = %v, want sorted parent-value row keys %v", got, want)
	}
	if got, want := stats.TargetRows, uint64(4); got != want {
		t.Fatalf("target rows = %d, want derived count %d", got, want)
	}
	wantParentValues := map[uint64]int64{10: 7, 20: 8, 30: 7, 40: 9}
	for rownum, want := range wantParentValues {
		if got := parentValues[rownum]; got != want {
			t.Fatalf("parent value for row %d = %d, want %d", rownum, got, want)
		}
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesCanDeriveUnorderedRowsFromParentValues(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			Rownums: []uint64{30, 10},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
				{Rownum: 10, ParentValue: 7},
			},
			Ok: true,
		},
		{
			Rownums: []uint64{20, 40},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 20, ParentValue: 8},
				{Rownum: 40, ParentValue: 9},
			},
			Ok: true,
		},
	}

	rownums, _, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7, 8, 9}, false, true, true)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if !sameUint64Set(rownums, []uint64{10, 20, 30, 40}) {
		t.Fatalf("rownums = %v, want parent-value row key set", rownums)
	}
	if got, want := stats.TargetRows, uint64(4); got != want {
		t.Fatalf("target rows = %d, want derived count %d", got, want)
	}
	wantParentValues := map[uint64]int64{10: 7, 20: 8, 30: 7, 40: 9}
	for rownum, want := range wantParentValues {
		if got := parentValues[rownum]; got != want {
			t.Fatalf("parent value for row %d = %d, want %d", rownum, got, want)
		}
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesCanDeriveRowsWhenRownumsOmitted(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
				{Rownum: 10, ParentValue: 7},
			},
			Ok: true,
		},
		{
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 20, ParentValue: 8},
				{Rownum: 40, ParentValue: 9},
			},
			Ok: true,
		},
	}

	rownums, _, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7, 8, 9}, false, true, true)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if !sameUint64Set(rownums, []uint64{10, 20, 30, 40}) {
		t.Fatalf("rownums = %v, want parent-value row key set", rownums)
	}
	if got, want := stats.TargetRows, uint64(4); got != want {
		t.Fatalf("target rows = %d, want derived count %d", got, want)
	}
	wantParentValues := map[uint64]int64{10: 7, 20: 8, 30: 7, 40: 9}
	for rownum, want := range wantParentValues {
		if got := parentValues[rownum]; got != want {
			t.Fatalf("parent value for row %d = %d, want %d", rownum, got, want)
		}
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesCanReturnAlignedParentValuesWithoutMap(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
				{Rownum: 10, ParentValue: 7},
			},
			Ok: true,
		},
		{
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 20, ParentValue: 8},
				{Rownum: 10, ParentValue: 7},
				{Rownum: 40, ParentValue: 9},
			},
			Ok: true,
		},
	}

	rownums, alignedParentValues, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7, 8, 9}, true, true, false)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if got, want := rownums, []uint64{10, 20, 30, 40}; !equalUint64Slices(got, want) {
		t.Fatalf("rownums = %v, want sorted rownums %v", got, want)
	}
	if got, want := alignedParentValues, []int64{7, 8, 7, 9}; !equalInt64Slices(got, want) {
		t.Fatalf("aligned parent values = %v, want %v", got, want)
	}
	if parentValues != nil {
		t.Fatalf("parent value map = %#v, want nil", parentValues)
	}
	if got, want := stats.TargetRows, uint64(4); got != want {
		t.Fatalf("target rows = %d, want derived count %d", got, want)
	}
	if got, want := stats.ParentValueEntries, uint64(5); got != want {
		t.Fatalf("parent value entries = %d, want raw count %d", got, want)
	}
	if got, want := stats.DuplicateParentValueEntries, uint64(1); got != want {
		t.Fatalf("duplicate parent value entries = %d, want duplicate count %d", got, want)
	}
}

func TestAggregateRelationshipReverseArtifactCandidateResponsesFallsBackWhenParentValuesDoNotCoverRows(t *testing.T) {
	responses := []*pb.RelationshipReverseArtifactCandidatesResponse{
		{
			Rownums: []uint64{30, 10},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 30, ParentValue: 7},
			},
			Ok: true,
		},
	}

	rownums, _, parentValues, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, []int64{7}, true, true, true)
	if err != nil {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() error = %v", err)
	}
	if !ok {
		t.Fatalf("aggregateRelationshipReverseArtifactCandidateResponses() ok = false, want true")
	}
	if got, want := rownums, []uint64{10, 30}; !equalUint64Slices(got, want) {
		t.Fatalf("rownums = %v, want sorted response rownums %v", got, want)
	}
	if got, want := stats.TargetRows, uint64(2); got != want {
		t.Fatalf("target rows = %d, want response row count %d", got, want)
	}
	if got, want := parentValues[30], int64(7); got != want {
		t.Fatalf("parent value for row 30 = %d, want %d", got, want)
	}
	if _, ok := parentValues[10]; ok {
		t.Fatalf("parent value unexpectedly present for row 10")
	}
}

func TestRelationshipReverseArtifactCandidateResponseCapacities(t *testing.T) {
	rownums, parentValues := relationshipReverseArtifactCandidateResponseCapacities([]*pb.RelationshipReverseArtifactCandidatesResponse{
		nil,
		{
			Rownums: []uint64{1, 2, 3},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 1, ParentValue: 7},
				{Rownum: 2, ParentValue: 8},
			},
		},
		{
			Rownums: []uint64{4},
			ParentValues: []*pb.RelationshipReverseArtifactParentValue{
				{Rownum: 4, ParentValue: 9},
			},
		},
	})

	if rownums != 4 || parentValues != 3 {
		t.Fatalf("capacities = rownums %d parentValues %d, want 4/3", rownums, parentValues)
	}
}

func TestAggregateRelationshipAlignedValueSumResponsesMergesPartials(t *testing.T) {
	groups, stats, ok, err := aggregateRelationshipAlignedValueSumResponses([]relationshipAlignedValueSumClientResult{
		{
			response: &pb.RelationshipAlignedValueSumResponse{
				Ok: true,
				Groups: []*pb.RelationshipAlignedValueSumGroup{{
					ParentValue:       7,
					RepresentativeRow: 101,
					Count:             1,
					Sum:               "10",
				}},
				Stats: &pb.RelationshipAlignedValueSumStats{
					ProjectionElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
				},
			},
			elapsed: 7 * time.Millisecond,
		},
		{
			response: &pb.RelationshipAlignedValueSumResponse{
				Ok: true,
				Groups: []*pb.RelationshipAlignedValueSumGroup{{
					ParentValue:       7,
					RepresentativeRow: 102,
					Count:             2,
					Sum:               "20",
				}, {
					ParentValue:       8,
					RepresentativeRow: 103,
					Count:             1,
					Sum:               "5",
				}},
				Stats: &pb.RelationshipAlignedValueSumStats{
					ProjectionElapsedNanos: (3 * time.Millisecond).Nanoseconds(),
				},
			},
			elapsed: 5 * time.Millisecond,
		},
	}, []uint64{101, 102, 103}, []uint64{7, 7, 8})
	if err != nil {
		t.Fatalf("aggregateRelationshipAlignedValueSumResponses() error = %v", err)
	}
	if !ok {
		t.Fatal("aggregateRelationshipAlignedValueSumResponses() ok = false, want true")
	}
	if stats.Rows != 3 || stats.SourceValues != 2 || stats.TargetRows != 4 || stats.Groups != 2 || stats.Values != 2 {
		t.Fatalf("stats = %+v, want rows/source/target/groups/values 3/2/4/2/2", stats)
	}
	if stats.Nodes != 2 || stats.ProjectionElapsed != 5*time.Millisecond {
		t.Fatalf("stats nodes/projection elapsed = %d/%s, want 2/5ms", stats.Nodes, stats.ProjectionElapsed)
	}
	if stats.ClientRPCElapsed != 12*time.Millisecond || stats.MaxClientRPCElapsed != 7*time.Millisecond {
		t.Fatalf("stats rpc elapsed/max = %s/%s, want 12ms/7ms", stats.ClientRPCElapsed, stats.MaxClientRPCElapsed)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 101 || groups[0].Count != 3 || groups[0].Sum.Cmp(big.NewInt(30)) != 0 {
		t.Fatalf("groups[0] = %+v, want parent 7 count 3 sum 30", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 103 || groups[1].Count != 1 || groups[1].Sum.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("groups[1] = %+v, want parent 8 count 1 sum 5", groups[1])
	}
}

func TestAggregateRelationshipAlignedValueSumResponsesRequiresAllNodesOK(t *testing.T) {
	_, _, ok, err := aggregateRelationshipAlignedValueSumResponses([]relationshipAlignedValueSumClientResult{
		{
			response: &pb.RelationshipAlignedValueSumResponse{
				Ok: true,
				Groups: []*pb.RelationshipAlignedValueSumGroup{{
					ParentValue:       7,
					RepresentativeRow: 101,
					Count:             1,
					Sum:               "10",
				}},
			},
		},
		{response: &pb.RelationshipAlignedValueSumResponse{Ok: false}},
	}, []uint64{101}, []uint64{7})
	if err != nil {
		t.Fatalf("aggregateRelationshipAlignedValueSumResponses() error = %v", err)
	}
	if ok {
		t.Fatal("aggregateRelationshipAlignedValueSumResponses() ok = true with one non-ok node, want false")
	}
}

func TestAggregateBitmapGroupAggregateResponsesMergesPartials(t *testing.T) {
	groups, stats, ok, err := aggregateBitmapGroupAggregateResponses([]*pb.BitmapGroupAggregatesResponse{
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 2,
				}, {
					Count: 2,
					Sum:   "30",
					Min:   "10",
					Max:   "20",
				}},
			}},
			Stats: &pb.BitmapGroupAggregateStats{
				ValueCount:             4,
				BsiProjectElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
				SumElapsedNanos:        (1 * time.Millisecond).Nanoseconds(),
			},
		},
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 1,
				}, {
					Count: 1,
					Sum:   "7",
					Min:   "7",
					Max:   "7",
				}},
			}, {
				Values: []uint64{2, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 1,
				}, {
					Count: 1,
					Sum:   "5",
					Min:   "5",
					Max:   "5",
				}},
			}},
			Stats: &pb.BitmapGroupAggregateStats{
				ValueCount:             3,
				BsiProjectElapsedNanos: (3 * time.Millisecond).Nanoseconds(),
				SumElapsedNanos:        (2 * time.Millisecond).Nanoseconds(),
			},
		},
	}, 4, 2, 2)
	if err != nil {
		t.Fatalf("aggregateBitmapGroupAggregateResponses() error = %v", err)
	}
	if !ok {
		t.Fatal("aggregateBitmapGroupAggregateResponses() ok = false, want true")
	}
	if stats.Nodes != 2 || stats.CandidateRows != 4 || stats.FieldCount != 2 || stats.AggregateCount != 2 || stats.ValueCount != 4 || stats.Groups != 2 {
		t.Fatalf("stats = %+v, want nodes/candidates/fields/aggs/values/groups 2/4/2/2/4/2", stats)
	}
	if stats.BSIProjectElapsed != 5*time.Millisecond || stats.SumElapsed != 3*time.Millisecond {
		t.Fatalf("stats elapsed = %s/%s, want 5ms/3ms", stats.BSIProjectElapsed, stats.SumElapsed)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}
	if got, want := groups[0].Values, []uint64{1, 10}; !equalUint64Slices(got, want) {
		t.Fatalf("groups[0] values = %#v, want %#v", got, want)
	}
	if groups[0].Aggs[0].Count != 3 || groups[0].Aggs[1].Count != 3 || groups[0].Aggs[1].Sum.Cmp(big.NewInt(37)) != 0 || groups[0].Aggs[1].Min.Cmp(big.NewInt(7)) != 0 || groups[0].Aggs[1].Max.Cmp(big.NewInt(20)) != 0 {
		t.Fatalf("groups[0] aggregates = %+v, want merged count 3 sum 37 min 7 max 20", groups[0].Aggs)
	}
	if got, want := groups[1].Values, []uint64{2, 10}; !equalUint64Slices(got, want) {
		t.Fatalf("groups[1] values = %#v, want %#v", got, want)
	}
}

func TestAggregateBitmapGroupAggregateResponsesRequiresAllNodesOK(t *testing.T) {
	_, _, ok, err := aggregateBitmapGroupAggregateResponses([]*pb.BitmapGroupAggregatesResponse{
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1},
				Aggs:   []*pb.BitmapGroupAggregateValue{{Count: 1}},
			}},
		},
		{Ok: false},
	}, 1, 1, 1)
	if err != nil {
		t.Fatalf("aggregateBitmapGroupAggregateResponses() error = %v", err)
	}
	if ok {
		t.Fatal("aggregateBitmapGroupAggregateResponses() ok = true with one non-ok node, want false")
	}
}

type batchMutateItemsBitmapIndexServer struct {
	pb.UnimplementedBitmapIndexServer
	mu             sync.Mutex
	batchSizesSeen []int
	streamCalls    int
}

func (s *batchMutateItemsBitmapIndexServer) BatchMutateItems(_ context.Context, batch *pb.IndexKVBatch) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchSizesSeen = append(s.batchSizesSeen, len(batch.GetItems()))
	return &emptypb.Empty{}, nil
}

func (s *batchMutateItemsBitmapIndexServer) BatchMutate(pb.BitmapIndex_BatchMutateServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamCalls++
	return nil
}

func (s *batchMutateItemsBitmapIndexServer) batchSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.batchSizesSeen...)
}

func (s *batchMutateItemsBitmapIndexServer) streamCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamCalls
}

type compareFanoutBitmapIndexServer struct {
	pb.UnimplementedBitmapIndexServer
	mu    sync.Mutex
	calls int
}

func (s *compareFanoutBitmapIndexServer) CompareBSIFields(context.Context, *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	bitmap := roaring64.BitmapOf(uint64(call))
	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.CompareBSIFieldsResponse{
		Rownums: data,
		Stats: &pb.CompareBSIFieldsStats{
			Left: &pb.BSIProjectionStats{
				ShardsVisited: 1,
			},
			Right: &pb.BSIProjectionStats{
				ShardsRetained: 2,
			},
			CompareElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
			OutputRows:          1,
		},
	}, nil
}

func (s *compareFanoutBitmapIndexServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type reverseArtifactFanoutBitmapIndexServer struct {
	pb.UnimplementedBitmapIndexServer
	mu               sync.Mutex
	candidateCalls   int
	omitRownumsCalls int
	omitParentCalls  int
	statsCalls       int
}

func (s *reverseArtifactFanoutBitmapIndexServer) RelationshipReverseArtifactCandidates(_ context.Context, req *pb.RelationshipReverseArtifactCandidatesRequest) (*pb.RelationshipReverseArtifactCandidatesResponse, error) {
	s.mu.Lock()
	s.candidateCalls++
	omitRownums := req.GetOmitRownums()
	omitParentValues := req.GetOmitParentValues()
	if omitRownums {
		s.omitRownumsCalls++
	}
	if omitParentValues {
		s.omitParentCalls++
	}
	call := s.candidateCalls
	s.mu.Unlock()

	rownum := uint64(call * 10)
	rownums := []uint64{rownum}
	if omitRownums {
		rownums = nil
	}
	parentValues := []*pb.RelationshipReverseArtifactParentValue{{
		Rownum:      rownum,
		ParentValue: 7,
	}}
	if omitParentValues {
		parentValues = nil
	}
	return &pb.RelationshipReverseArtifactCandidatesResponse{
		Rownums:      rownums,
		ParentValues: parentValues,
		Stats: &pb.RelationshipReverseArtifactStats{
			Rows:               100,
			Values:             10,
			SourceValues:       1,
			TargetRows:         1,
			LookupElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
		},
		Ok: true,
	}, nil
}

func (s *reverseArtifactFanoutBitmapIndexServer) RelationshipReverseArtifactStats(context.Context, *pb.RelationshipReverseArtifactStatsRequest) (*pb.RelationshipReverseArtifactStatsResponse, error) {
	s.mu.Lock()
	s.statsCalls++
	s.mu.Unlock()

	return &pb.RelationshipReverseArtifactStatsResponse{
		Stats: &pb.RelationshipReverseArtifactStats{
			Rows:   100,
			Values: 10,
		},
		Ok: true,
	}, nil
}

func (s *reverseArtifactFanoutBitmapIndexServer) candidateCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.candidateCalls
}

func (s *reverseArtifactFanoutBitmapIndexServer) omitRownumsCandidateCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.omitRownumsCalls
}

func (s *reverseArtifactFanoutBitmapIndexServer) omitParentValuesCandidateCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.omitParentCalls
}

func (s *reverseArtifactFanoutBitmapIndexServer) statsCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statsCalls
}

func equalIntSlices(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalUint64Slices(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalInt64Slices(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func sameUint64Set(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[uint64]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}
