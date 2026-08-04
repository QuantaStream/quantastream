package shared

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestNewLoaderConnectionDefaultsToStandardNative(t *testing.T) {
	address, stop := startLoaderConnectionTestGRPC(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := NewLoaderConnection(ctx, LoaderConnectionConfig{Address: address})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer conn.Disconnect()

	if conn.ServiceName != "quantastream" || conn.ServicePort != 0 || !conn.IsLocalCluster {
		t.Fatalf("connection = %+v, want standard-native single-node connection", conn)
	}
	if len(conn.clientConn) != 1 {
		t.Fatalf("client connections = %d, want one", len(conn.clientConn))
	}
	if len(conn.ids) != 1 || conn.ids[0] != "single-node" {
		t.Fatalf("connection ids = %#v, want single-node", conn.ids)
	}
}

func TestNewLoaderConnectionValidatesStandardNativeAddress(t *testing.T) {
	_, err := NewLoaderConnection(context.Background(), LoaderConnectionConfig{
		Mode: LoaderConnectionStandardNative,
	})
	if err == nil || !strings.Contains(err.Error(), "requires address") {
		t.Fatalf("NewLoaderConnection() error = %v, want missing address", err)
	}
}

func TestNewLoaderConnectionValidatesDistributedConsul(t *testing.T) {
	_, err := NewLoaderConnection(context.Background(), LoaderConnectionConfig{
		Mode: LoaderConnectionDistributed,
	})
	if err == nil || !strings.Contains(err.Error(), "requires Consul client") {
		t.Fatalf("NewLoaderConnection() error = %v, want missing Consul client", err)
	}
}

func TestNewLoaderConnectionRejectsUnsupportedMode(t *testing.T) {
	_, err := NewLoaderConnection(context.Background(), LoaderConnectionConfig{
		Mode: "mystery",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported loader connection mode") {
		t.Fatalf("NewLoaderConnection() error = %v, want unsupported mode", err)
	}
}

func TestApplyLoaderDistributedConnectionConfig(t *testing.T) {
	conn := NewDefaultConnection("loader-config")

	applyLoaderDistributedConnectionConfig(conn, LoaderConnectionConfig{
		ServiceName: "quantastream-test",
		ServicePort: 4100,
		Quorum:      2,
		Replicas:    3,
	})

	if conn.ServiceName != "quantastream-test" || conn.ServicePort != 4100 || conn.Quorum != 2 || conn.Replicas != 3 {
		t.Fatalf("distributed connection config = %+v", conn)
	}
}

func startLoaderConnectionTestGRPC(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
