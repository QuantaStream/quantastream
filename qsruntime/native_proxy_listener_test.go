package qsruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
	mysql "github.com/go-sql-driver/mysql"
)

func TestNativeProxyListenConfigDefaultsFromFrontDoor(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{BindAddress: "0.0.0.0", Port: 4400})
	config := (NativeProxyListenConfig{}).WithDefaults(frontDoor)
	if config.Address != "0.0.0.0:4400" {
		t.Fatalf("address = %q", config.Address)
	}
}

func TestNativeProxyFrontDoorListenAndServeIsDisabledScaffold(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{PacketIOReady: true})
	err := frontDoor.ListenAndServe(context.Background(), NativeProxyListenConfig{})
	if !errors.Is(err, ErrNativeProxyListenerNotReady) {
		t.Fatalf("err = %v", err)
	}
}

func TestNativeProxyFrontDoorListenAndServeAcceptsInjectedConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	listener := newNativeProxyTestListener(serverConn)
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{PacketIOReady: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- frontDoor.ListenAndServe(ctx, NativeProxyListenConfig{Listener: listener, EnableAcceptLoop: true, ConnectionIDStart: 41})
	}()

	client := qsmysql.NewStream(clientConn, clientConn)
	handshake, err := client.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("read handshake failed: %v", err)
	}
	if handshake.SequenceID != 0 || len(handshake.Payload) == 0 {
		t.Fatalf("handshake = %#v", handshake)
	}
	if got := nativeProxyHandshakeConnectionID(t, handshake); got != 41 {
		t.Fatalf("handshake connection id = %d, want 41", got)
	}
	response, err := qsmysql.DecodePacket(nativeProxyTestHandshakeResponsePacket(t))
	if err != nil {
		t.Fatalf("DecodePacket handshake response failed: %v", err)
	}
	if err := client.WritePacket(context.Background(), response); err != nil {
		t.Fatalf("write handshake response failed: %v", err)
	}
	authOK, err := client.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("read auth OK failed: %v", err)
	}
	if authOK.Payload[0] != 0x00 {
		t.Fatalf("authOK = %#v", authOK)
	}
	quit, err := qsmysql.DecodePacket(nativeProxyTestCommandPacket(t, qsmysql.CommandQuit))
	if err != nil {
		t.Fatalf("DecodePacket quit failed: %v", err)
	}
	if err := client.WritePacket(context.Background(), quit); err != nil {
		t.Fatalf("write quit failed: %v", err)
	}
	cancel()
	listener.Close()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not stop after cancellation")
	}
}

func TestNativeProxyFrontDoorDatabaseSQLClientReceivesAccessDenied(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed atomic.Bool
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed.Store(true)
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{
		PacketIOReady: true,
		Authenticator: qsmysql.StaticAuthenticator{Accounts: []qsmysql.StaticAccount{{
			Username:        "guy",
			Password:        "secret",
			DefaultDatabase: "quanta",
			Roles:           []qsbridge.RoleName{"reader"},
		}}},
		Server: NativeProxyServerConfig{Authorizer: qsbridge.NewAccessPolicy(qsbridge.AccessGrant{
			PrincipalKind: qsbridge.AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     qsbridge.AccessSelect,
			Table:         qsbridge.TableInstance{Schema: "quanta", Table: "customer"},
		})},
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- frontDoor.ListenAndServe(ctx, NativeProxyListenConfig{
			Listener:         listener,
			EnableAcceptLoop: true,
		})
	}()
	defer func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("ListenAndServe err = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("ListenAndServe did not stop after cancellation")
		}
	}()

	clientCtx, clientCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer clientCancel()
	db, err := sql.Open("mysql", fmt.Sprintf("guy:secret@tcp(%s)/quanta", listener.Addr().String()))
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var orderKey int64
	err = db.QueryRowContext(clientCtx, "select o_orderkey from orders").Scan(&orderKey)
	if err == nil {
		t.Fatalf("query unexpectedly succeeded with order_key=%d", orderKey)
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("query err = %T %v, want MySQLError", err, err)
	}
	if mysqlErr.Number != 1142 || !strings.Contains(strings.ToLower(mysqlErr.Message), "access denied") {
		t.Fatalf("mysql err = %#v, want access denied 1142", mysqlErr)
	}
	if executed.Load() {
		t.Fatal("runtime executed a query that should have been denied by access policy")
	}
}

type nativeProxyTestListener struct {
	conns  chan net.Conn
	closed chan struct{}
}

func newNativeProxyTestListener(conn net.Conn) *nativeProxyTestListener {
	listener := &nativeProxyTestListener{
		conns:  make(chan net.Conn, 1),
		closed: make(chan struct{}),
	}
	listener.conns <- conn
	return listener
}

func (l *nativeProxyTestListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *nativeProxyTestListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *nativeProxyTestListener) Addr() net.Addr {
	return nativeProxyTestAddr("native-proxy-test")
}

type nativeProxyTestAddr string

func (a nativeProxyTestAddr) Network() string { return "test" }

func (a nativeProxyTestAddr) String() string { return string(a) }

func nativeProxyHandshakeConnectionID(t *testing.T, packet qsmysql.Packet) uint32 {
	t.Helper()
	payload := packet.Payload
	if len(payload) < 6 || payload[0] != 0x0a {
		t.Fatalf("handshake payload = %#v", payload)
	}
	versionEnd := -1
	for i := 1; i < len(payload); i++ {
		if payload[i] == 0 {
			versionEnd = i
			break
		}
	}
	if versionEnd < 0 || versionEnd+5 > len(payload) {
		t.Fatalf("handshake payload missing connection id: %#v", payload)
	}
	offset := versionEnd + 1
	return uint32(payload[offset]) |
		uint32(payload[offset+1])<<8 |
		uint32(payload[offset+2])<<16 |
		uint32(payload[offset+3])<<24
}
