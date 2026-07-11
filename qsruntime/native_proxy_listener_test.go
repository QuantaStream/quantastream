package qsruntime

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsmysql"
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
		errCh <- frontDoor.ListenAndServe(ctx, NativeProxyListenConfig{Listener: listener, EnableAcceptLoop: true})
	}()

	client := qsmysql.NewStream(clientConn, clientConn)
	handshake, err := client.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("read handshake failed: %v", err)
	}
	if handshake.SequenceID != 0 || len(handshake.Payload) == 0 {
		t.Fatalf("handshake = %#v", handshake)
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
