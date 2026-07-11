package qsruntime

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// ErrNativeProxyListenerNotReady marks the TCP listener as an intentionally guarded scaffold.
var ErrNativeProxyListenerNotReady = errors.New("native proxy TCP listener scaffold is not enabled")

// NativeProxyListenConfig names the future TCP mounting point and accept-loop guard.
type NativeProxyListenConfig struct {
	Address          string
	Listener         net.Listener
	Options          qsbridge.ExecutionOptions
	EnableAcceptLoop bool
}

// WithDefaults fills listener configuration from the front-door bootstrap settings.
func (c NativeProxyListenConfig) WithDefaults(frontDoor NativeProxyFrontDoor) NativeProxyListenConfig {
	if c.Address == "" {
		c.Address = fmt.Sprintf("%s:%d", frontDoor.BindAddress, frontDoor.Port)
	}
	return c
}

// ListenAndServe accepts MySQL clients when the accept loop is explicitly enabled.
func (f NativeProxyFrontDoor) ListenAndServe(ctx context.Context, config NativeProxyListenConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config = config.WithDefaults(f)
	if !config.EnableAcceptLoop || !f.WireReady() {
		return ErrNativeProxyListenerNotReady
	}
	listener := config.Listener
	ownsListener := false
	if listener == nil {
		if config.Address == "" {
			return fmt.Errorf("native proxy listener requires an address or listener")
		}
		var err error
		listener, err = net.Listen("tcp", config.Address)
		if err != nil {
			return err
		}
		ownsListener = true
	}
	if ownsListener {
		defer listener.Close()
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stop:
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go f.serveMySQLConn(ctx, conn, config.Options)
	}
}

func (f NativeProxyFrontDoor) serveMySQLConn(ctx context.Context, conn net.Conn, options qsbridge.ExecutionOptions) {
	defer conn.Close()
	_ = f.ServeMySQLSession(ctx, qsmysql.NewStream(conn, conn), options)
}
