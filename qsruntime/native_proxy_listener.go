package qsruntime

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// ErrNativeProxyListenerNotReady marks the TCP listener as an intentionally disabled scaffold.
var ErrNativeProxyListenerNotReady = errors.New("native proxy TCP listener scaffold is not production-ready")

// NativeProxyListenConfig names the future TCP mounting point without owning the accept loop yet.
type NativeProxyListenConfig struct {
	Address  string
	Listener net.Listener
}

// WithDefaults fills listener configuration from the front-door bootstrap settings.
func (c NativeProxyListenConfig) WithDefaults(frontDoor NativeProxyFrontDoor) NativeProxyListenConfig {
	if c.Address == "" {
		c.Address = fmt.Sprintf("%s:%d", frontDoor.BindAddress, frontDoor.Port)
	}
	return c
}

// ListenAndServe is a deliberately disabled scaffold for the future MySQL TCP accept loop.
func (f NativeProxyFrontDoor) ListenAndServe(ctx context.Context, config NativeProxyListenConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config = config.WithDefaults(f)
	if config.Address == "" && config.Listener == nil {
		return fmt.Errorf("native proxy listener requires an address or listener")
	}
	return ErrNativeProxyListenerNotReady
}
