package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// NativeProxyMySQLSessionConfig captures per-connection MySQL session wiring.
type NativeProxyMySQLSessionConfig struct {
	Options        qsbridge.ExecutionOptions
	ConnectionID   uint32
	AuthPluginData []byte
}

// WithDefaults returns deterministic session defaults for tests and local QIAB.
func (c NativeProxyMySQLSessionConfig) WithDefaults() NativeProxyMySQLSessionConfig {
	if c.ConnectionID == 0 {
		c.ConnectionID = 1
	}
	if len(c.AuthPluginData) == 0 {
		c.AuthPluginData = []byte("quantastream-auth-v1")
	}
	return c
}

// ServeMySQLSession runs one MySQL protocol session over an already-mounted packet stream.
func (f NativeProxyFrontDoor) ServeMySQLSession(ctx context.Context, stream qsmysql.PacketReadWriter, options qsbridge.ExecutionOptions) error {
	return f.ServeMySQLSessionWithConfig(ctx, stream, NativeProxyMySQLSessionConfig{Options: options})
}

// ServeMySQLSessionWithConfig runs one MySQL protocol session with explicit per-connection metadata.
func (f NativeProxyFrontDoor) ServeMySQLSessionWithConfig(ctx context.Context, stream qsmysql.PacketReadWriter, config NativeProxyMySQLSessionConfig) error {
	config = config.WithDefaults()
	runner := qsmysql.NewSessionRunner(qsmysql.SessionRunnerConfig{
		ConnectionID:   config.ConnectionID,
		AuthPluginData: config.AuthPluginData,
		Stream:         stream,
		Handler:        NativeProxyMySQLCommandHandler{FrontDoor: f, Options: config.Options, Profile: NewNativeProxyMySQLSessionProfile()},
		Authenticator:  f.Authenticator,
		CommandLogger:  f.MySQLCommandLogger,
	})
	return runner.Serve(ctx)
}
