package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// ServeMySQLSession runs one MySQL protocol session over an already-mounted packet stream.
func (f NativeProxyFrontDoor) ServeMySQLSession(ctx context.Context, stream qsmysql.PacketReadWriter, options qsbridge.ExecutionOptions) error {
	runner := qsmysql.NewSessionRunner(qsmysql.SessionRunnerConfig{
		ConnectionID:   1,
		AuthPluginData: []byte("quantastream-auth-seed"),
		Stream:         stream,
		Handler:        NativeProxyMySQLCommandHandler{FrontDoor: f, Options: options},
		Authenticator:  f.Authenticator,
	})
	return runner.Serve(ctx)
}
