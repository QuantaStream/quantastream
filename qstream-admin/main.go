// QStream admin CLI tool.
package main

import (
	admin "github.com/QuantaStream/quantastream/qstream-admin-lib"
	"github.com/alecthomas/kong"
)

func main() {

	ctx := kong.Parse(&admin.Cli)
	err := ctx.Run(&admin.Context{ConsulAddr: admin.Cli.ConsulAddr, Port: admin.Cli.Port, Debug: admin.Cli.Debug})
	ctx.FatalIfErrorf(err)
}
