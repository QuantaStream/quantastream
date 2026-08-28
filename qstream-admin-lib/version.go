package admin

import (
	"fmt"

	"github.com/QuantaStream/quantastream/version"
)

// Context - Global command line variables
type Context struct {
	ConsulAddr string `help:"Consul agent address/port." default:"127.0.0.1:8500"`
	Port       int    `help:"Port number for QuantaStream service." default:"4000"`
	Debug      bool   `help:"Print Debug messages."`
}

// Variables to identify the build
var (
	Version = version.Version
	Build   = version.BuildString()
)

// VersionCmd - Version command
type VersionCmd struct {
}

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *Context) error {

	if Version == "" {
		Version = version.Version
	}
	if Build == "" {
		Build = version.BuildString()
	}
	fmt.Printf("Version: %s\n  Build: %s\n", Version, Build)
	return nil
}
