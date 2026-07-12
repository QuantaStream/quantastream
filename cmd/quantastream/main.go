package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/shared"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("quantastream", flag.ContinueOnError)
	flags.SetOutput(stderr)

	mode := flags.String("mode", qsinabox.StandardMode, "deployment mode to start")
	bindAddress := flags.String("bind", "127.0.0.1", "MySQL bind address")
	mysqlPort := flags.Int("mysql-port", 4000, "MySQL listen port")
	configDir := flags.String("config-dir", "configuration", "schema/catalog configuration directory")
	dataDir := flags.String("data-dir", "data", "local data directory")
	database := flags.String("database", "quanta", "default database/schema name")
	statusOnly := flags.Bool("status", false, "print startup readiness and exit successfully")
	mountLocalNode := flags.Bool("mount-local-node", false, "construct the in-process local node backend before reporting status")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *mode != qsinabox.StandardMode {
		fmt.Fprintf(stderr, "unsupported mode %q; only %q is wired in this skeleton\n", *mode, qsinabox.StandardMode)
		return 2
	}

	config := qsinabox.StandardConfig{
		BindAddress: *bindAddress,
		MySQLPort:   *mysqlPort,
		ConfigDir:   *configDir,
		DataDir:     *dataDir,
		Database:    *database,
	}
	services := shared.LocalNodeServices{}
	if *mountLocalNode {
		backend, err := qsinabox.MountStandardLocalBackend(config, nil)
		if err != nil {
			fmt.Fprintf(stderr, "mount inabox-standard local backend: %v\n", err)
			return 2
		}
		defer backend.Close()
		services = backend.Services
	}
	plan := qsinabox.NewStandardPlan(config, services)
	for _, line := range plan.SummaryLines() {
		fmt.Fprintln(stdout, line)
	}
	if *statusOnly {
		return 0
	}
	if !plan.Ready {
		fmt.Fprintln(stderr, "inabox-standard runtime is not ready; rerun with -status to inspect the current skeleton")
		return 2
	}
	return 0
}
