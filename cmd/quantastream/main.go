package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/shared"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runWithContext(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithContext(context.Background(), args, stdout, stderr)
}

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("quantastream", flag.ContinueOnError)
	flags.SetOutput(stderr)

	mode := flags.String("mode", qsinabox.StandardMode, "deployment mode to start")
	bindAddress := flags.String("bind", "127.0.0.1", "MySQL bind address")
	mysqlPort := flags.Int("mysql-port", 4000, "MySQL listen port")
	nativeGRPCBind := flags.String("native-grpc-bind", "", "native node gRPC bind address; defaults to MySQL bind address")
	nativeGRPCPort := flags.Int("native-grpc-port", 0, "native node gRPC listen port for high-throughput loaders; disabled when zero")
	configDir := flags.String("config-dir", "configuration", "schema/catalog configuration directory")
	dataDir := flags.String("data-dir", "data", "local data directory")
	database := flags.String("database", "quanta", "default database/schema name")
	runtimeProbes := flags.Bool("runtime-probes", envBool("QUANTASTREAM_RUNTIME_PROBES"), "log runtime execution probes after each query")
	statusOnly := flags.Bool("status", false, "print startup readiness and exit successfully")
	mountLocalNode := flags.Bool("mount-local-node", false, "construct the in-process local node backend before reporting status; regular startup always mounts it")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *mode != qsinabox.StandardMode {
		fmt.Fprintf(stderr, "unsupported mode %q; only %q is wired in this skeleton\n", *mode, qsinabox.StandardMode)
		return 2
	}

	config := qsinabox.StandardConfig{
		BindAddress:         *bindAddress,
		MySQLPort:           *mysqlPort,
		NativeGRPCBind:      *nativeGRPCBind,
		NativeGRPCPort:      *nativeGRPCPort,
		ConfigDir:           *configDir,
		DataDir:             *dataDir,
		Database:            *database,
		RuntimeProbeLogging: *runtimeProbes,
	}

	if *statusOnly {
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
		return 0
	}

	mountStart := time.Now()
	process, diagnostics, err := qsinabox.MountStandardProcess(ctx, config)
	mountElapsed := time.Since(mountStart)
	if err != nil {
		fmt.Fprintf(stderr, "mount inabox-standard process: %v\n", err)
		return 2
	}
	if diagnostics.BlocksNative() {
		fmt.Fprintf(stderr, "mount inabox-standard process: %s\n", diagnosticMessages(diagnostics))
		return 2
	}
	defer process.Close()

	fmt.Fprintf(stdout, "mount_elapsed=%s\n", mountElapsed)
	plan := qsinabox.NewStandardPlan(config, process.Backend.Services)
	for _, line := range plan.SummaryLines() {
		fmt.Fprintln(stdout, line)
	}
	if !plan.Ready {
		fmt.Fprintln(stderr, "inabox-standard runtime is not ready; rerun with -status to inspect the current skeleton")
		return 2
	}
	fmt.Fprintf(stdout, "listening=%s\n", config.WithDefaults().Address())
	if process.NativeNode != nil {
		fmt.Fprintf(stdout, "native_grpc_listening=%s\n", process.NativeNode.Address)
	}
	if err := process.ListenAndServe(ctx); err != nil {
		fmt.Fprintf(stderr, "serve inabox-standard: %v\n", err)
		return 1
	}
	return 0
}

func diagnosticMessages(diagnostics qsbridge.DiagnosticSet) string {
	if len(diagnostics) == 0 {
		return ""
	}
	messages := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		messages = append(messages, fmt.Sprintf("%s: %s", diagnostic.Code, diagnostic.Message))
	}
	return fmt.Sprint(messages)
}

func envBool(name string) bool {
	value := os.Getenv(name)
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}
