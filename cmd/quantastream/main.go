package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/version"
	"gopkg.in/yaml.v2"
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
	walPath := flags.String("wal-path", envString("QUANTASTREAM_WAL_PATH", ""), "optional local write-ahead log path; disabled when empty")
	database := flags.String("database", "quanta", "default database/schema name")
	authMode := flags.String("auth-mode", envString("QUANTASTREAM_AUTH_MODE", qsmysql.AuthModePermissive), "MySQL auth mode: permissive or static")
	authUser := flags.String("auth-user", envString("QUANTASTREAM_AUTH_USER", ""), "static MySQL auth username; defaults to qstream when auth-mode=static")
	authPassword := flags.String("auth-password", envString("QUANTASTREAM_AUTH_PASSWORD", ""), "static MySQL auth password; prefer QUANTASTREAM_AUTH_PASSWORD for scripts")
	authAccountFile := flags.String("auth-account-file", envString("QUANTASTREAM_AUTH_ACCOUNT_FILE", ""), "YAML static auth account file; used when auth-mode=static")
	accessPolicyFile := flags.String("access-policy-file", envString("QUANTASTREAM_ACCESS_POLICY_FILE", ""), "YAML static SQL access policy file; empty leaves SQL authorization permissive")
	runtimeProbes := flags.Bool("runtime-probes", envBool("QUANTASTREAM_RUNTIME_PROBES"), "log runtime execution probes after each query")
	mysqlCommandTrace := flags.Bool("mysql-command-trace", envBool("QUANTASTREAM_MYSQL_COMMAND_TRACE"), "log decoded MySQL commands and responses for client compatibility capture")
	pprofBind := flags.String("pprof-bind", "", "optional pprof listen address, for example 127.0.0.1:6060")
	statusOnly := flags.Bool("status", false, "print startup readiness and exit successfully")
	mountLocalNode := flags.Bool("mount-local-node", false, "construct the in-process local node backend before reporting status; regular startup always mounts it")
	printBSIPKAuthorityManifest := flags.Bool("print-bsi-pk-authority-manifest", false, "print the logical BSI primary-key authority manifest for the mounted standard catalog and exit")
	writeBSIPKAuthorityManifest := flags.Bool("write-bsi-pk-authority-manifest", false, "write the logical BSI primary-key authority manifest for the mounted standard catalog and exit")
	showVersion := flags.Bool("version", false, "print QuantaStream version and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.Summary())
		return 0
	}
	if *mode != qsinabox.StandardMode {
		fmt.Fprintf(stderr, "unsupported mode %q; only %q is wired in this skeleton\n", *mode, qsinabox.StandardMode)
		return 2
	}

	config := qsinabox.StandardConfig{
		BindAddress:              *bindAddress,
		MySQLPort:                *mysqlPort,
		NativeGRPCBind:           *nativeGRPCBind,
		NativeGRPCPort:           *nativeGRPCPort,
		ConfigDir:                *configDir,
		DataDir:                  *dataDir,
		WriteAheadLogPath:        *walPath,
		Database:                 *database,
		AuthMode:                 *authMode,
		AuthUser:                 *authUser,
		AuthPassword:             *authPassword,
		AuthAccountFile:          *authAccountFile,
		AccessPolicyFile:         *accessPolicyFile,
		RuntimeProbeLogging:      *runtimeProbes,
		MySQLCommandTraceLogging: *mysqlCommandTrace,
	}
	if _, err := config.MySQLAuthenticator(); err != nil {
		fmt.Fprintf(stderr, "configure mysql auth: %v\n", err)
		return 2
	}
	if _, err := config.AccessAuthorizer(); err != nil {
		fmt.Fprintf(stderr, "configure access policy: %v\n", err)
		return 2
	}

	if *printBSIPKAuthorityManifest && *writeBSIPKAuthorityManifest {
		fmt.Fprintln(stderr, "-print-bsi-pk-authority-manifest and -write-bsi-pk-authority-manifest cannot both be set")
		return 2
	}

	if *printBSIPKAuthorityManifest || *writeBSIPKAuthorityManifest {
		manifest, err := qsinabox.BuildStandardBSIPrimaryKeyAuthorityManifest(config, "quantastream-cli")
		if err != nil {
			fmt.Fprintf(stderr, "build BSI primary-key authority manifest: %v\n", err)
			return 2
		}
		if *writeBSIPKAuthorityManifest {
			if err := qsinabox.SaveStandardBSIPrimaryKeyAuthorityManifest(config, manifest); err != nil {
				fmt.Fprintf(stderr, "write BSI primary-key authority manifest: %v\n", err)
				return 2
			}
			fmt.Fprintf(stdout, "bsi_pk_authority_manifest_written=%s\n", qsinabox.StandardBSIPrimaryKeyAuthorityManifestPath(config))
			fmt.Fprintf(stdout, "bsi_pk_authority_manifest_entries=%d\n", len(manifest.Entries))
			return 0
		}
		data, err := yaml.Marshal(manifest)
		if err != nil {
			fmt.Fprintf(stderr, "marshal BSI primary-key authority manifest: %v\n", err)
			return 2
		}
		fmt.Fprint(stdout, string(data))
		return 0
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
		plan := qsinabox.NewObservedStandardPlan(config, services)
		for _, line := range plan.SummaryLines() {
			fmt.Fprintln(stdout, line)
		}
		return 0
	}

	startPprofServer(*pprofBind, stderr)

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
	plan := qsinabox.NewObservedStandardPlan(config, process.Backend.Services)
	for _, line := range plan.SummaryLines() {
		fmt.Fprintln(stdout, line)
	}
	if process.RuntimeMount.WriteAheadLog != nil {
		recovery := process.RuntimeMount.WriteAheadLogRecovery
		fmt.Fprintf(stdout, "wal_startup_torn_tail_bytes=%d\n", recovery.TornTailBytes)
		if recovery.TornTailLine != 0 {
			fmt.Fprintf(stdout, "wal_startup_torn_tail_line=%d\n", recovery.TornTailLine)
		}
		replay := process.RuntimeMount.WriteAheadLogReplay
		fmt.Fprintf(stdout, "wal_startup_replayed_records=%d\n", replay.ReplayRecordCount)
		fmt.Fprintf(stdout, "wal_startup_replayed_put_rows=%d\n", replay.PutRowCount)
		fmt.Fprintf(stdout, "wal_startup_replayed_update_rows=%d\n", replay.UpdateRowCount)
		fmt.Fprintf(stdout, "wal_startup_replayed_commit_boundaries=%d\n", replay.CommitBoundaryCount)
		fmt.Fprintf(stdout, "wal_startup_pending_records=%d\n", replay.PendingRecordCount)
		fmt.Fprintf(stdout, "wal_startup_checkpoint_lsn=%d\n", replay.CheckpointLSN)
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

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func startPprofServer(bind string, stderr io.Writer) {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		return
	}
	go func() {
		fmt.Fprintf(stderr, "pprof_listening=%s\n", bind)
		if err := http.ListenAndServe(bind, http.DefaultServeMux); err != nil {
			fmt.Fprintf(stderr, "pprof stopped: %v\n", err)
		}
	}()
}
