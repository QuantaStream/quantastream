package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/QuantaStream/quantastream/qsloader"
	"github.com/QuantaStream/quantastream/shared"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:]))
}

func run(ctx context.Context, args []string) int {
	flags := flag.NewFlagSet("quantastream-loader", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	listen := flags.String("listen", "127.0.0.1:8088", "HTTP listen address for loader protocol adapters")
	configDir := flags.String("config-dir", "configuration", "schema/catalog configuration directory")
	database := flags.String("database", "quanta", "default database/schema name")
	tables := flags.String("tables", "", "comma-separated selector table allowlist; defaults to active/discovered catalog tables")
	connectionMode := flags.String("connection-mode", string(shared.LoaderConnectionStandardNative), "engine connection mode: standard-native or distributed")
	nativeGRPCAddr := flags.String("native-grpc-addr", "127.0.0.1:4100", "native gRPC endpoint for standard-native mode")
	consulAddr := flags.String("consul-addr", "127.0.0.1:8500", "Consul endpoint for distributed mode")
	workers := flags.Int("workers", 1, "session router worker count")
	channelSize := flags.Int("channel-size", 100000, "session router channel size")
	flushInterval := flags.Duration("flush-interval", time.Second, "session router idle flush interval")
	defaultSource := flags.String("default-source", "json-http", "source value used when a JSON event omits source")
	physicalBuildRouting := flags.Bool("physical-build-routing", false, "route by physical time-quantum build shard when safe for the source shape")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	mode := shared.LoaderConnectionMode(strings.TrimSpace(*connectionMode))
	switch mode {
	case shared.LoaderConnectionStandardNative, shared.LoaderConnectionDistributed:
	default:
		fmt.Fprintf(os.Stderr, "unsupported -connection-mode %q\n", *connectionMode)
		return 2
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	loader, err := qsloader.NewServer(ctx, qsloader.Config{
		ListenAddress:        *listen,
		ConfigDir:            *configDir,
		Database:             *database,
		Tables:               splitCSV(*tables),
		ConnectionMode:       mode,
		NativeGRPCAddr:       *nativeGRPCAddr,
		ConsulAddr:           *consulAddr,
		Workers:              *workers,
		ChannelSize:          *channelSize,
		FlushInterval:        *flushInterval,
		DefaultSource:        *defaultSource,
		PhysicalBuildRouting: *physicalBuildRouting,
	}, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start quantastream-loader: %v\n", err)
		return 2
	}
	defer loader.Close()

	server := &http.Server{
		Addr:              *listen,
		Handler:           loader.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Printf("quantastream-loader listening=%s connection_mode=%s", *listen, mode)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown quantastream-loader: %v\n", err)
			return 1
		}
		return 0
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "serve quantastream-loader: %v\n", err)
			return 1
		}
		return 0
	}
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	raw := strings.Split(value, ",")
	items := make([]string, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}
