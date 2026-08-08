package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/QuantaStream/quantastream/qsloader"
	"github.com/QuantaStream/quantastream/shared"
)

const loaderShutdownTimeout = 5 * time.Second

type loaderCloseable interface {
	Close() error
}

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
	pprofBind := flags.String("pprof-bind", "", "optional pprof listen address, for example 127.0.0.1:6061")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)
	mode := shared.LoaderConnectionMode(strings.TrimSpace(*connectionMode))
	switch mode {
	case shared.LoaderConnectionStandardNative, shared.LoaderConnectionDistributed:
	default:
		fmt.Fprintf(os.Stderr, "unsupported -connection-mode %q\n", *connectionMode)
		return 2
	}
	startPprofServer(*pprofBind, logger)

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
	defer func() {
		if loader != nil {
			if err := closeLoaderWithin(loader, loaderShutdownTimeout); err != nil {
				logger.Printf("quantastream-loader close: %v", err)
			}
		}
	}()

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
		logger.Printf("quantastream-loader shutdown requested")
		if err := server.Close(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "close quantastream-loader http server: %v\n", err)
			return 1
		}
		if err := closeLoaderWithin(loader, loaderShutdownTimeout); err != nil {
			logger.Printf("quantastream-loader close: %v", err)
		}
		loader = nil
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

func startPprofServer(bind string, logger *log.Logger) {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		return
	}
	go func() {
		if logger != nil {
			logger.Printf("quantastream-loader pprof listening=%s", bind)
		}
		if err := http.ListenAndServe(bind, http.DefaultServeMux); err != nil {
			if logger != nil {
				logger.Printf("quantastream-loader pprof stopped: %v", err)
			}
		}
	}()
}

func closeLoaderWithin(loader loaderCloseable, timeout time.Duration) error {
	if loader == nil {
		return nil
	}
	if timeout <= 0 {
		return loader.Close()
	}
	done := make(chan error, 1)
	go func() {
		done <- loader.Close()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("timed out after %s draining loader sessions", timeout)
	}
}
