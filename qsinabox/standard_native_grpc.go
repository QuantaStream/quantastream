package qsinabox

import (
	"context"
	"fmt"
	"net"
	"sync"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/server"
	"github.com/QuantaStream/quantastream/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// StandardNativeNodeServer publishes the standard-mode node services on a
// regular gRPC listener for high-throughput loaders.
type StandardNativeNodeServer struct {
	Address  string
	server   *grpc.Server
	listener net.Listener
	done     chan error

	startOnce sync.Once
	closeOnce sync.Once
}

// MountStandardNativeNodeServer prepares the native node listener when enabled.
func MountStandardNativeNodeServer(config StandardConfig, backend StandardLocalBackend) (*StandardNativeNodeServer, error) {
	config = config.WithDefaults()
	if !config.NativeGRPCEnabled() {
		return nil, nil
	}
	if backend.Adapter.BitmapIndex == nil {
		return nil, fmt.Errorf("inabox-standard native gRPC requires BitmapIndex service")
	}
	if backend.Adapter.KVStore == nil {
		return nil, fmt.Errorf("inabox-standard native gRPC requires KVStore service")
	}
	address := config.NativeGRPCAddress()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen native gRPC %s: %w", address, err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(shared.GRPCRecvBufsize),
		grpc.MaxSendMsgSize(shared.GRPCSendBufsize),
	)
	pb.RegisterBitmapIndexServer(grpcServer, backend.Adapter.BitmapIndex)
	pb.RegisterKVStoreServer(grpcServer, backend.Adapter.KVStore)
	if backend.Adapter.StringSearch != nil {
		pb.RegisterStringSearchServer(grpcServer, backend.Adapter.StringSearch)
	}
	grpc_health_v1.RegisterHealthServer(grpcServer, &server.HealthImpl{})
	return &StandardNativeNodeServer{
		Address:  listener.Addr().String(),
		server:   grpcServer,
		listener: listener,
		done:     make(chan error, 1),
	}, nil
}

// Start begins serving native node gRPC requests until the context is canceled
// or Close is called.
func (s *StandardNativeNodeServer) Start(ctx context.Context) <-chan error {
	if s == nil {
		done := make(chan error, 1)
		close(done)
		return done
	}
	s.startOnce.Do(func() {
		go func() {
			err := s.server.Serve(s.listener)
			if err == grpc.ErrServerStopped {
				err = nil
			}
			s.done <- err
			close(s.done)
		}()
		go func() {
			<-ctx.Done()
			s.Close()
		}()
	})
	return s.done
}

// Close stops the native gRPC listener.
func (s *StandardNativeNodeServer) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.server.GracefulStop()
		_ = s.listener.Close()
	})
}
