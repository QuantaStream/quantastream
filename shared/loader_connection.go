package shared

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/consul/api"
)

// LoaderConnectionMode identifies the topology a loader should use to reach
// the engine write surface.
type LoaderConnectionMode string

const (
	// LoaderConnectionStandardNative connects to the single-node native gRPC
	// surface exposed by inabox-standard.
	LoaderConnectionStandardNative LoaderConnectionMode = "standard-native"
	// LoaderConnectionDistributed connects through the existing Consul-backed
	// distributed node client.
	LoaderConnectionDistributed LoaderConnectionMode = "distributed"
)

// LoaderConnectionConfig describes the node connection a loader needs before it
// builds sessions and BatchBuffers.
type LoaderConnectionConfig struct {
	Mode        LoaderConnectionMode
	Owner       string
	Address     string
	Consul      *api.Client
	ServiceName string
	ServicePort int
	Quorum      int
	Replicas    int
}

// NewLoaderConnection builds the topology-specific node connection used by
// high-throughput loaders.
func NewLoaderConnection(ctx context.Context, config LoaderConnectionConfig) (*Conn, error) {
	mode := config.Mode
	if mode == "" {
		mode = LoaderConnectionStandardNative
	}
	owner := strings.TrimSpace(config.Owner)
	if owner == "" {
		owner = "loader"
	}
	switch mode {
	case LoaderConnectionStandardNative:
		if strings.TrimSpace(config.Address) == "" {
			return nil, fmt.Errorf("standard-native loader connection requires address")
		}
		return NewSingleNodeConnection(ctx, owner, config.Address)
	case LoaderConnectionDistributed:
		if config.Consul == nil {
			return nil, fmt.Errorf("distributed loader connection requires Consul client")
		}
		conn := NewDefaultConnection(owner)
		applyLoaderDistributedConnectionConfig(conn, config)
		if err := conn.Connect(config.Consul); err != nil {
			return nil, err
		}
		return conn, nil
	default:
		return nil, fmt.Errorf("unsupported loader connection mode %q", mode)
	}
}

func applyLoaderDistributedConnectionConfig(conn *Conn, config LoaderConnectionConfig) {
	if strings.TrimSpace(config.ServiceName) != "" {
		conn.ServiceName = config.ServiceName
	}
	if config.ServicePort > 0 {
		conn.ServicePort = config.ServicePort
	}
	if config.Quorum > 0 {
		conn.Quorum = config.Quorum
	}
	if config.Replicas > 0 {
		conn.Replicas = config.Replicas
	}
}
