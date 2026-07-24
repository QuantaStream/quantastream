package qsinabox

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/QuantaStream/quantastream/server"
	"github.com/QuantaStream/quantastream/shared"
)

const standardLocalNodeID = "quantastream-inabox-standard"

// StandardLocalBackend is the in-process node side of inabox-standard.
type StandardLocalBackend struct {
	Node     *server.Node
	Adapter  server.LocalNodeAdapter
	Services shared.LocalNodeServices

	closeOnce sync.Once
}

// MountStandardLocalBackend constructs and initializes the first in-process
// node backend for inabox-standard. It mounts the local bitmap/KV surfaces
// needed for the current read, DDL, and insert path; searchable text and broad
// warmup iteration remain explicit follow-up gates.
func MountStandardLocalBackend(config StandardConfig, observer shared.LocalNodeObserver) (StandardLocalBackend, error) {
	config = config.WithDefaults()
	if err := prepareStandardLocalStorage(config); err != nil {
		return StandardLocalBackend{}, err
	}

	node, err := server.NewNode("inabox-standard", 0, config.BindAddress, config.DataDir, standardLocalNodeID, nil)
	if err != nil {
		return StandardLocalBackend{}, err
	}
	node.ServiceName = "quantastream"
	node.IsLocalCluster = true
	node.Stop = make(chan bool)
	node.Err = make(chan error, 1)
	node.State = server.Active

	kvStore := server.NewKVStore(node)
	node.AddNodeService(kvStore)

	bitmapIndex := server.NewBitmapIndex(node)
	node.AddNodeService(bitmapIndex)

	if err := node.InitServices(); err != nil {
		node.ShutdownServices()
		return StandardLocalBackend{}, err
	}

	adapter := server.LocalNodeAdapter{
		BitmapIndex: bitmapIndex,
		KVStore:     kvStore,
		Observer:    observer,
	}
	return StandardLocalBackend{
		Node:     node,
		Adapter:  adapter,
		Services: adapter.Services(),
	}, nil
}

// Close shuts down the in-process node services.
func (b *StandardLocalBackend) Close() {
	b.closeOnce.Do(func() {
		if b.Node == nil {
			return
		}
		b.Node.ShutdownServices()
		if b.Node.Stop != nil {
			close(b.Node.Stop)
		}
		if bitmapIndex, ok := b.Node.GetNodeService("BitmapIndex").(*server.BitmapIndex); ok {
			bitmapIndex.WaitForShutdown()
		}
	})
}

func prepareStandardLocalStorage(config StandardConfig) error {
	config = config.WithDefaults()
	for _, dir := range []string{
		config.DataDir,
		filepath.Join(config.DataDir, "bitmap"),
		filepath.Join(config.DataDir, "index"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("prepare inabox-standard directory %s: %w", dir, err)
		}
	}

	dataConfigDir := filepath.Join(config.DataDir, "config")
	if info, err := os.Stat(dataConfigDir); err == nil && !info.IsDir() {
		return fmt.Errorf("inabox-standard config path %s is not a directory", dataConfigDir)
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("inspect inabox-standard config directory %s: %w", dataConfigDir, err)
	}

	sourceConfigDir, err := filepath.Abs(config.ConfigDir)
	if err != nil {
		return fmt.Errorf("resolve inabox-standard config directory %s: %w", config.ConfigDir, err)
	}
	if _, err := os.Stat(sourceConfigDir); err != nil {
		return fmt.Errorf("inspect inabox-standard config directory %s: %w", sourceConfigDir, err)
	}

	return copyStandardConfigDir(sourceConfigDir, dataConfigDir)
}

func copyStandardConfigDir(sourceDir, targetDir string) error {
	return filepath.WalkDir(sourceDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(targetDir, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	})
}
