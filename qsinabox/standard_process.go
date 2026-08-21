package qsinabox

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
)

// StandardProcess owns the composed inabox-standard local backend, SQL runtime,
// and MySQL front door.
type StandardProcess struct {
	Config       StandardConfig
	Backend      StandardLocalBackend
	RuntimeMount StandardDirectRuntimeMount
	TableCache   *core.TableCacheStruct
	FrontDoor    qsruntime.NativeProxyFrontDoor
	NativeNode   *StandardNativeNodeServer

	closeOnce sync.Once
}

// MountStandardProcess constructs the full inabox-standard server composition.
func MountStandardProcess(ctx context.Context, config StandardConfig) (StandardProcess, qsbridge.DiagnosticSet, error) {
	config = config.WithDefaults()
	if _, err := config.MySQLAuthenticator(); err != nil {
		return StandardProcess{}, nil, fmt.Errorf("configure inabox-standard MySQL auth: %w", err)
	}
	if _, err := config.AccessAuthorizer(); err != nil {
		return StandardProcess{}, nil, fmt.Errorf("configure inabox-standard access policy: %w", err)
	}
	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		return StandardProcess{}, nil, err
	}
	tableCache := core.NewTableCacheStruct()
	runtimeMount := backend.NewDirectRuntime(config, tableCache, 0)
	resolverFactory := standardDirectPrimaryKeyResolverFactory(config, tableCache, backend.Adapter.BitmapIndex, runtimeMount.Pool)
	if err := runtimeMount.EnableWriteAheadLog(ctx, config.WriteAheadLogPath, resolverFactory); err != nil {
		runtimeMount.Close()
		backend.Close()
		return StandardProcess{}, nil, fmt.Errorf("enable inabox-standard WAL: %w", err)
	}
	if runtimeMount.WriteAheadLogReplay.PutRowCount != 0 || runtimeMount.WriteAheadLogReplay.UpdateRowCount != 0 {
		manifest, err := BuildStandardBSIPrimaryKeyAuthorityManifest(config, "standard-wal-replay")
		if err != nil {
			runtimeMount.Close()
			backend.Close()
			return StandardProcess{}, nil, fmt.Errorf("refresh BSI primary-key authority manifest after WAL replay: %w", err)
		}
		if err := standardPopulateBSIPrimaryKeyAuthorityArtifactFileCounts(config, &manifest); err != nil {
			runtimeMount.Close()
			backend.Close()
			return StandardProcess{}, nil, fmt.Errorf("refresh BSI primary-key authority artifact counts after WAL replay: %w", err)
		}
		if err := SaveStandardBSIPrimaryKeyAuthorityManifest(config, manifest); err != nil {
			runtimeMount.Close()
			backend.Close()
			return StandardProcess{}, nil, fmt.Errorf("save BSI primary-key authority manifest after WAL replay: %w", err)
		}
	}
	nativeRuntime, diagnostics, err := buildStandardNativeProxyRuntime(ctx, config, backend, tableCache, runtimeMount.Runtime)
	if err != nil || diagnostics.BlocksNative() {
		runtimeMount.Close()
		backend.Close()
		return StandardProcess{}, diagnostics, err
	}
	nativeNode, err := MountStandardNativeNodeServer(config, backend)
	if err != nil {
		runtimeMount.Close()
		backend.Close()
		return StandardProcess{}, diagnostics, err
	}
	frontDoorConfig := config.NativeProxyFrontDoorConfig()
	frontDoorConfig.Server.SessionActionHandler = StandardSessionActionHandler{
		Config:  config,
		Backend: backend,
	}
	frontDoor := qsruntime.NewNativeProxyFrontDoor(nativeRuntime, frontDoorConfig)
	return StandardProcess{
		Config:       config,
		Backend:      backend,
		RuntimeMount: runtimeMount,
		TableCache:   tableCache,
		FrontDoor:    frontDoor,
		NativeNode:   nativeNode,
	}, nil, nil
}

// Close shuts down runtime and local node resources owned by the process.
func (p *StandardProcess) Close() {
	p.closeOnce.Do(func() {
		if p.NativeNode != nil {
			p.NativeNode.Close()
		}
		p.RuntimeMount.Close()
		p.Backend.Close()
	})
}

// ListenAndServe accepts MySQL clients through the mounted native front door.
func (p StandardProcess) ListenAndServe(ctx context.Context) error {
	if !p.FrontDoor.Ready() {
		return fmt.Errorf("inabox-standard front door is not ready")
	}
	if p.NativeNode != nil {
		p.NativeNode.Start(ctx)
	}
	return p.FrontDoor.ListenAndServe(ctx, qsruntime.NativeProxyListenConfig{
		Address:          p.Config.WithDefaults().Address(),
		EnableAcceptLoop: true,
	})
}

func buildStandardNativeProxyRuntime(ctx context.Context, config StandardConfig, backend StandardLocalBackend, tableCache *core.TableCacheStruct, direct qsruntime.DirectBitmapRuntime) (qsruntime.NativeProxyRuntime, qsbridge.DiagnosticSet, error) {
	config = config.WithDefaults()
	directConfig := qsruntime.NewDirectRuntimeConfig(backend.ConfigBaseDir(config), "", 0, 0)
	dictionaryResolver := qsruntime.LegacyTableCacheDictionaryResolver{
		TableCache: tableCache,
		Schema:     config.Database,
	}
	runtime, diagnostics, err := qsruntime.SQLRuntimeBuilder{
		Parser: qsbridge.SimpleParserBridge{},
		Lowerer: qsbridge.QuantaIntermediateLowerer{
			Dictionaries: dictionaryResolver,
		},
		DefaultSchema:  config.Database,
		CatalogVersion: qsbridge.CatalogVersion("inabox-standard-local"),
		EnvironmentBuilder: qsruntime.RuntimeEnvironmentBuilder{
			Config: directConfig,
			Profile: qsruntime.RuntimeInspectionProfile{
				Implementation: "inabox-standard",
				Detail:         "local-node/in-process",
			},
			CatalogFactory: qsruntime.LegacyTableCacheCatalogFactory{
				TableCache: tableCache,
				LoadTable:  standardCatalogTableLoader(backend, config),
				Functions:  qsbridge.BuiltinSQLFunctionDefinitions(),
			},
			DirectFactory: qsruntime.DirectRuntimeFactoryFunc(func(context.Context, qsruntime.DirectRuntimeConfig) (qsruntime.DirectRuntime, qsbridge.DiagnosticSet, error) {
				return direct, nil, nil
			}),
		},
		StorageMutationGuard:    standardStorageMutationGuard(config),
		EnableFilterExpressions: true,
	}.Build(ctx)
	if err != nil || diagnostics.BlocksNative() {
		return qsruntime.NativeProxyRuntime{}, diagnostics, err
	}
	return qsruntime.NativeProxyRuntime{Runtime: runtime}, nil, nil
}

func standardStorageMutationGuard(config StandardConfig) qsruntime.StorageMutationGuard {
	config = config.WithDefaults()
	return func(ctx context.Context, operation string) qsbridge.DiagnosticSet {
		if err := ctx.Err(); err != nil {
			return qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, err.Error()),
			}
		}
		if err := core.RejectLocalStorageMutationIfQuiesced(config.DataDir, operation); err != nil {
			return qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, err.Error()),
			}
		}
		return nil
	}
}

func standardCatalogTableLoader(backend StandardLocalBackend, config StandardConfig) qsruntime.LegacyTableLoader {
	baseDir := backend.ConfigBaseDir(config)
	return func(tableCache *core.TableCacheStruct, name string) (*core.Table, error) {
		session, err := core.OpenSession(tableCache, baseDir, name, false, backend.NewLocalConnection())
		if err != nil {
			return nil, err
		}
		if session != nil {
			defer session.CloseSession()
		}
		return standardCachedTable(tableCache, name), nil
	}
}

func standardCachedTable(tableCache *core.TableCacheStruct, name string) *core.Table {
	if tableCache == nil {
		return nil
	}
	tableCache.TableCacheLock.RLock()
	defer tableCache.TableCacheLock.RUnlock()
	for tableName, table := range tableCache.TableCache {
		if strings.EqualFold(tableName, name) || (table != nil && strings.EqualFold(table.Name, name)) {
			return table
		}
	}
	return nil
}
