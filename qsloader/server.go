package qsloader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/hashicorp/consul/api"
)

// Config describes the long-lived loader process.
type Config struct {
	ListenAddress        string
	ConfigDir            string
	Database             string
	Tables               []string
	ConnectionMode       shared.LoaderConnectionMode
	NativeGRPCAddr       string
	ConsulAddr           string
	Workers              int
	ChannelSize          int
	FlushInterval        time.Duration
	DefaultSource        string
	PhysicalBuildRouting bool
}

// Server owns the protocol adapters, table selectors, router, and engine
// connection for a quantastream-loader process.
type Server struct {
	config       Config
	tableCache   *core.TableCacheStruct
	tables       []*core.Table
	conn         *shared.Conn
	router       *core.SessionRouter
	jsonAdapter  JSONAdapter
	putProfile   *core.RouterPutRowProfile
	flushProfile *core.RouterFlushProfile
	drainProfile *core.RouterDrainProfile
	logger       *log.Logger
}

// IngestResponse is returned by protocol endpoints.
type IngestResponse struct {
	Accepted int                `json:"accepted"`
	Failed   int                `json:"failed"`
	Routes   []IngestRouteReply `json:"routes,omitempty"`
	Errors   []string           `json:"errors,omitempty"`
}

// IngestRouteReply exposes compact per-record routing results.
type IngestRouteReply struct {
	Index        int    `json:"index"`
	Table        string `json:"table"`
	EventID      string `json:"event_id,omitempty"`
	ShardMode    string `json:"shard_mode,omitempty"`
	BuildSharded bool   `json:"build_sharded,omitempty"`
}

// NewServer constructs a loader server and connects to the configured engine
// mutation surface.
func NewServer(ctx context.Context, config Config, logger *log.Logger) (*Server, error) {
	config = config.withDefaults()
	tableCache := core.NewTableCacheStruct()
	tables, err := loadSelectorTables(tableCache, config)
	if err != nil {
		return nil, err
	}
	conn, err := newLoaderConn(ctx, config)
	if err != nil {
		return nil, err
	}
	putProfile := &core.RouterPutRowProfile{}
	flushProfile := &core.RouterFlushProfile{}
	drainProfile := &core.RouterDrainProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:                tableCache,
		BasePath:                  config.ConfigDir,
		Conn:                      conn,
		ShardCount:                config.Workers,
		ChannelSize:               config.ChannelSize,
		FlushInterval:             config.FlushInterval,
		PrimaryKeyResolverFactory: qsinabox.NewSharedStandardSessionBSIPrimaryKeyResolverFactory(tableCache),
		OnPutRowResult:            putProfile.Callback(),
		OnFlushProfile:            flushProfile.Callback(),
		OnDrainProfile:            drainProfile.Callback(),
		OnError: func(err error) {
			if logger != nil {
				logger.Printf("loader route error: %v", err)
			}
		},
	})
	if err != nil {
		_ = conn.Disconnect()
		return nil, err
	}
	return &Server{
		config:       config,
		tableCache:   tableCache,
		tables:       tables,
		conn:         conn,
		router:       router,
		jsonAdapter:  JSONAdapter{DefaultSource: config.DefaultSource, Now: time.Now},
		putProfile:   putProfile,
		flushProfile: flushProfile,
		drainProfile: drainProfile,
		logger:       logger,
	}, nil
}

func (c Config) withDefaults() Config {
	if strings.TrimSpace(c.ListenAddress) == "" {
		c.ListenAddress = "127.0.0.1:8088"
	}
	if strings.TrimSpace(c.ConfigDir) == "" {
		c.ConfigDir = "configuration"
	}
	if strings.TrimSpace(c.Database) == "" {
		c.Database = "quanta"
	}
	if c.ConnectionMode == "" {
		c.ConnectionMode = shared.LoaderConnectionStandardNative
	}
	if strings.TrimSpace(c.NativeGRPCAddr) == "" {
		c.NativeGRPCAddr = "127.0.0.1:4100"
	}
	if strings.TrimSpace(c.ConsulAddr) == "" {
		c.ConsulAddr = "127.0.0.1:8500"
	}
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.ChannelSize <= 0 {
		c.ChannelSize = 100000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if strings.TrimSpace(c.DefaultSource) == "" {
		c.DefaultSource = "json-http"
	}
	return c
}

func newLoaderConn(ctx context.Context, config Config) (*shared.Conn, error) {
	switch config.ConnectionMode {
	case shared.LoaderConnectionStandardNative:
		return shared.NewLoaderConnection(ctx, shared.LoaderConnectionConfig{
			Mode:    shared.LoaderConnectionStandardNative,
			Owner:   "quantastream-loader",
			Address: config.NativeGRPCAddr,
		})
	case shared.LoaderConnectionDistributed:
		consul, err := api.NewClient(&api.Config{Address: config.ConsulAddr})
		if err != nil {
			return nil, err
		}
		return shared.NewLoaderConnection(ctx, shared.LoaderConnectionConfig{
			Mode:   shared.LoaderConnectionDistributed,
			Owner:  "quantastream-loader",
			Consul: consul,
			Quorum: 3,
		})
	default:
		return nil, fmt.Errorf("unsupported loader connection mode %q", config.ConnectionMode)
	}
}

func loadSelectorTables(tableCache *core.TableCacheStruct, config Config) ([]*core.Table, error) {
	tableNames := config.Tables
	if len(tableNames) == 0 {
		var err error
		tableNames, err = shared.ActiveOrDiscoveredSchemaTables(config.ConfigDir, config.Database)
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(tableNames)
	tables := make([]*core.Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			continue
		}
		table, err := core.LoadTable(tableCache, config.ConfigDir, nil, tableName, nil)
		if err != nil {
			return nil, fmt.Errorf("load table %s: %w", tableName, err)
		}
		if strings.TrimSpace(table.Selector) == "" {
			continue
		}
		tables = append(tables, table)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no selector-enabled tables loaded from %s", config.ConfigDir)
	}
	return tables, nil
}

// Handler returns the HTTP handler for all loader endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/ingest/json", s.handleJSONIngest)
	return mux
}

// Close drains the router and closes the engine connection.
func (s *Server) Close() error {
	var firstErr error
	if s.router != nil {
		if err := s.router.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.router = nil
	}
	if s.conn != nil {
		if err := s.conn.Disconnect(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.conn = nil
	}
	return firstErr
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "ok",
		"tables":          len(s.tables),
		"connection_mode": string(s.config.ConnectionMode),
	})
}

func (s *Server) handleJSONIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	requests, err := s.jsonAdapter.Decode(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	response := s.Route(requests)
	status := http.StatusAccepted
	if response.Failed > 0 && response.Accepted == 0 {
		status = http.StatusBadRequest
	} else if response.Failed > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, response)
}

// Route sends decoded requests through selector routing and into SessionRouter.
func (s *Server) Route(requests []EnvelopeRequest) IngestResponse {
	response := IngestResponse{Routes: make([]IngestRouteReply, 0, len(requests))}
	for _, request := range requests {
		options := request.RouteOptions
		options.Tables = s.tables
		route, diagnostics, err := core.BuildSelectedIngestRecordFromEnvelope(request.Envelope, options)
		if diagnostics.BlocksNative() {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d diagnostics block native: %s",
				request.OriginalIndex, diagnosticsSummary(diagnostics)))
			continue
		}
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d: %v", request.OriginalIndex, err))
			continue
		}
		if s.config.PhysicalBuildRouting && route.Record.BuildShardKey == "" {
			if buildShard, ok := core.ResolveIngestBuildShardKey(core.IngestBuildShardKeyRequest{
				Table:   route.Selector.Table.BasicTable,
				Payload: payloadDataMap(request.Envelope.Payload),
			}); ok {
				route.Record.BuildShardKey = buildShard.BuildShardKey
				route.BuildShardKey = buildShard.BuildShardKey
			}
		}
		if err := s.router.Enqueue(route.Record); err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d enqueue: %v", request.OriginalIndex, err))
			continue
		}
		response.Accepted++
		response.Routes = append(response.Routes, IngestRouteReply{
			Index:        request.OriginalIndex,
			Table:        route.Record.TableName,
			EventID:      route.Record.EventID,
			ShardMode:    string(route.ShardKey.Mode),
			BuildSharded: route.Record.BuildShardKey != "",
		})
	}
	return response
}

func payloadDataMap(payload map[string]interface{}) map[string]interface{} {
	if payload == nil {
		return nil
	}
	if data, ok := payload["data"].(map[string]interface{}); ok {
		return data
	}
	return payload
}

func diagnosticsSummary(diagnostics qsbridge.DiagnosticSet) string {
	codes := diagnostics.Codes()
	if len(codes) == 0 {
		return "unknown diagnostics"
	}
	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, string(code))
	}
	return strings.Join(parts, ",")
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]interface{}{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
