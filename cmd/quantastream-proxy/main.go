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
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	"github.com/QuantaStream/quantastream/version"
	log "github.com/sirupsen/logrus"
)

const defaultDistributedNodePort = 4400

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runWithContext(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("quantastream-proxy", flag.ContinueOnError)
	flags.SetOutput(stderr)

	bindAddress := flags.String("bind", envString("QUANTASTREAM_BIND", "127.0.0.1"), "MySQL bind address")
	mysqlPort := flags.Int("mysql-port", envInt("QUANTASTREAM_MYSQL_PORT", 4000), "MySQL listen port")
	consulAddress := flags.String("consul", envString("QUANTASTREAM_CONSUL_ENDPOINT", "127.0.0.1:8500"), "Consul agent address")
	nodePort := flags.Int("node-port", envInt("QUANTASTREAM_NODE_PORT", defaultDistributedNodePort), "Quanta distributed node service port")
	sessionPoolSize := flags.Int("session-pool-size", envInt("QUANTASTREAM_SESSION_POOL_SIZE", 0), "direct runtime session pool size; zero uses runtime default")
	schemaDir := flags.String("schema-dir", envString("QUANTASTREAM_SCHEMA_DIR", "configuration"), "schema/catalog configuration directory for SQL schema mutations")
	database := flags.String("database", envString("QUANTASTREAM_DATABASE", "quanta"), "default database/schema name")
	authMode := flags.String("auth-mode", envString("QUANTASTREAM_AUTH_MODE", qsmysql.AuthModePermissive), "MySQL auth mode: permissive or static")
	authUser := flags.String("auth-user", envString("QUANTASTREAM_AUTH_USER", ""), "static MySQL auth username; defaults to MOLIG004 when auth-mode=static")
	authPassword := flags.String("auth-password", envString("QUANTASTREAM_AUTH_PASSWORD", ""), "static MySQL auth password; prefer QUANTASTREAM_AUTH_PASSWORD for scripts")
	authAccountFile := flags.String("auth-account-file", envString("QUANTASTREAM_AUTH_ACCOUNT_FILE", ""), "YAML static auth account file; used when auth-mode=static")
	accessPolicyFile := flags.String("access-policy-file", envString("QUANTASTREAM_ACCESS_POLICY_FILE", ""), "YAML static SQL access policy file; empty leaves SQL authorization permissive")
	runtimeProbes := flags.Bool("runtime-probes", envBool("QUANTASTREAM_RUNTIME_PROBES"), "log runtime execution probes after each query")
	pprofBind := flags.String("pprof-bind", envString("QUANTASTREAM_PPROF_BIND", ""), "optional pprof listen address, for example 127.0.0.1:6060")
	statusOnly := flags.Bool("status", false, "print startup readiness and exit successfully")
	showVersion := flags.Bool("version", false, "print QuantaStream version and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.Summary())
		return 0
	}
	if *mysqlPort < 0 {
		fmt.Fprintf(stderr, "mysql port cannot be negative: %d\n", *mysqlPort)
		return 2
	}
	if *nodePort < 0 {
		fmt.Fprintf(stderr, "node port cannot be negative: %d\n", *nodePort)
		return 2
	}
	if *sessionPoolSize < 0 {
		fmt.Fprintf(stderr, "session pool size cannot be negative: %d\n", *sessionPoolSize)
		return 2
	}

	startPprofServer(*pprofBind, stderr)

	mountStart := time.Now()
	process, err := mountDistributedProxy(ctx, distributedProxyConfig{
		BindAddress:      *bindAddress,
		MySQLPort:        *mysqlPort,
		ConsulAddress:    *consulAddress,
		NodePort:         *nodePort,
		SessionPoolSize:  *sessionPoolSize,
		SchemaDir:        *schemaDir,
		Database:         *database,
		AuthMode:         *authMode,
		AuthUser:         *authUser,
		AuthPassword:     *authPassword,
		AuthAccountFile:  *authAccountFile,
		AccessPolicyFile: *accessPolicyFile,
		RuntimeProbes:    *runtimeProbes,
	})
	mountElapsed := time.Since(mountStart)
	if err != nil {
		fmt.Fprintf(stderr, "mount distributed proxy: %v\n", err)
		return 2
	}
	defer process.Close()

	fmt.Fprintf(stdout, "mount_elapsed=%s\n", mountElapsed)
	for _, line := range process.SummaryLines() {
		fmt.Fprintln(stdout, line)
	}
	if !process.FrontDoor.Ready() {
		fmt.Fprintln(stderr, "distributed proxy front door is not ready")
		return 2
	}
	if *statusOnly {
		return 0
	}

	address := fmt.Sprintf("%s:%d", process.Config.BindAddress, process.Config.MySQLPort)
	fmt.Fprintf(stdout, "listening=%s\n", address)
	if err := process.FrontDoor.ListenAndServe(ctx, qsruntime.NativeProxyListenConfig{
		Address:          address,
		EnableAcceptLoop: true,
	}); err != nil {
		fmt.Fprintf(stderr, "serve distributed proxy: %v\n", err)
		return 1
	}
	return 0
}

type distributedProxyConfig struct {
	BindAddress      string
	MySQLPort        int
	ConsulAddress    string
	NodePort         int
	SessionPoolSize  int
	SchemaDir        string
	Database         string
	AuthMode         string
	AuthUser         string
	AuthPassword     string
	AuthAccountFile  string
	AccessPolicyFile string
	RuntimeProbes    bool
}

type distributedProxyProcess struct {
	Config       distributedProxyConfig
	Source       *source.QuantaSource
	TableCache   *core.TableCacheStruct
	Tables       []string
	FrontDoor    qsruntime.NativeProxyFrontDoor
	RuntimeReady bool
}

func mountDistributedProxy(ctx context.Context, config distributedProxyConfig) (distributedProxyProcess, error) {
	config = config.withDefaults()
	authenticator, err := config.mysqlAuthenticator()
	if err != nil {
		return distributedProxyProcess{}, fmt.Errorf("configure distributed proxy MySQL auth: %w", err)
	}
	authorizer, err := config.accessAuthorizer()
	if err != nil {
		return distributedProxyProcess{}, fmt.Errorf("configure distributed proxy access policy: %w", err)
	}
	catalogTableCache := core.NewTableCacheStruct()
	runtimeTableCache := core.NewTableCacheStruct()
	directConfig := qsruntime.NewDirectRuntimeConfig("", config.ConsulAddress, config.NodePort, config.SessionPoolSize)
	quantaSource, err := source.NewQuantaSource(
		runtimeTableCache,
		directConfig.BaseDir,
		directConfig.ConsulAddress,
		directConfig.ServicePort,
		directConfig.SessionPoolSize,
	)
	if err != nil {
		return distributedProxyProcess{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = quantaSource.Close()
		}
	}()

	tables, err := loadDistributedCatalogTables(ctx, catalogTableCache, quantaSource)
	if err != nil {
		return distributedProxyProcess{}, err
	}
	nativeRuntime, diagnostics, err := qsruntime.NewNativeProxyRuntimeFromSourceWithLegacyOptions(ctx, quantaSource, catalogTableCache, qsruntime.NativeProxyRuntimeConfig{
		Direct:                  directConfig,
		DefaultSchema:           config.Database,
		SchemaDir:               config.SchemaDir,
		CatalogVersion:          qsbridge.CatalogVersion("quantastream-proxy-distributed"),
		Functions:               qsbridge.BuiltinSQLFunctionDefinitions(),
		Profile:                 qsruntime.LegacyDirectRuntimeProfile(),
		ContextWrapper:          qsruntime.WithQueryScratchpad,
		EnableFilterExpressions: true,
	}, qsruntime.NativeProxyRuntimeLegacyOptions{
		PrimaryKeyResolverFactory: qsinabox.NewSharedStandardSessionBSIPrimaryKeyResolverFactory(catalogTableCache),
	})
	if err != nil {
		return distributedProxyProcess{}, err
	}
	if diagnostics.BlocksNative() {
		return distributedProxyProcess{}, fmt.Errorf("runtime diagnostics block native execution: %s", diagnosticMessages(diagnostics))
	}

	serverConfig := qsruntime.NativeProxyServerConfig{
		Route:          qsruntime.ConsulDirectRoute(qsruntime.RuntimeTarget{}),
		ContextWrapper: qsruntime.WithQueryScratchpad,
		Authorizer:     authorizer,
	}
	if config.RuntimeProbes {
		serverConfig.ProbeLogger = qsruntime.RuntimeProbeLoggerFunc(func(probe qsruntime.ExecutionProbe) {
			log.Infof("RUNTIME probe section=%s name=%s value=%s detail=%s", probe.Section, probe.Name, probe.Value, probe.Detail)
		})
	}
	frontDoor := qsruntime.NewNativeProxyFrontDoor(nativeRuntime, qsruntime.NativeProxyFrontDoorConfig{
		Server:        serverConfig,
		BindAddress:   config.BindAddress,
		Port:          config.MySQLPort,
		PacketIOReady: true,
		MySQLAdapter:  qsmysql.ByteModelReadiness(),
		Authenticator: authenticator,
		Protocol: qsbridge.NewProtocolProfile(
			qsbridge.ProtocolMySQL,
			"mysql-wire",
			qsbridge.ProtocolCapabilityStatementResults,
			qsbridge.ProtocolCapabilitySessionActions,
			qsbridge.ProtocolCapabilityExplain,
		),
	})
	closed = true
	return distributedProxyProcess{
		Config:       config,
		Source:       quantaSource,
		TableCache:   catalogTableCache,
		Tables:       tables,
		FrontDoor:    frontDoor,
		RuntimeReady: nativeRuntime.Ready(),
	}, nil
}

func (c distributedProxyConfig) withDefaults() distributedProxyConfig {
	if strings.TrimSpace(c.BindAddress) == "" {
		c.BindAddress = "127.0.0.1"
	}
	if c.MySQLPort == 0 {
		c.MySQLPort = 4000
	}
	if strings.TrimSpace(c.ConsulAddress) == "" {
		c.ConsulAddress = "127.0.0.1:8500"
	}
	if c.NodePort == 0 {
		c.NodePort = defaultDistributedNodePort
	}
	if strings.TrimSpace(c.SchemaDir) == "" {
		c.SchemaDir = "configuration"
	}
	if strings.TrimSpace(c.Database) == "" {
		c.Database = "quanta"
	}
	return c
}

func (c distributedProxyConfig) mysqlAuthConfig() qsmysql.AuthConfig {
	c = c.withDefaults()
	return qsmysql.AuthConfig{
		Mode:        c.AuthMode,
		Username:    c.AuthUser,
		Password:    c.AuthPassword,
		Database:    c.Database,
		AccountFile: c.AuthAccountFile,
	}.WithDefaults(c.Database)
}

func (c distributedProxyConfig) mysqlAuthenticator() (qsmysql.Authenticator, error) {
	c = c.withDefaults()
	return c.mysqlAuthConfig().Authenticator(c.Database)
}

func (c distributedProxyConfig) accessAuthorizer() (qsbridge.AccessAuthorizer, error) {
	c = c.withDefaults()
	if strings.TrimSpace(c.AccessPolicyFile) == "" {
		return nil, nil
	}
	policy, err := qsbridge.LoadAccessPolicyFile(c.AccessPolicyFile)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func (p distributedProxyProcess) Close() {
	if p.Source != nil {
		_ = p.Source.Close()
	}
}

func (p distributedProxyProcess) SummaryLines() []string {
	summary := p.FrontDoor.Summary()
	authorization := "authorization=permissive"
	if strings.TrimSpace(p.Config.AccessPolicyFile) != "" {
		authorization = "authorization=static_policy"
	}
	lines := []string{
		"mode=distributed-proxy",
		fmt.Sprintf("consul=%s", p.Config.ConsulAddress),
		fmt.Sprintf("node_port=%d", p.Config.NodePort),
		fmt.Sprintf("mysql=%s:%d", p.Config.BindAddress, p.Config.MySQLPort),
		fmt.Sprintf("schema_dir=%s", p.Config.SchemaDir),
		fmt.Sprintf("database=%s", p.Config.Database),
		fmt.Sprintf("auth=%s", p.Config.mysqlAuthConfig().SummaryMode(p.Config.Database)),
		authorization,
		fmt.Sprintf("tables=%d [%s]", len(p.Tables), strings.Join(p.Tables, ",")),
		fmt.Sprintf("runtime_ready=%t", summary.RuntimeReady),
		fmt.Sprintf("wire_ready=%t", summary.WireReady),
		fmt.Sprintf("ready=%t", summary.Ready),
	}
	if user := p.Config.mysqlAuthConfig().SummaryUser(p.Config.Database); user != "" {
		lines = append(lines, fmt.Sprintf("auth_user=%s", user))
	}
	if accountFile := p.Config.mysqlAuthConfig().SummaryAccountFile(p.Config.Database); accountFile != "" {
		lines = append(lines, fmt.Sprintf("auth_account_file=%s", accountFile))
	}
	if strings.TrimSpace(p.Config.AccessPolicyFile) != "" {
		lines = append(lines, fmt.Sprintf("access_policy_file=%s", p.Config.AccessPolicyFile))
	}
	return lines
}

func loadDistributedCatalogTables(ctx context.Context, tableCache *core.TableCacheStruct, quantaSource *source.QuantaSource) ([]string, error) {
	if tableCache == nil {
		return nil, fmt.Errorf("distributed proxy table cache is not initialized")
	}
	if quantaSource == nil || quantaSource.GetConnection() == nil || quantaSource.GetSessionPool() == nil {
		return nil, fmt.Errorf("distributed proxy source is not initialized")
	}
	tables, err := shared.GetTables(quantaSource.GetConnection().Consul)
	if err != nil {
		return nil, err
	}
	sort.Strings(tables)
	kvStore := shared.NewKVStore(quantaSource.GetConnection())
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := core.LoadTable(tableCache, "", kvStore, table, quantaSource.GetConnection().Consul); err != nil {
			if missingTablePreloadError(table, err) {
				continue
			}
			return nil, fmt.Errorf("preload distributed proxy table %s: %w", table, err)
		}
	}
	return tables, nil
}

func missingTablePreloadError(table string, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	table = strings.ToLower(strings.TrimSpace(table))
	return table != "" &&
		strings.Contains(message, "table "+table+" not found") &&
		strings.Contains(message, "unmarshalconsul")
}

func diagnosticMessages(diagnostics qsbridge.DiagnosticSet) string {
	if len(diagnostics) == 0 {
		return ""
	}
	messages := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		messages = append(messages, fmt.Sprintf("%s: %s", diagnostic.Code, diagnostic.Message))
	}
	return strings.Join(messages, "; ")
}

func envString(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
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
