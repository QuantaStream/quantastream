package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	u "github.com/araddon/gou"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
	"github.com/QuantaStream/quantastream/test"
	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/hashicorp/consul/api"
	logger "github.com/sirupsen/logrus"
)

const (
	engineProxy          = "proxy"
	engineDistributed    = "distributed"
	engineInaboxStandard = "inabox-standard"
	engineInaboxLocal    = "inabox-local"
	engineRuntime        = "runtime"
	engineRuntimeInspect = "runtime-inspect"
	engineInaboxDirect   = "inabox-direct"
	engineMySQLReference = "mysql-reference"
)

var consulAddress = "127.0.0.1:8500"

// var buf bytes.Buffer
var log = logger.New()

type runnerConfig struct {
	Engine            string
	Host              string
	User              string
	Password          string
	Database          string
	Port              string
	Consul            string
	CaseID            string
	EngineDiff        string
	MySQLDSN          string
	MySQLDriver       string
	Verbose           bool
	CompatReport      bool
	DumpActual        bool
	CaptureExpected   string
	SlowThreshold     time.Duration
	BenchmarkReport   string
	BenchmarkProfile  string
	BenchmarkMetadata string
	BenchmarkWarmup   int
	BenchmarkRuns     int
	BenchmarkSummary  string
}

type runnerHarness struct {
	Runner roadmap.Runner
	Close  func() error
}

func main() {
	shared.SetUTCdefault()

	suiteFile := flag.String("suite_file", "", "Path to a SQL roadmap YAML suite to execute.")
	engine := flag.String("engine", engineProxy, "SQLRunner execution harness: inabox-standard, inabox-local, distributed, inabox-direct, proxy, runtime, runtime-inspect, or mysql-reference.")
	host := flag.String("host", "", "Quanta host to connect to.")
	user := flag.String("user", "", "The username that will connect to the database.")
	password := flag.String("password", "", "The password to use to connect.")
	database := flag.String("db", "quanta", "The database to connect to.")
	port := flag.String("port", "4000", "Port to connect to.")
	consul := flag.String("consul", "127.0.0.1:8500", "Address of consul.")
	caseID := flag.String("case", "", "Run only the suite test with this exact id.")
	engineDiff := flag.String("engine_diff", "", "Run a compatibility differential pass as reference,target engines.")
	logLevel := flag.String("log_level", "", "Set the logging level to DEBUG for additional logging.")
	mysqlDSN := flag.String("mysql_dsn", "", "database/sql DSN for the mysql-reference engine.")
	mysqlDriver := flag.String("mysql_driver", defaultMySQLReferenceDriver, "database/sql driver name for the mysql-reference engine.")
	verbose := flag.Bool("verbose", false, "Print each roadmap case SQL and detailed timing while it runs.")
	dumpActual := flag.Bool("dump_actual", false, "Print actual query rows when a roadmap case mismatches.")
	captureExpected := flag.String("capture_expected", "", "Write a runnable SQLRunner suite with expectations captured from the selected engine.")
	slowThreshold := flag.Duration("slow_threshold", 0, "Print a slow-case summary for roadmap cases at or above this duration, such as 10s.")
	compatReport := flag.Bool("compat_report", false, "Print a compatibility scorecard grouped by feature and result category.")
	benchmarkReport := flag.String("benchmark_report", "", "Write a JSON benchmark report for repeated measured suite runs.")
	benchmarkProfile := flag.String("benchmark_profile", "developer-local", "Benchmark profile name recorded in -benchmark_report output.")
	benchmarkMetadata := flag.String("benchmark_metadata", "", "Comma-separated key=value metadata recorded in -benchmark_report output.")
	benchmarkWarmup := flag.Int("benchmark_warmup", 0, "Warm-up suite runs before measured benchmark runs.")
	benchmarkRuns := flag.Int("benchmark_runs", 1, "Measured suite runs written to -benchmark_report output.")
	benchmarkSummary := flag.String("benchmark_summary", "", "Read a JSON benchmark report and print a human-readable summary.")
	flag.Parse()

	cfg := runnerConfig{
		Engine:            strings.ToLower(strings.TrimSpace(*engine)),
		Host:              *host,
		User:              *user,
		Password:          *password,
		Database:          *database,
		Port:              *port,
		Consul:            *consul,
		CaseID:            strings.TrimSpace(*caseID),
		EngineDiff:        strings.TrimSpace(*engineDiff),
		MySQLDSN:          *mysqlDSN,
		MySQLDriver:       *mysqlDriver,
		Verbose:           *verbose,
		CompatReport:      *compatReport,
		DumpActual:        *dumpActual,
		CaptureExpected:   strings.TrimSpace(*captureExpected),
		SlowThreshold:     *slowThreshold,
		BenchmarkReport:   strings.TrimSpace(*benchmarkReport),
		BenchmarkProfile:  strings.TrimSpace(*benchmarkProfile),
		BenchmarkMetadata: strings.TrimSpace(*benchmarkMetadata),
		BenchmarkWarmup:   *benchmarkWarmup,
		BenchmarkRuns:     *benchmarkRuns,
		BenchmarkSummary:  strings.TrimSpace(*benchmarkSummary),
	}
	cfg = applyEngineDefaults(cfg)

	if cfg.BenchmarkSummary != "" {
		if err := printBenchmarkSummary(cfg.BenchmarkSummary); err != nil {
			log.Printf("SQL benchmark summary failed: %v", err)
			os.Exit(1)
		}
		return
	}

	if err := validateFlags(*suiteFile, cfg); err != nil {
		printUsage(err)
		os.Exit(0)
	}

	configureLogging(*logLevel)

	u.Debugf("suite_file : %s", *suiteFile)
	u.Debugf("engine : %s", cfg.Engine)
	u.Debugf("host : %s", cfg.Host)
	u.Debugf("user : %s", cfg.User)
	u.Debugf("database : %s", cfg.Database)
	u.Debugf("port : %s", cfg.Port)
	u.Debugf("log_level : %s", *logLevel)
	u.Debugf("slow_threshold : %s", cfg.SlowThreshold)

	suite, err := loadSuite(*suiteFile)
	check(err)
	if err := filterSuiteCase(suite, cfg.CaseID); err != nil {
		log.Printf("SQL roadmap suite failed: %v", err)
		os.Exit(1)
	}

	if cfg.EngineDiff != "" {
		if err := executeCompatibilityDiff(context.Background(), suite, cfg); err != nil {
			log.Printf("SQL compatibility diff failed: %v", err)
			os.Exit(1)
		}
		return
	}

	harness, err := buildHarness(suite, cfg)
	if err != nil {
		log.Printf("SQL roadmap suite failed: %v", err)
		os.Exit(1)
	}
	if harness.Close != nil {
		defer func() {
			if err := harness.Close(); err != nil {
				log.Printf("SQL roadmap harness close failed: %v", err)
			}
		}()
	}

	if cfg.CaptureExpected != "" {
		if err := captureCompatibilityExpected(context.Background(), suite, harness.Runner, cfg); err != nil {
			log.Printf("SQL roadmap capture failed: %v", err)
			os.Exit(1)
		}
		return
	}

	if cfg.BenchmarkReport != "" {
		if err := executeBenchmarkSuite(context.Background(), suite, harness.Runner, cfg); err != nil {
			log.Printf("SQL benchmark suite failed: %v", err)
			os.Exit(1)
		}
		return
	}

	if err := executeSuite(context.Background(), suite, harness.Runner, cfg.Verbose, cfg.SlowThreshold, cfg.CompatReport); err != nil {
		log.Printf("SQL roadmap suite failed: %v", err)
		os.Exit(1)
	}
}

func validateFlags(suiteFile string, cfg runnerConfig) error {
	if suiteFile == "" {
		return fmt.Errorf("suite_file is required")
	}
	if cfg.BenchmarkWarmup < 0 {
		return fmt.Errorf("benchmark_warmup cannot be negative")
	}
	if cfg.BenchmarkReport != "" && cfg.BenchmarkRuns <= 0 {
		return fmt.Errorf("benchmark_runs must be greater than zero")
	}
	if cfg.BenchmarkReport != "" && cfg.CaptureExpected != "" {
		return fmt.Errorf("benchmark_report cannot be combined with capture_expected")
	}
	if cfg.BenchmarkReport != "" && cfg.EngineDiff != "" {
		return fmt.Errorf("benchmark_report cannot be combined with engine_diff")
	}
	if cfg.EngineDiff != "" {
		diff, err := parseEngineDiff(cfg.EngineDiff)
		if err != nil {
			return err
		}
		if err := validateEngineForFlags(diff.Reference, cfg); err != nil {
			return err
		}
		return validateEngineForFlags(diff.Target, cfg)
	}
	return validateEngineForFlags(cfg.Engine, cfg)
}

func printUsage(err error) {
	u.Warn()
	u.Warn(err.Error())
	u.Warn()
	u.Warn("Inabox-standard example: ./sqlrunner -engine inabox-standard -suite_file sqltests/basic_queries.yaml")
	u.Warn("Inabox-local example: ./sqlrunner -engine inabox-local -suite_file sqltests/joins_sql.yaml")
	u.Warn("Distributed example: ./sqlrunner -engine distributed -suite_file sqltests/joins_sql.yaml -host 10.0.0.10 -user MOLIG004 -db quanta -port 4000")
	u.Warn("Runtime example: ./sqlrunner -engine runtime -suite_file sqltests/basic_queries.yaml")
	u.Warn("Runtime inspection example: ./sqlrunner -engine runtime-inspect -suite_file sqltests/runtime_inspection.yaml")
	u.Warn("Inabox-direct example: ./sqlrunner -engine inabox-direct -suite_file sqltests/inabox_direct_smoke.yaml -consul 127.0.0.1:8500")
	u.Warn("MySQL reference example: ./sqlrunner -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test'")
	u.Warn("Capture example: ./sqlrunner -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test' -capture_expected expected/mysql_compat_select.yaml")
	u.Warn("Diff example: ./sqlrunner -engine_diff mysql-reference,inabox-direct -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test'")
	u.Warn("Benchmark example: ./sqlrunner -engine inabox-direct -suite_file sqltests/inabox_direct_tpch_kernels.yaml -benchmark_report expected/local/tpch.json -benchmark_runs 3 -benchmark_profile developer-local")
	u.Warn("Benchmark summary example: ./sqlrunner -benchmark_summary expected/local/tpch.json")
}

func configureLogging(logLevel string) {
	if logLevel != "DEBUG" {
		return
	}
	formatter := runtime.Formatter{ChildFormatter: &logger.TextFormatter{
		FullTimestamp: true,
	}}
	formatter.Line = true
	log.SetFormatter(&formatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(logger.DebugLevel)
	log.WithFields(logger.Fields{
		"file": "driver.go",
	}).Info("SqlRunner is running...")
}

func loadSuite(suiteFile string) (*roadmap.Suite, error) {
	data, err := os.ReadFile(suiteFile)
	if err != nil {
		return nil, err
	}
	return roadmap.Parse(data)
}

func filterSuiteCase(suite *roadmap.Suite, caseID string) error {
	if caseID == "" {
		return nil
	}
	for _, test := range suite.Tests {
		if test.ID == caseID {
			suite.Tests = []roadmap.TestCase{test}
			return nil
		}
	}
	return fmt.Errorf("case %q not found in suite %q", caseID, suite.Name)
}

func buildHarness(suite *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	cfg = applyEngineDefaults(cfg)
	switch cfg.Engine {
	case engineProxy, engineDistributed, engineInaboxLocal:
		return buildProxyHarness(suite, cfg)
	case engineInaboxStandard:
		return buildInaboxStandardHarness(suite, cfg)
	case engineInaboxDirect:
		return buildLegacyDirectHarness(suite, cfg)
	case engineRuntime, engineRuntimeInspect:
		return buildRuntimeHarness(suite, cfg)
	case engineMySQLReference:
		return buildMySQLReferenceHarness(suite, cfg)
	default:
		return runnerHarness{}, fmt.Errorf("unsupported engine %q", cfg.Engine)
	}
}

func applyEngineDefaults(cfg runnerConfig) runnerConfig {
	if cfg.Engine != engineInaboxLocal && cfg.Engine != engineInaboxStandard {
		return cfg
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.User == "" {
		cfg.User = "MOLIG004"
	}
	if cfg.Port == "" {
		cfg.Port = "4000"
	}
	if cfg.Database == "" {
		cfg.Database = "quanta"
	}
	return cfg
}

func buildInaboxStandardHarness(suite *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	var proxyConnect test.ProxyConnectStrings
	proxyConnect.Host = cfg.Host
	proxyConnect.User = cfg.User
	proxyConnect.Password = cfg.Password
	proxyConnect.Port = cfg.Port
	proxyConnect.Database = cfg.Database
	proxyConnect.Timeout = suite.MaxCaseTimeout()

	db, err := proxyConnect.ProxyConnectConnect()
	if err != nil {
		return runnerHarness{}, err
	}

	return runnerHarness{
		Runner: roadmap.Runner{
			DB:         db,
			Verbose:    cfg.Verbose,
			DumpActual: cfg.DumpActual,
			Logf:       log.Printf,
		},
		Close: db.Close,
	}, nil
}

func buildProxyHarness(suite *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	var proxyConnect test.ProxyConnectStrings
	proxyConnect.Host = cfg.Host
	proxyConnect.User = cfg.User
	proxyConnect.Password = cfg.Password
	proxyConnect.Port = cfg.Port
	proxyConnect.Database = cfg.Database

	sharedKV, err := initializeCluster(cfg.Consul)
	if err != nil {
		return runnerHarness{}, err
	}
	if err := grantSQLRunnerRoles(sharedKV); err != nil {
		return runnerHarness{}, err
	}

	proxyConnect.Timeout = suite.MaxCaseTimeout()
	db, err := proxyConnect.ProxyConnectConnect()
	if err != nil {
		return runnerHarness{}, err
	}

	return runnerHarness{
		Runner: roadmap.Runner{
			DB:         db,
			Admin:      func(_ context.Context, command string) error { return test.ExecuteAdminCommandAndWait(command) },
			Verbose:    cfg.Verbose,
			DumpActual: cfg.DumpActual,
			Logf:       log.Printf,
		},
		Close: db.Close,
	}, nil
}

func buildRuntimeHarness(_ *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	runtime, err := newRuntimeFixtureSQLRuntime(context.Background())
	if err != nil {
		return runnerHarness{}, err
	}
	engine := runtimeRoadmapEngine{Runtime: runtime}
	if cfg.Verbose {
		engine.Logf = log.Printf
	}
	if cfg.Engine == engineRuntimeInspect {
		engine.Inspect = true
	}
	return runnerHarness{
		Runner: roadmap.Runner{
			Engine:     engine,
			Admin:      func(context.Context, string) error { return nil },
			Verbose:    cfg.Verbose,
			DumpActual: cfg.DumpActual,
			Logf:       log.Printf,
		},
	}, nil
}

func initializeCluster(consul string) (*shared.KVStore, error) {
	test.ConsulAddress = consul
	u.Debugf("ConsulAddress : %s", test.ConsulAddress)

	consulClient, err := api.NewClient(&api.Config{Address: test.ConsulAddress})
	if err != nil {
		return nil, err
	}

	conn := shared.NewDefaultConnection("sqlrunner")
	// conn.ServicePort = main.Port
	conn.Quorum = 3
	if err := conn.Connect(consulClient); err != nil {
		return nil, err
	}

	sharedKV := shared.NewKVStore(conn)

	// we're in cluster so we can just call admin status directly

	now := time.Now()
	for {
		status, active, size := sharedKV.GetClusterState()
		u.Debugf("consul status sqlrunner clusterState %v count %v size %v\n", status, active, size)

		if status.String() == "GREEN" && active >= 3 && size >= 3 {
			break
		}
		if time.Since(now) > time.Second*30 {
			return nil, fmt.Errorf("consul timeout driver after NewKVStore")
		}
		time.Sleep(500 * time.Millisecond)
	}
	return sharedKV, nil
}

func grantSQLRunnerRoles(_ *shared.KVStore) error {
	// The historical KVStore-backed RBAC seed path is quarantined.
	// MySQL-compatible authentication and authorization will be owned by
	// the qsbridge session and protocol stack instead of this proxy-era hook.
	return nil
}

func executeSuite(ctx context.Context, suite *roadmap.Suite, runner roadmap.Runner, verbose bool, slowThreshold time.Duration, compatReport bool) error {
	summary := runner.Run(ctx, suite)
	log.Printf("\n-------- SQL Roadmap Suite: %s --------", summary.Suite)
	logSummaryResults(summary, verbose)
	logSlowCases(summary, slowThreshold, verbose)
	if compatReport {
		logCompatibilityReport(suite, summary)
	}
	if summary.HasFailures() {
		return fmt.Errorf("suite contains FAIL or XPASS results")
	}
	return nil
}

func captureCompatibilityExpected(ctx context.Context, suite *roadmap.Suite, runner roadmap.Runner, cfg runnerConfig) error {
	capture := runner.CaptureCompatibilityExpected(ctx, suite, roadmap.CompatibilityCaptureOptions{Canonical: roadmap.DefaultCanonicalOptions()})
	log.Printf("\n-------- SQL Compatibility Capture: %s --------", capture.Summary.Suite)
	for _, result := range capture.Summary.Results {
		duration := formatCaseDuration(result.Duration, cfg.Verbose)
		if result.Details == "" {
			log.Printf("%-6s %s%s", result.Status, result.ID, duration)
		} else {
			log.Printf("%-6s %s%s: %s", result.Status, result.ID, duration, result.Details)
		}
	}
	data, err := roadmap.MarshalCompatibilityExpectedSuite(capture.Suite)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.CaptureExpected, data, 0644); err != nil {
		return err
	}
	log.Printf("WROTE  %s (%d captured cases)", cfg.CaptureExpected, len(capture.Expected.Cases))
	if capture.Summary.HasFailures() {
		return fmt.Errorf("capture contains FAIL results")
	}
	return nil
}

func logSlowCases(summary roadmap.Summary, threshold time.Duration, verbose bool) {
	slow := slowCaseResults(summary, threshold)
	if len(slow) == 0 {
		return
	}
	log.Printf("SLOW   cases >= %s", formatSlowThreshold(threshold, verbose))
	for _, result := range slow {
		log.Printf("SLOW   %s%s %s", result.ID, formatCaseDuration(result.Duration, verbose), result.Status)
	}
}

func slowCaseResults(summary roadmap.Summary, threshold time.Duration) []roadmap.CaseResult {
	if threshold <= 0 {
		return nil
	}
	var slow []roadmap.CaseResult
	for _, result := range summary.Results {
		if result.Duration >= threshold {
			slow = append(slow, result)
		}
	}
	sort.Slice(slow, func(i, j int) bool {
		if slow[i].Duration == slow[j].Duration {
			return slow[i].ID < slow[j].ID
		}
		return slow[i].Duration > slow[j].Duration
	})
	return slow
}

func formatSlowThreshold(duration time.Duration, verbose bool) string {
	if verbose {
		return duration.Round(time.Millisecond).String()
	}
	rounded := duration.Round(time.Second)
	if rounded == 0 {
		return "<1s"
	}
	return rounded.String()
}

func formatCaseDuration(duration time.Duration, verbose bool) string {
	if duration == 0 {
		return ""
	}
	if verbose {
		return fmt.Sprintf(" [%s]", duration.Round(time.Millisecond))
	}
	rounded := duration.Round(time.Second)
	if rounded == 0 {
		return " [<1s]"
	}
	return fmt.Sprintf(" [%s]", rounded)
}

func logAdminOutput(command string, output []byte) {
	log.Print("-------------------------------------------------------------------------------")
	log.Printf("%s", command)
	log.Print("Output:")
	log.Printf("%v", string(output[:]))
}

var _ = logAdminOutput // fixme: (atw) unused

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
