package admin

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/version"
	"github.com/hashicorp/consul/api"
)

const defaultSupportBundleMaxLogBytes int64 = 1024 * 1024

type SupportCmd struct {
	Bundle SupportBundleCmd `cmd:"" help:"Create a local diagnostic tarball for support."`
}

type SupportBundleCmd struct {
	Output            string   `help:"Output .tar.gz path. Defaults to qstream-support-<timestamp>.tar.gz in the current directory."`
	DataDir           string   `help:"QuantaStream data directory to summarize." default:"data"`
	ConfigDir         string   `help:"Optional config directory to summarize. Defaults to <data-dir>/config when present, then ./config when present."`
	WALPath           string   `help:"Optional local WAL path to plan. Defaults to <data-dir>/storage.wal when present."`
	AuthAccountFile   string   `help:"Optional static auth account file to validate and summarize without raw contents."`
	AccessPolicyFile  string   `help:"Optional static SQL access policy file to validate and summarize without raw contents."`
	BackupSource      []string `help:"Backup source directory whose manifest should be included. Use a comma-separated list for multiple backups."`
	LogPath           []string `help:"Log file to include as a recent tail. Use a comma-separated list for multiple logs."`
	MaxLogBytes       int64    `help:"Maximum bytes to include per log file." default:"1048576"`
	SkipClusterStatus bool     `help:"Skip best-effort Consul service-discovery status." default:"false"`
}

func (c *SupportBundleCmd) Run(ctx *Context) error {
	output, err := c.outputPath()
	if err != nil {
		return err
	}
	archive, err := newSupportBundleArchive(output)
	if err != nil {
		return err
	}
	defer archive.Close()

	dataDir, dataDirErr := resolveOptionalLocalPath(c.DataDir)
	if dataDirErr != nil {
		dataDir = strings.TrimSpace(c.DataDir)
	}
	configDir := c.resolveConfigDir(dataDir)
	walPath := c.resolveWALPath(dataDir)
	maxLogBytes := c.MaxLogBytes
	if maxLogBytes <= 0 {
		maxLogBytes = defaultSupportBundleMaxLogBytes
	}

	archive.AddText("README.txt", supportBundleReadme())
	archive.AddText("metadata/version.txt", supportBundleVersion())
	archive.AddText("metadata/runtime.txt", supportBundleRuntime(ctx, dataDir, configDir, walPath, dataDirErr))
	archive.AddText("config/summary.txt", supportBundleConfigSummary(dataDir, configDir))
	archive.AddText("security/summary.txt", supportBundleSecuritySummary(c.AuthAccountFile, c.AccessPolicyFile))
	if walPath != "" {
		archive.AddText("wal/plan.txt", supportBundleWALPlan(walPath))
	} else {
		archive.AddText("wal/skipped.txt", "wal_plan_skipped=no_wal_path\n")
	}
	c.addBackupManifests(archive)
	c.addLogs(archive, maxLogBytes)
	if c.SkipClusterStatus {
		archive.AddText("cluster/status-skipped.txt", "cluster_status_skipped=true\n")
	} else {
		archive.AddText("cluster/status.txt", supportBundleClusterStatus(ctx))
	}
	if err := archive.Close(); err != nil {
		return err
	}
	fmt.Printf("support_bundle_created=%s\n", output)
	fmt.Printf("support_bundle_entries=%d\n", archive.EntryCount())
	return nil
}

func (c *SupportBundleCmd) outputPath() (string, error) {
	output := strings.TrimSpace(c.Output)
	if output == "" {
		output = fmt.Sprintf("qstream-support-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z"))
	}
	abs, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolve support bundle output %q: %w", output, err)
	}
	return filepath.Clean(abs), nil
}

func (c *SupportBundleCmd) resolveConfigDir(dataDir string) string {
	if strings.TrimSpace(c.ConfigDir) != "" {
		resolved, err := resolveOptionalLocalPath(c.ConfigDir)
		if err != nil {
			return filepath.Clean(c.ConfigDir)
		}
		return resolved
	}
	candidates := []string{}
	if dataDir != "" {
		candidates = append(candidates, filepath.Join(dataDir, "config"))
	}
	candidates = append(candidates, "config")
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			abs, err := filepath.Abs(candidate)
			if err == nil {
				return filepath.Clean(abs)
			}
			return filepath.Clean(candidate)
		}
	}
	if len(candidates) > 0 {
		abs, err := filepath.Abs(candidates[0])
		if err == nil {
			return filepath.Clean(abs)
		}
		return filepath.Clean(candidates[0])
	}
	return ""
}

func (c *SupportBundleCmd) resolveWALPath(dataDir string) string {
	if strings.TrimSpace(c.WALPath) != "" {
		resolved, err := resolveOptionalLocalPath(c.WALPath)
		if err != nil {
			return filepath.Clean(c.WALPath)
		}
		return resolved
	}
	if dataDir == "" {
		return ""
	}
	candidate := filepath.Join(dataDir, "storage.wal")
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		abs, err := filepath.Abs(candidate)
		if err == nil {
			return filepath.Clean(abs)
		}
		return filepath.Clean(candidate)
	}
	return ""
}

func (c *SupportBundleCmd) addBackupManifests(archive *supportBundleArchive) {
	if len(c.BackupSource) == 0 {
		archive.AddText("backups/skipped.txt", "backup_manifest_sources=0\n")
		return
	}
	for i, source := range c.BackupSource {
		name := fmt.Sprintf("backups/backup-%03d-manifest.json", i+1)
		manifest, dir, err := core.LoadLocalStorageBackupManifest(source)
		if err != nil {
			archive.AddText(fmt.Sprintf("backups/backup-%03d-error.txt", i+1), fmt.Sprintf("backup_source=%s\nbackup_manifest_error=%v\n", source, err))
			continue
		}
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			archive.AddText(fmt.Sprintf("backups/backup-%03d-error.txt", i+1), fmt.Sprintf("backup_source=%s\nbackup_dir=%s\nbackup_manifest_encode_error=%v\n", source, dir, err))
			continue
		}
		archive.AddBytes(name, append(data, '\n'))
	}
}

func (c *SupportBundleCmd) addLogs(archive *supportBundleArchive, maxLogBytes int64) {
	if len(c.LogPath) == 0 {
		archive.AddText("logs/skipped.txt", "log_paths=0\n")
		return
	}
	seen := map[string]int{}
	for _, path := range c.LogPath {
		resolved, err := resolveOptionalLocalPath(path)
		if err != nil {
			resolved = path
		}
		base := safeSupportBundleName(filepath.Base(resolved))
		seen[base]++
		if seen[base] > 1 {
			ext := filepath.Ext(base)
			stem := strings.TrimSuffix(base, ext)
			base = fmt.Sprintf("%s-%d%s", stem, seen[base], ext)
		}
		body, err := readSupportLogTail(resolved, maxLogBytes)
		if err != nil {
			archive.AddText("logs/"+base+".error.txt", fmt.Sprintf("log_path=%s\nlog_error=%v\n", resolved, err))
			continue
		}
		archive.AddBytes("logs/"+base, body)
	}
}

func supportBundleReadme() string {
	return strings.Join([]string{
		"QuantaStream support bundle",
		"",
		"This archive is designed for diagnostics. It includes build/runtime metadata,",
		"catalog/config summaries, optional WAL planning output, backup manifests,",
		"optional log tails, and best-effort cluster service-discovery status.",
		"",
		"It intentionally does not include table data files or raw auth/access policy files.",
		"Security inputs are represented only by redacted validation/count summaries.",
		"",
	}, "\n")
}

func supportBundleVersion() string {
	return fmt.Sprintf("product=%s\nproduct_short=%s\nversion=%s\ncommit=%s\nbuild_date=%s\nsummary=%s\n",
		version.ProductName,
		version.ShortName,
		version.Version,
		version.Commit,
		version.BuildDate,
		version.Summary(),
	)
}

func supportBundleRuntime(ctx *Context, dataDir, configDir, walPath string, dataDirErr error) string {
	host, _ := os.Hostname()
	cwd, _ := os.Getwd()
	consulAddr := ""
	port := 0
	debug := false
	if ctx != nil {
		consulAddr = ctx.ConsulAddr
		port = ctx.Port
		debug = ctx.Debug
	}
	var b strings.Builder
	fmt.Fprintf(&b, "generated_at=%s\n", time.Now().UTC().Format(time.RFC3339Nano))
	fmt.Fprintf(&b, "hostname=%s\n", host)
	fmt.Fprintf(&b, "cwd=%s\n", cwd)
	fmt.Fprintf(&b, "go_os=%s\n", runtime.GOOS)
	fmt.Fprintf(&b, "go_arch=%s\n", runtime.GOARCH)
	fmt.Fprintf(&b, "consul_addr=%s\n", consulAddr)
	fmt.Fprintf(&b, "admin_port=%d\n", port)
	fmt.Fprintf(&b, "debug=%t\n", debug)
	fmt.Fprintf(&b, "data_dir=%s\n", dataDir)
	fmt.Fprintf(&b, "data_dir_exists=%t\n", dirExists(dataDir))
	if dataDirErr != nil {
		fmt.Fprintf(&b, "data_dir_resolve_error=%v\n", dataDirErr)
	}
	fmt.Fprintf(&b, "config_dir=%s\n", configDir)
	fmt.Fprintf(&b, "config_dir_exists=%t\n", dirExists(configDir))
	fmt.Fprintf(&b, "wal_path=%s\n", walPath)
	fmt.Fprintf(&b, "wal_path_exists=%t\n", fileExists(walPath))
	return b.String()
}

func supportBundleConfigSummary(dataDir, configDir string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "data_dir=%s\n", dataDir)
	fmt.Fprintf(&b, "data_dir_exists=%t\n", dirExists(dataDir))
	fmt.Fprintf(&b, "config_dir=%s\n", configDir)
	fmt.Fprintf(&b, "config_dir_exists=%t\n", dirExists(configDir))
	writeCatalogObjectSummary(&b, configDir)
	writeSchemaSummary(&b, configDir, "tables", filepath.Join(configDir, "*", "schema.yaml"))
	writeSchemaSummary(&b, configDir, "views", filepath.Join(configDir, "views", "*.yaml"))
	writeSensitiveFileSummary(&b, configDir)
	return b.String()
}

func supportBundleSecuritySummary(authAccountFile, accessPolicyFile string) string {
	var b strings.Builder
	writeSupportBundleAuthSummary(&b, authAccountFile)
	writeSupportBundleAccessSummary(&b, accessPolicyFile)
	return b.String()
}

func writeSupportBundleAuthSummary(b *strings.Builder, path string) {
	path = strings.TrimSpace(path)
	fmt.Fprintf(b, "auth_account_file_configured=%t\n", path != "")
	if path == "" {
		return
	}
	resolved, err := resolveOptionalLocalPath(path)
	if err != nil {
		resolved = filepath.Clean(path)
	}
	fmt.Fprintf(b, "auth_account_file=%s\n", resolved)
	fmt.Fprintf(b, "auth_account_file_exists=%t\n", fileExists(resolved))
	accounts, err := qsmysql.LoadStaticAccountFile(resolved)
	if err != nil {
		fmt.Fprintf(b, "auth_account_file_valid=false\n")
		fmt.Fprintf(b, "auth_account_file_error=%v\n", err)
		return
	}
	fmt.Fprintf(b, "auth_account_file_valid=true\n")
	fmt.Fprintf(b, "auth_account_count=%d\n", len(accounts))
	cleartext := 0
	mysqlNative := 0
	cachingSHA2 := 0
	roleBindings := 0
	for _, account := range accounts {
		if account.Password != "" {
			cleartext++
		}
		if account.MySQLNativePasswordVerifier != "" {
			mysqlNative++
		}
		if account.CachingSHA2PasswordVerifier != "" {
			cachingSHA2++
		}
		roleBindings += len(account.Roles)
	}
	fmt.Fprintf(b, "auth_accounts_with_cleartext_password=%d\n", cleartext)
	fmt.Fprintf(b, "auth_accounts_with_mysql_native_verifier=%d\n", mysqlNative)
	fmt.Fprintf(b, "auth_accounts_with_caching_sha2_verifier=%d\n", cachingSHA2)
	fmt.Fprintf(b, "auth_role_binding_count=%d\n", roleBindings)
}

func writeSupportBundleAccessSummary(b *strings.Builder, path string) {
	path = strings.TrimSpace(path)
	fmt.Fprintf(b, "access_policy_file_configured=%t\n", path != "")
	if path == "" {
		return
	}
	resolved, err := resolveOptionalLocalPath(path)
	if err != nil {
		resolved = filepath.Clean(path)
	}
	fmt.Fprintf(b, "access_policy_file=%s\n", resolved)
	fmt.Fprintf(b, "access_policy_file_exists=%t\n", fileExists(resolved))
	policy, err := qsbridge.LoadAccessPolicyFile(resolved)
	if err != nil {
		fmt.Fprintf(b, "access_policy_file_valid=false\n")
		fmt.Fprintf(b, "access_policy_file_error=%v\n", err)
		return
	}
	grants := policy.Grants()
	fmt.Fprintf(b, "access_policy_file_valid=true\n")
	fmt.Fprintf(b, "access_grant_count=%d\n", len(grants))
	principalKinds := map[qsbridge.AccessPrincipalKind]int{}
	privileges := map[qsbridge.AccessPrivilege]int{}
	wildcardTables := 0
	columnScoped := 0
	schemaAgnostic := 0
	for _, grant := range grants {
		principalKinds[grant.PrincipalKind]++
		privileges[grant.Privilege]++
		if grant.Table.Schema == "" || grant.Table.Schema == "*" {
			schemaAgnostic++
		}
		if grant.Table.Table == "*" {
			wildcardTables++
		}
		if len(grant.Fields) > 0 {
			columnScoped++
		}
	}
	fmt.Fprintf(b, "access_principal_kind_user_count=%d\n", principalKinds[qsbridge.AccessPrincipalUser])
	fmt.Fprintf(b, "access_principal_kind_role_count=%d\n", principalKinds[qsbridge.AccessPrincipalRole])
	for _, privilege := range []qsbridge.AccessPrivilege{
		qsbridge.AccessSelect,
		qsbridge.AccessInsert,
		qsbridge.AccessUpdate,
		qsbridge.AccessDelete,
		qsbridge.AccessTruncate,
		qsbridge.AccessCreate,
		qsbridge.AccessDrop,
	} {
		fmt.Fprintf(b, "access_privilege_%s_count=%d\n", privilege, privileges[privilege])
	}
	fmt.Fprintf(b, "access_schema_agnostic_grant_count=%d\n", schemaAgnostic)
	fmt.Fprintf(b, "access_wildcard_table_grant_count=%d\n", wildcardTables)
	fmt.Fprintf(b, "access_column_scoped_grant_count=%d\n", columnScoped)
}

func writeCatalogObjectSummary(b *strings.Builder, configDir string) {
	path := filepath.Join(configDir, "CATALOG_OBJECTS")
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(b, "catalog_objects_exists=false\n")
		return
	}
	fmt.Fprintf(b, "catalog_objects_exists=%t\n", !info.IsDir())
	if !info.IsDir() {
		fmt.Fprintf(b, "catalog_objects_size=%d\n", info.Size())
	}
}

func writeSchemaSummary(b *strings.Builder, configDir, label, pattern string) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Fprintf(b, "%s_glob_error=%v\n", label, err)
		return
	}
	sort.Strings(matches)
	fmt.Fprintf(b, "%s_schema_count=%d\n", label, len(matches))
	for _, path := range matches {
		rel := path
		if r, err := filepath.Rel(configDir, path); err == nil {
			rel = filepath.ToSlash(r)
		}
		info, err := os.Stat(path)
		if err != nil {
			fmt.Fprintf(b, "%s_schema=%s stat_error=%v\n", label, rel, err)
			continue
		}
		fmt.Fprintf(b, "%s_schema=%s bytes=%d\n", label, rel, info.Size())
	}
}

func writeSensitiveFileSummary(b *strings.Builder, configDir string) {
	patterns := []struct {
		label   string
		pattern string
	}{
		{label: "auth", pattern: filepath.Join(configDir, "*auth*")},
		{label: "access", pattern: filepath.Join(configDir, "*access*")},
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern.pattern)
		if err != nil {
			fmt.Fprintf(b, "%s_file_glob_error=%v\n", pattern.label, err)
			continue
		}
		sort.Strings(matches)
		fmt.Fprintf(b, "%s_file_count=%d\n", pattern.label, len(matches))
		for _, path := range matches {
			info, err := os.Stat(path)
			if err != nil || info.IsDir() {
				continue
			}
			fmt.Fprintf(b, "%s_file=%s bytes=%d included=false\n", pattern.label, filepath.Base(path), info.Size())
		}
	}
}

func supportBundleWALPlan(walPath string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "wal_plan_path=%s\n", walPath)
	plan, err := core.PlanLocalWALRecovery(walPath)
	if err != nil {
		fmt.Fprintf(&b, "wal_plan_error=%v\n", err)
		return b.String()
	}
	printWALRecoveryPlanTo(&b, plan)
	return b.String()
}

func supportBundleClusterStatus(ctx *Context) string {
	consulAddr := "127.0.0.1:8500"
	if ctx != nil && strings.TrimSpace(ctx.ConsulAddr) != "" {
		consulAddr = strings.TrimSpace(ctx.ConsulAddr)
	}
	cfg := api.DefaultConfig()
	cfg.Address = consulAddr
	cfg.HttpClient = &http.Client{Timeout: 2 * time.Second}
	consul, err := api.NewClient(cfg)
	if err != nil {
		return fmt.Sprintf("cluster_status_error=create_consul_client: %v\n", err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_status_source=consul_service_discovery\n")
	fmt.Fprintf(&b, "consul_addr=%s\n", consulAddr)
	if target, err := shared.GetClusterSizeTarget(consul); err == nil {
		fmt.Fprintf(&b, "cluster_size_target=%d\n", target)
	} else {
		fmt.Fprintf(&b, "cluster_size_target_error=%v\n", err)
	}
	entries, _, err := consul.Health().Service("quantastream", "", false, nil)
	if err != nil {
		fmt.Fprintf(&b, "cluster_status_error=%v\n", err)
		return b.String()
	}
	fmt.Fprintf(&b, "service_entries=%d\n", len(entries))
	for _, entry := range entries {
		checks := make([]string, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			checks = append(checks, fmt.Sprintf("%s:%s", check.CheckID, check.Status))
		}
		sort.Strings(checks)
		fmt.Fprintf(&b, "service id=%s address=%s port=%d node=%s datacenter=%s checks=%s\n",
			entry.Service.ID,
			entry.Service.Address,
			entry.Service.Port,
			entry.Node.Node,
			entry.Node.Datacenter,
			strings.Join(checks, ","),
		)
	}
	return b.String()
}

func printWALRecoveryPlanTo(w io.Writer, plan core.LocalWALRecoveryPlan) {
	fmt.Fprintf(w, "wal_path=%s\n", plan.WALPath)
	fmt.Fprintf(w, "wal_checkpoint_path=%s\n", plan.CheckpointPath)
	fmt.Fprintf(w, "wal_checkpoint_exists=%t\n", plan.CheckpointExists)
	fmt.Fprintf(w, "wal_checkpoint_lsn=%d\n", plan.CheckpointLSN)
	fmt.Fprintf(w, "wal_last_lsn=%d\n", plan.LastLSN)
	fmt.Fprintf(w, "wal_records=%d\n", plan.RecordCount)
	fmt.Fprintf(w, "wal_checkpointed_records=%d\n", plan.CheckpointedRecordCount)
	fmt.Fprintf(w, "wal_replay_records=%d\n", plan.ReplayRecordCount())
	fmt.Fprintf(w, "wal_pending_records=%d\n", plan.PendingRecordCount())
	fmt.Fprintf(w, "wal_replay_commit_boundaries=%d\n", plan.ReplayCommitBoundaryCount)
	fmt.Fprintf(w, "wal_needs_replay=%t\n", plan.NeedsReplay())
	fmt.Fprintf(w, "wal_has_pending_tail=%t\n", plan.HasPendingTail())
}

type supportBundleArchive struct {
	file    *os.File
	gzip    *gzip.Writer
	tar     *tar.Writer
	entries int
	closed  bool
	err     error
}

func newSupportBundleArchive(output string) (*supportBundleArchive, error) {
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return nil, fmt.Errorf("create support bundle output directory: %w", err)
	}
	file, err := os.Create(output)
	if err != nil {
		return nil, fmt.Errorf("create support bundle %s: %w", output, err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	return &supportBundleArchive{file: file, gzip: gzipWriter, tar: tarWriter}, nil
}

func (a *supportBundleArchive) AddText(name, body string) {
	a.AddBytes(name, []byte(body))
}

func (a *supportBundleArchive) AddBytes(name string, body []byte) {
	if a.closed || a.err != nil {
		return
	}
	name = cleanSupportBundleEntryName(name)
	header := &tar.Header{
		Name:    name,
		Mode:    0644,
		Size:    int64(len(body)),
		ModTime: time.Now().UTC(),
	}
	if err := a.tar.WriteHeader(header); err != nil {
		a.err = fmt.Errorf("write support bundle entry %s header: %w", name, err)
		return
	}
	if _, err := a.tar.Write(body); err != nil {
		a.err = fmt.Errorf("write support bundle entry %s: %w", name, err)
		return
	}
	a.entries++
}

func (a *supportBundleArchive) EntryCount() int {
	return a.entries
}

func (a *supportBundleArchive) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	var closeErr error
	if err := a.tar.Close(); err != nil {
		closeErr = err
	}
	if err := a.gzip.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	if err := a.file.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	if closeErr == nil {
		closeErr = a.err
	}
	return closeErr
}

func resolveOptionalLocalPath(value string) (string, error) {
	resolved, err := core.ResolveLocalFileTarget(value)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func readSupportLogTail(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if maxBytes <= 0 || info.Size() <= maxBytes {
		return io.ReadAll(file)
	}
	if _, err := file.Seek(info.Size()-maxBytes, io.SeekStart); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("[truncated to last %d bytes from %s]\n", maxBytes, path)
	return append([]byte(prefix), body...), nil
}

func cleanSupportBundleEntryName(name string) string {
	name = filepath.ToSlash(filepath.Clean(strings.TrimSpace(name)))
	for strings.HasPrefix(name, "/") || strings.HasPrefix(name, "../") {
		name = strings.TrimPrefix(name, "/")
		name = strings.TrimPrefix(name, "../")
	}
	if name == "" || name == "." || strings.HasPrefix(name, "..") {
		return "unnamed.txt"
	}
	return name
}

func safeSupportBundleName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(name)
}

func dirExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func fileExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
