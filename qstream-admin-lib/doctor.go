package admin

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// DoctorCmd groups deployment preflight checks.
type DoctorCmd struct {
	Local DoctorLocalCmd `cmd:"" help:"Run local single-node release preflight checks."`
}

type DoctorLocalCmd struct {
	DataDir          string        `help:"QuantaStream data directory to inspect." default:"data"`
	ConfigDir        string        `help:"Config directory to inspect. Defaults to <data-dir>/config when present, then ./configuration or ./config."`
	WALPath          string        `help:"Optional local WAL path to validate. Defaults to <data-dir>/storage.wal when present."`
	AuthAccountFile  string        `help:"Optional static auth account file to validate."`
	AccessPolicyFile string        `help:"Optional static SQL access policy file to validate."`
	BackupSource     []string      `help:"Optional backup source to validate. Use a comma-separated list for multiple backups."`
	MySQLAddr        string        `name:"mysql-addr" help:"Optional MySQL-compatible endpoint host:port that must be reachable."`
	NativeGRPCAddr   string        `name:"native-grpc-addr" help:"Optional native gRPC endpoint host:port that must be reachable."`
	PortTimeout      time.Duration `help:"Timeout for endpoint checks." default:"2s"`
}

type doctorCheckStatus string

const (
	doctorStatusPass doctorCheckStatus = "PASS"
	doctorStatusWarn doctorCheckStatus = "WARN"
	doctorStatusFail doctorCheckStatus = "FAIL"
	doctorStatusSkip doctorCheckStatus = "SKIP"
)

type doctorCheck struct {
	Name   string
	Status doctorCheckStatus
	Detail string
}

func (c *DoctorLocalCmd) Run(ctx *Context) error {
	checks := c.runChecks()
	printDoctorChecks(checks)
	failures, warnings, skips := summarizeDoctorChecks(checks)
	if failures > 0 {
		return fmt.Errorf("doctor local failed checks=%d warnings=%d skips=%d", failures, warnings, skips)
	}
	return nil
}

func (c *DoctorLocalCmd) runChecks() []doctorCheck {
	dataDir, dataErr := resolveDoctorPath(c.DataDir)
	configDir := c.resolveConfigDir(dataDir)
	walPath := c.resolveWALPath(dataDir)
	checks := []doctorCheck{
		doctorDirCheck("data_dir", dataDir, dataErr, doctorStatusWarn),
		doctorDirCheck("config_dir", configDir, nil, doctorStatusFail),
		doctorCatalogCheck(configDir),
		doctorSchemaCountCheck("tables_schema", filepath.Join(configDir, "*", "schema.yaml"), 1),
		doctorSchemaCountCheck("views_schema", filepath.Join(configDir, "views", "*.yaml"), 0),
		doctorWALCheck(walPath),
		doctorAuthCheck(c.AuthAccountFile),
		doctorAccessCheck(c.AccessPolicyFile),
	}
	checks = append(checks, c.backupChecks()...)
	checks = append(checks, doctorEndpointCheck("mysql_endpoint", c.MySQLAddr, c.PortTimeout))
	checks = append(checks, doctorEndpointCheck("native_grpc_endpoint", c.NativeGRPCAddr, c.PortTimeout))
	return checks
}

func (c *DoctorLocalCmd) resolveConfigDir(dataDir string) string {
	if strings.TrimSpace(c.ConfigDir) != "" {
		resolved, err := resolveDoctorPath(c.ConfigDir)
		if err != nil {
			return filepath.Clean(c.ConfigDir)
		}
		return resolved
	}
	candidates := []string{}
	if dataDir != "" {
		candidates = append(candidates, filepath.Join(dataDir, "config"))
	}
	candidates = append(candidates, "configuration", "config")
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			abs, err := filepath.Abs(candidate)
			if err == nil {
				return filepath.Clean(abs)
			}
			return filepath.Clean(candidate)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	abs, err := filepath.Abs(candidates[0])
	if err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(candidates[0])
}

func (c *DoctorLocalCmd) resolveWALPath(dataDir string) string {
	if strings.TrimSpace(c.WALPath) != "" {
		resolved, err := resolveDoctorPath(c.WALPath)
		if err != nil {
			return filepath.Clean(c.WALPath)
		}
		return resolved
	}
	if dataDir == "" {
		return ""
	}
	candidate := filepath.Join(dataDir, "storage.wal")
	if fileExists(candidate) {
		abs, err := filepath.Abs(candidate)
		if err == nil {
			return filepath.Clean(abs)
		}
		return filepath.Clean(candidate)
	}
	return ""
}

func (c *DoctorLocalCmd) backupChecks() []doctorCheck {
	if len(c.BackupSource) == 0 {
		return []doctorCheck{{Name: "backup_source", Status: doctorStatusSkip, Detail: "not configured"}}
	}
	checks := make([]doctorCheck, 0, len(c.BackupSource))
	for i, source := range c.BackupSource {
		name := fmt.Sprintf("backup_source_%03d", i+1)
		manifest, err := core.ValidateLocalStorageBackup(source)
		if err != nil {
			checks = append(checks, doctorCheck{Name: name, Status: doctorStatusFail, Detail: err.Error()})
			continue
		}
		checks = append(checks, doctorCheck{
			Name:   name,
			Status: doctorStatusPass,
			Detail: fmt.Sprintf("name=%s files=%d bytes=%d", manifest.Product.Name, manifest.FileCount, manifest.ByteCount),
		})
	}
	return checks
}

func doctorDirCheck(name, path string, pathErr error, missingStatus doctorCheckStatus) doctorCheck {
	if pathErr != nil {
		return doctorCheck{Name: name, Status: missingStatus, Detail: fmt.Sprintf("resolve_error=%v", pathErr)}
	}
	if path == "" {
		return doctorCheck{Name: name, Status: missingStatus, Detail: "not configured"}
	}
	info, err := os.Stat(path)
	if err != nil {
		return doctorCheck{Name: name, Status: missingStatus, Detail: fmt.Sprintf("missing path=%s", path)}
	}
	if !info.IsDir() {
		return doctorCheck{Name: name, Status: doctorStatusFail, Detail: fmt.Sprintf("not directory path=%s", path)}
	}
	return doctorCheck{Name: name, Status: doctorStatusPass, Detail: fmt.Sprintf("path=%s", path)}
}

func doctorCatalogCheck(configDir string) doctorCheck {
	if configDir == "" || !dirExists(configDir) {
		return doctorCheck{Name: "catalog_objects", Status: doctorStatusSkip, Detail: "config directory unavailable"}
	}
	path := filepath.Join(configDir, "CATALOG_OBJECTS")
	if !fileExists(path) {
		return doctorCheck{Name: "catalog_objects", Status: doctorStatusWarn, Detail: fmt.Sprintf("missing path=%s", path)}
	}
	return doctorCheck{Name: "catalog_objects", Status: doctorStatusPass, Detail: fmt.Sprintf("path=%s", path)}
}

func doctorSchemaCountCheck(name, pattern string, minimum int) doctorCheck {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return doctorCheck{Name: name, Status: doctorStatusFail, Detail: fmt.Sprintf("glob_error=%v", err)}
	}
	sortStrings(matches)
	status := doctorStatusPass
	if len(matches) < minimum {
		status = doctorStatusWarn
	}
	return doctorCheck{Name: name, Status: status, Detail: fmt.Sprintf("count=%d", len(matches))}
}

func doctorWALCheck(path string) doctorCheck {
	if strings.TrimSpace(path) == "" {
		return doctorCheck{Name: "wal", Status: doctorStatusSkip, Detail: "not configured"}
	}
	plan, err := core.PlanLocalWALRecovery(path)
	if err != nil {
		return doctorCheck{Name: "wal", Status: doctorStatusFail, Detail: err.Error()}
	}
	detail := fmt.Sprintf("records=%d replay=%d pending=%d", plan.RecordCount, plan.ReplayRecordCount(), plan.PendingRecordCount())
	status := doctorStatusPass
	if plan.TornTailBytes != 0 {
		status = doctorStatusWarn
		detail += fmt.Sprintf(" torn_tail_bytes=%d", plan.TornTailBytes)
		if plan.TornTailLine != 0 {
			detail += fmt.Sprintf(" torn_tail_line=%d", plan.TornTailLine)
		}
	}
	return doctorCheck{
		Name:   "wal",
		Status: status,
		Detail: detail,
	}
}

func doctorAuthCheck(path string) doctorCheck {
	path = strings.TrimSpace(path)
	if path == "" {
		return doctorCheck{Name: "auth_account_file", Status: doctorStatusSkip, Detail: "not configured"}
	}
	resolved, err := resolveDoctorPath(path)
	if err != nil {
		resolved = filepath.Clean(path)
	}
	accounts, err := qsmysql.LoadStaticAccountFile(resolved)
	if err != nil {
		return doctorCheck{Name: "auth_account_file", Status: doctorStatusFail, Detail: err.Error()}
	}
	return doctorCheck{Name: "auth_account_file", Status: doctorStatusPass, Detail: fmt.Sprintf("accounts=%d", len(accounts))}
}

func doctorAccessCheck(path string) doctorCheck {
	path = strings.TrimSpace(path)
	if path == "" {
		return doctorCheck{Name: "access_policy_file", Status: doctorStatusSkip, Detail: "not configured"}
	}
	resolved, err := resolveDoctorPath(path)
	if err != nil {
		resolved = filepath.Clean(path)
	}
	policy, err := qsbridge.LoadAccessPolicyFile(resolved)
	if err != nil {
		return doctorCheck{Name: "access_policy_file", Status: doctorStatusFail, Detail: err.Error()}
	}
	return doctorCheck{Name: "access_policy_file", Status: doctorStatusPass, Detail: fmt.Sprintf("grants=%d", len(policy.Grants()))}
}

func doctorEndpointCheck(name, addr string, timeout time.Duration) doctorCheck {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return doctorCheck{Name: name, Status: doctorStatusSkip, Detail: "not configured"}
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return doctorCheck{Name: name, Status: doctorStatusFail, Detail: err.Error()}
	}
	_ = conn.Close()
	return doctorCheck{Name: name, Status: doctorStatusPass, Detail: fmt.Sprintf("reachable addr=%s", addr)}
}

func printDoctorChecks(checks []doctorCheck) {
	failures, warnings, skips := summarizeDoctorChecks(checks)
	result := "PASS"
	if failures > 0 {
		result = "FAIL"
	}
	fmt.Println("doctor_scope=local")
	for _, check := range checks {
		fmt.Printf("doctor_check name=%s status=%s detail=%q\n", check.Name, check.Status, check.Detail)
	}
	fmt.Printf("doctor_result=%s failures=%d warnings=%d skips=%d\n", result, failures, warnings, skips)
}

func summarizeDoctorChecks(checks []doctorCheck) (failures, warnings, skips int) {
	for _, check := range checks {
		switch check.Status {
		case doctorStatusFail:
			failures++
		case doctorStatusWarn:
			warnings++
		case doctorStatusSkip:
			skips++
		}
	}
	return failures, warnings, skips
}

func resolveDoctorPath(path string) (string, error) {
	resolved, err := resolveOptionalLocalPath(path)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func sortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
