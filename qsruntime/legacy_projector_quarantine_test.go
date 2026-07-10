package qsruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyProjectorCallsStayQuarantined(t *testing.T) {
	needles := []string{
		"core." + "NewProjection",
		"projector." + "Next",
		"core." + "Projector.Next",
	}
	seen := map[string]map[string]int{}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		text := string(body)
		for _, needle := range needles {
			count := strings.Count(text, needle)
			if count == 0 {
				continue
			}
			if seen[entry.Name()] == nil {
				seen[entry.Name()] = map[string]int{}
			}
			seen[entry.Name()][needle] = count
			t.Fatalf("%s contains %q; qsruntime must not call legacy core.Projector materialization", entry.Name(), needle)
		}
	}
}

func TestLegacyProjectionMaterializerReachabilityIsExplicit(t *testing.T) {
	concreteConstructors := map[string]int{
		"qsruntime/legacy_direct_relationship_join.go": 0,
		"qsruntime/legacy_direct_runtime.go":           0,
		"sqlrunner/legacy_direct_runtime.go":           0,
	}
	for file, want := range concreteConstructors {
		body := readRepoFileForProjectorQuarantine(t, file)
		if got := strings.Count(body, "LegacyQuantaSourceProjectionMaterializer{"); got != want {
			t.Fatalf("%s constructs LegacyQuantaSourceProjectionMaterializer %d times, want %d", file, got, want)
		}
	}

	adapterBoundaries := map[string]string{
		"qsruntime/direct_bitmap_filter_adapter.go":    "return ProjectionMaterializerKernelAdapter{Materializer: a.Materializer}",
		"qsruntime/direct_bitmap_runtime.go":           "return ProjectionMaterializerKernelAdapter{Materializer: r.Materializer}",
		"qsruntime/legacy_direct_relationship_join.go": "return ProjectionMaterializerKernelAdapter{Materializer: e.Materializer}",
	}
	for file, snippet := range adapterBoundaries {
		body := readRepoFileForProjectorQuarantine(t, file)
		if !strings.Contains(body, snippet) {
			t.Fatalf("%s missing explicit materializer adapter boundary %q", file, snippet)
		}
	}
}

func TestLegacyProxyQueryLayerStaysOutOfRefactorRuntime(t *testing.T) {
	forbidden := []string{
		"Sql" + "ToQuanta",
		"SQL" + "ToQuanta",
		"sql" + "_to_quanta",
		"Join" + "Merge",
		"quanta" + "_join",
	}
	for _, dir := range []string{"qsruntime", "sqlrunner"} {
		assertNoLegacyProxyQueryLayerTokens(t, dir, forbidden)
	}
}

func assertNoLegacyProxyQueryLayerTokens(t *testing.T, repoDir string, forbidden []string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("..", repoDir))
	if err != nil {
		t.Fatalf("read %s package: %v", repoDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		repoPath := filepath.Join(repoDir, entry.Name())
		body := readRepoFileForProjectorQuarantine(t, repoPath)
		for _, token := range forbidden {
			if strings.Contains(body, token) {
				t.Fatalf("%s references legacy proxy query-layer token %q; use qsbridge/qsruntime planner and kernels instead", repoPath, token)
			}
		}
	}
}

func readRepoFileForProjectorQuarantine(t *testing.T, repoPath string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", repoPath))
	if err != nil {
		t.Fatalf("read %s: %v", repoPath, err)
	}
	return string(body)
}
