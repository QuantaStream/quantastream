package shared

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
)

const (
	// CatalogViewsDirName contains file-backed logical view definitions.
	CatalogViewsDirName = "views"
	// ConsulCatalogViewsPrefix contains distributed logical view definitions.
	ConsulCatalogViewsPrefix = "catalog/views"
)

// ViewDefinition stores a logical, non-materialized SQL view definition.
type ViewDefinition struct {
	SchemaName       string                 `yaml:"schema_name"`
	ViewName         string                 `yaml:"view_name"`
	SQL              string                 `yaml:"sql"`
	CanonicalSQL     string                 `yaml:"canonical_sql,omitempty"`
	Columns          []ViewColumnDefinition `yaml:"columns,omitempty"`
	Dependencies     []ViewDependency       `yaml:"dependencies,omitempty"`
	CreationDate     time.Time              `yaml:"creation_date"`
	ModificationDate time.Time              `yaml:"modification_date"`
}

// ViewColumnDefinition records optional projected column metadata for a view.
type ViewColumnDefinition struct {
	Name string `yaml:"name"`
	Type string `yaml:"type,omitempty"`
}

// ViewDependency records an object referenced by a view definition.
type ViewDependency struct {
	SchemaName string `yaml:"schema_name,omitempty"`
	ObjectName string `yaml:"object_name"`
	ObjectType string `yaml:"object_type"`
}

// LoadViewDefinition loads a logical view definition from configDir/views.
func LoadViewDefinition(configDir string, viewName string) (ViewDefinition, error) {
	var view ViewDefinition
	path, err := catalogViewDefinitionPath(configDir, viewName)
	if err != nil {
		return view, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return view, fmt.Errorf("load view definition %s: %w", viewName, err)
	}
	if err := yaml.Unmarshal(data, &view); err != nil {
		return view, fmt.Errorf("parse view definition %s: %w", viewName, err)
	}
	if strings.TrimSpace(view.ViewName) == "" {
		view.ViewName = strings.TrimSpace(viewName)
	}
	if !strings.EqualFold(strings.TrimSpace(view.ViewName), strings.TrimSpace(viewName)) {
		return view, fmt.Errorf("view definition name %q does not match requested view %q", view.ViewName, viewName)
	}
	if err := ValidateViewDefinition(view); err != nil {
		return view, err
	}
	return view, nil
}

// SaveViewDefinition writes a logical view definition under configDir/views.
func SaveViewDefinition(configDir string, view ViewDefinition) error {
	view.ViewName = strings.TrimSpace(view.ViewName)
	view.SchemaName = strings.TrimSpace(view.SchemaName)
	if view.CreationDate.IsZero() {
		view.CreationDate = time.Now().UTC()
	}
	if view.ModificationDate.IsZero() {
		view.ModificationDate = view.CreationDate
	}
	if err := ValidateViewDefinition(view); err != nil {
		return err
	}
	path, err := catalogViewDefinitionPath(configDir, view.ViewName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create view definition directory: %w", err)
	}
	data, err := yaml.Marshal(view)
	if err != nil {
		return fmt.Errorf("marshal view definition %s: %w", view.ViewName, err)
	}
	return os.WriteFile(path, data, 0644)
}

// RemoveViewDefinition removes a logical view definition from configDir/views.
func RemoveViewDefinition(configDir string, viewName string) error {
	path, err := catalogViewDefinitionPath(configDir, viewName)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove view definition %s: %w", viewName, err)
	}
	return nil
}

// ViewDefinitionExists reports whether a logical view definition exists.
func ViewDefinitionExists(configDir string, viewName string) bool {
	path, err := catalogViewDefinitionPath(configDir, viewName)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// MarshalViewConsul writes a logical view definition to Consul.
func MarshalViewConsul(view ViewDefinition, consul *api.Client) error {
	if consul == nil {
		return fmt.Errorf("consul client must not be nil")
	}
	view.ViewName = strings.TrimSpace(view.ViewName)
	view.SchemaName = strings.TrimSpace(view.SchemaName)
	if view.CreationDate.IsZero() {
		view.CreationDate = time.Now().UTC()
	}
	if view.ModificationDate.IsZero() {
		view.ModificationDate = view.CreationDate
	}
	if err := ValidateViewDefinition(view); err != nil {
		return err
	}
	data, err := yaml.Marshal(view)
	if err != nil {
		return fmt.Errorf("marshal view definition %s: %w", view.ViewName, err)
	}
	_, err = consul.KV().Put(&api.KVPair{
		Key:   consulViewDefinitionPath(view.ViewName),
		Value: data,
	}, nil)
	if err != nil {
		return fmt.Errorf("write view definition %s to consul: %w", view.ViewName, err)
	}
	return nil
}

// LoadViewDefinitionConsul loads a logical view definition from Consul.
func LoadViewDefinitionConsul(consul *api.Client, viewName string) (ViewDefinition, error) {
	var view ViewDefinition
	if consul == nil {
		return view, fmt.Errorf("consul client must not be nil")
	}
	if err := validateCatalogViewFileName(viewName); err != nil {
		return view, err
	}
	pair, _, err := consul.KV().Get(consulViewDefinitionPath(viewName), nil)
	if err != nil {
		return view, fmt.Errorf("load view definition %s from consul: %w", viewName, err)
	}
	if pair == nil {
		return view, os.ErrNotExist
	}
	if err := yaml.Unmarshal(pair.Value, &view); err != nil {
		return view, fmt.Errorf("parse view definition %s from consul: %w", viewName, err)
	}
	if strings.TrimSpace(view.ViewName) == "" {
		view.ViewName = strings.TrimSpace(viewName)
	}
	if !strings.EqualFold(strings.TrimSpace(view.ViewName), strings.TrimSpace(viewName)) {
		return view, fmt.Errorf("view definition name %q does not match requested view %q", view.ViewName, viewName)
	}
	if err := ValidateViewDefinition(view); err != nil {
		return view, err
	}
	return view, nil
}

// ViewExists reports whether a logical view definition exists in Consul.
func ViewExists(consul *api.Client, viewName string) (bool, error) {
	if consul == nil {
		return false, fmt.Errorf("consul client must not be nil")
	}
	if err := validateCatalogViewFileName(viewName); err != nil {
		return false, err
	}
	pair, _, err := consul.KV().Get(consulViewDefinitionPath(viewName), nil)
	if err != nil {
		return false, fmt.Errorf("check view %s in consul: %w", viewName, err)
	}
	return pair != nil, nil
}

// DeleteView removes a logical view definition from Consul.
func DeleteView(consul *api.Client, viewName string) error {
	if consul == nil {
		return fmt.Errorf("consul client must not be nil")
	}
	if err := validateCatalogViewFileName(viewName); err != nil {
		return err
	}
	_, err := consul.KV().DeleteTree(path.Join(ConsulCatalogViewsPrefix, strings.TrimSpace(viewName))+"/", nil)
	if err != nil {
		return fmt.Errorf("delete view %s from consul: %w", viewName, err)
	}
	return nil
}

// CheckViewDependenciesInCatalog returns active file-backed views that depend on tableName.
func CheckViewDependenciesInCatalog(configDir string, schemaName string, tableName string) ([]string, error) {
	views, err := ActiveCatalogViews(configDir, schemaName)
	if err != nil {
		return nil, err
	}
	dependencies := make([]string, 0)
	for _, viewName := range views {
		view, err := LoadViewDefinition(configDir, viewName)
		if err != nil {
			return nil, err
		}
		if viewDependsOnTable(view, schemaName, tableName) {
			dependencies = append(dependencies, viewName)
		}
	}
	sort.Strings(dependencies)
	return dependencies, nil
}

// CheckViewDependencies returns active Consul-backed views that depend on tableName.
func CheckViewDependencies(consul *api.Client, schemaName string, tableName string) ([]string, error) {
	if consul == nil {
		return nil, fmt.Errorf("consul client must not be nil")
	}
	pairs, _, err := consul.KV().List(ConsulCatalogViewsPrefix+"/", nil)
	if err != nil {
		return nil, fmt.Errorf("list catalog views from consul: %w", err)
	}
	dependencies := make([]string, 0)
	for _, pair := range pairs {
		if pair == nil || path.Base(pair.Key) != "definition.yaml" {
			continue
		}
		var view ViewDefinition
		if err := yaml.Unmarshal(pair.Value, &view); err != nil {
			return nil, fmt.Errorf("parse view definition %s from consul: %w", pair.Key, err)
		}
		if strings.TrimSpace(view.ViewName) == "" {
			view.ViewName = path.Base(path.Dir(pair.Key))
		}
		if err := ValidateViewDefinition(view); err != nil {
			return nil, err
		}
		if viewDependsOnTable(view, schemaName, tableName) {
			dependencies = append(dependencies, view.ViewName)
		}
	}
	sort.Strings(dependencies)
	return dependencies, nil
}

// ValidateViewDefinition applies filesystem catalog checks for a logical view.
func ValidateViewDefinition(view ViewDefinition) error {
	if strings.TrimSpace(view.ViewName) == "" {
		return fmt.Errorf("view name must not be empty")
	}
	if err := validateCatalogViewFileName(view.ViewName); err != nil {
		return err
	}
	if strings.TrimSpace(view.SQL) == "" {
		return fmt.Errorf("view SQL must not be empty")
	}
	for _, dependency := range view.Dependencies {
		if strings.TrimSpace(dependency.ObjectName) == "" {
			return fmt.Errorf("view dependency object name must not be empty")
		}
		if strings.TrimSpace(dependency.ObjectType) == "" {
			return fmt.Errorf("view dependency object type must not be empty")
		}
	}
	return nil
}

func viewDependsOnTable(view ViewDefinition, schemaName string, tableName string) bool {
	for _, dependency := range view.Dependencies {
		if !strings.EqualFold(strings.TrimSpace(dependency.ObjectType), CatalogObjectTypeTable) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(dependency.ObjectName), strings.TrimSpace(tableName)) {
			continue
		}
		dependencySchema := strings.TrimSpace(dependency.SchemaName)
		if schemaName == "" || dependencySchema == "" || strings.EqualFold(dependencySchema, schemaName) {
			return true
		}
	}
	return false
}

func catalogViewDefinitionPath(configDir string, viewName string) (string, error) {
	if err := validateCatalogViewFileName(viewName); err != nil {
		return "", err
	}
	return filepath.Join(configDir, CatalogViewsDirName, strings.TrimSpace(viewName)+".yaml"), nil
}

func consulViewDefinitionPath(viewName string) string {
	return path.Join(ConsulCatalogViewsPrefix, strings.TrimSpace(viewName), "definition.yaml")
}

func validateCatalogViewFileName(viewName string) error {
	name := strings.TrimSpace(viewName)
	if name == "" {
		return fmt.Errorf("view name must not be empty")
	}
	if name == "." || name == ".." || strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return fmt.Errorf("view name %q is not a valid catalog file name", viewName)
	}
	return nil
}
