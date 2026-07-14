package shared

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	// CatalogObjectsFileName is the file-backed active catalog manifest for inabox-standard.
	CatalogObjectsFileName = "CATALOG_OBJECTS"
	// CatalogObjectTypeTable identifies active table objects.
	CatalogObjectTypeTable = "TABLE"
)

// CatalogObjectsFile stores active catalog objects for file-backed deployments.
type CatalogObjectsFile struct {
	Objects []CatalogObjectRecord `yaml:"objects"`
}

// CatalogObjectRecord records one active catalog object.
type CatalogObjectRecord struct {
	SchemaName       string    `yaml:"schema_name"`
	TableName        string    `yaml:"table_name"`
	CreationDate     time.Time `yaml:"creation_date"`
	ModificationDate time.Time `yaml:"modification_date"`
	ObjectType       string    `yaml:"object_type"`
}

// LoadCatalogObjectsFile loads the file-backed active catalog manifest.
func LoadCatalogObjectsFile(configDir string) (CatalogObjectsFile, error) {
	var catalog CatalogObjectsFile
	data, err := os.ReadFile(catalogObjectsPath(configDir))
	if os.IsNotExist(err) {
		return catalog, nil
	}
	if err != nil {
		return catalog, fmt.Errorf("load catalog objects: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return catalog, nil
	}
	if err := yaml.Unmarshal(data, &catalog); err != nil {
		return catalog, fmt.Errorf("parse catalog objects: %w", err)
	}
	return catalog, nil
}

// SaveCatalogObjectsFile persists the active catalog manifest.
func SaveCatalogObjectsFile(configDir string, catalog CatalogObjectsFile) error {
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("create catalog config directory: %w", err)
	}
	sort.SliceStable(catalog.Objects, func(i, j int) bool {
		if !strings.EqualFold(catalog.Objects[i].SchemaName, catalog.Objects[j].SchemaName) {
			return strings.ToLower(catalog.Objects[i].SchemaName) < strings.ToLower(catalog.Objects[j].SchemaName)
		}
		return strings.ToLower(catalog.Objects[i].TableName) < strings.ToLower(catalog.Objects[j].TableName)
	})
	data, err := yaml.Marshal(catalog)
	if err != nil {
		return fmt.Errorf("marshal catalog objects: %w", err)
	}
	return os.WriteFile(catalogObjectsPath(configDir), data, 0644)
}

// CatalogObjectsFileExists reports whether configDir contains an active catalog manifest.
func CatalogObjectsFileExists(configDir string) bool {
	_, err := os.Stat(catalogObjectsPath(configDir))
	return err == nil
}

// ActiveCatalogTables returns active TABLE objects from the file-backed catalog.
func ActiveCatalogTables(configDir string, schemaName string) ([]string, error) {
	catalog, err := LoadCatalogObjectsFile(configDir)
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(catalog.Objects))
	for _, object := range catalog.Objects {
		if !strings.EqualFold(object.ObjectType, CatalogObjectTypeTable) {
			continue
		}
		if schemaName != "" && object.SchemaName != "" && !strings.EqualFold(object.SchemaName, schemaName) {
			continue
		}
		if object.TableName == "" {
			continue
		}
		tables = append(tables, object.TableName)
	}
	sort.Strings(tables)
	return tables, nil
}

// CatalogTableActive reports whether tableName is active in the file-backed catalog.
func CatalogTableActive(configDir string, schemaName string, tableName string) (bool, error) {
	catalog, err := LoadCatalogObjectsFile(configDir)
	if err != nil {
		return false, err
	}
	for _, object := range catalog.Objects {
		if !strings.EqualFold(object.ObjectType, CatalogObjectTypeTable) {
			continue
		}
		if schemaName != "" && object.SchemaName != "" && !strings.EqualFold(object.SchemaName, schemaName) {
			continue
		}
		if strings.EqualFold(object.TableName, tableName) {
			return true, nil
		}
	}
	return false, nil
}

// ActivateCatalogTable marks a table schema as active in the file-backed catalog.
func ActivateCatalogTable(configDir string, schemaName string, tableName string, now time.Time) error {
	if tableName == "" {
		return fmt.Errorf("table name must not be empty")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	catalog, err := LoadCatalogObjectsFile(configDir)
	if err != nil {
		return err
	}
	for i, object := range catalog.Objects {
		if !strings.EqualFold(object.ObjectType, CatalogObjectTypeTable) {
			continue
		}
		if schemaName != "" && object.SchemaName != "" && !strings.EqualFold(object.SchemaName, schemaName) {
			continue
		}
		if strings.EqualFold(object.TableName, tableName) {
			catalog.Objects[i].ModificationDate = now.UTC()
			if catalog.Objects[i].SchemaName == "" {
				catalog.Objects[i].SchemaName = schemaName
			}
			return SaveCatalogObjectsFile(configDir, catalog)
		}
	}
	catalog.Objects = append(catalog.Objects, CatalogObjectRecord{
		SchemaName:       schemaName,
		TableName:        tableName,
		CreationDate:     now.UTC(),
		ModificationDate: now.UTC(),
		ObjectType:       CatalogObjectTypeTable,
	})
	return SaveCatalogObjectsFile(configDir, catalog)
}

// RemoveCatalogTable removes a table from the file-backed active catalog.
func RemoveCatalogTable(configDir string, schemaName string, tableName string) error {
	catalog, err := LoadCatalogObjectsFile(configDir)
	if err != nil {
		return err
	}
	filtered := catalog.Objects[:0]
	for _, object := range catalog.Objects {
		remove := strings.EqualFold(object.ObjectType, CatalogObjectTypeTable) &&
			strings.EqualFold(object.TableName, tableName) &&
			(schemaName == "" || object.SchemaName == "" || strings.EqualFold(object.SchemaName, schemaName))
		if !remove {
			filtered = append(filtered, object)
		}
	}
	catalog.Objects = filtered
	return SaveCatalogObjectsFile(configDir, catalog)
}

// DiscoverSchemaTables returns table directories containing schema.yaml.
func DiscoverSchemaTables(configDir string) ([]string, error) {
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(configDir, entry.Name(), "schema.yaml")); err == nil {
			tables = append(tables, entry.Name())
		}
	}
	sort.Strings(tables)
	return tables, nil
}

// ActiveOrDiscoveredSchemaTables returns active catalog tables when CATALOG_OBJECTS exists.
func ActiveOrDiscoveredSchemaTables(configDir string, schemaName string) ([]string, error) {
	if CatalogObjectsFileExists(configDir) {
		return ActiveCatalogTables(configDir, schemaName)
	}
	return DiscoverSchemaTables(configDir)
}

// ValidateCatalogTableDefinition applies schema checks shared by admin and SQL DDL paths.
func ValidateCatalogTableDefinition(table *BasicTable) error {
	if table == nil {
		return fmt.Errorf("table must not be nil")
	}
	for i := 0; i < len(table.Attributes); i++ {
		typ := TypeFromString(table.Attributes[i].Type)
		if typ == NotDefined && table.Attributes[i].MappingStrategy != "ChildRelation" {
			return fmt.Errorf("unknown type %s for field %s", table.Attributes[i].Type, table.Attributes[i].FieldName)
		}
	}
	return nil
}

// CheckParentRelationInCatalog reports whether all FK parents for table are active.
func CheckParentRelationInCatalog(configDir string, schemaName string, table *BasicTable) (bool, error) {
	if table == nil {
		return false, fmt.Errorf("table must not be nil")
	}
	for _, attribute := range table.Attributes {
		if attribute.ForeignKey == "" {
			continue
		}
		parentTable := foreignKeyTableName(attribute.ForeignKey)
		if parentTable == "" {
			return false, fmt.Errorf("foreign key table name must be specified for %s", attribute.FieldName)
		}
		active, err := CatalogTableActive(configDir, schemaName, parentTable)
		if err != nil {
			return false, err
		}
		if !active {
			return false, nil
		}
	}
	return true, nil
}

// CheckChildRelationInCatalog returns active child tables that reference tableName.
func CheckChildRelationInCatalog(configDir string, schemaName string, tableName string) ([]string, error) {
	tables, err := ActiveCatalogTables(configDir, schemaName)
	if err != nil {
		return nil, err
	}
	dependencies := make([]string, 0)
	for _, candidate := range tables {
		if strings.EqualFold(candidate, tableName) {
			continue
		}
		table, err := LoadSchema(configDir, candidate, nil)
		if err != nil {
			return nil, err
		}
		for _, attribute := range table.Attributes {
			if attribute.ForeignKey == "" {
				continue
			}
			if strings.EqualFold(foreignKeyTableName(attribute.ForeignKey), tableName) {
				dependencies = append(dependencies, candidate)
				break
			}
		}
	}
	sort.Strings(dependencies)
	return dependencies, nil
}

func catalogObjectsPath(configDir string) string {
	return filepath.Join(configDir, CatalogObjectsFileName)
}

func foreignKeyTableName(foreignKey string) string {
	parts := strings.SplitN(strings.TrimSpace(foreignKey), ".", 2)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}
