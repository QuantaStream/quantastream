package qsbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

// AccessPolicyFile is the YAML shape for file-backed static SQL authorization.
type AccessPolicyFile struct {
	Grants []AccessGrantFile `yaml:"grants"`
}

// AccessGrantFile is one file-backed static access grant.
type AccessGrantFile struct {
	PrincipalKind AccessPrincipalKind `yaml:"principal_kind"`
	Principal     string              `yaml:"principal"`
	Privilege     AccessPrivilege     `yaml:"privilege"`
	Schema        string              `yaml:"schema,omitempty"`
	Table         string              `yaml:"table"`
	Fields        []string            `yaml:"fields,omitempty"`
}

// LoadAccessPolicyFile loads a YAML static SQL authorization policy.
func LoadAccessPolicyFile(path string) (AccessPolicy, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return AccessPolicy{}, fmt.Errorf("access policy file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AccessPolicy{}, err
	}
	return DecodeAccessPolicyFile(data)
}

// DecodeAccessPolicyFile decodes and validates a static SQL authorization file.
func DecodeAccessPolicyFile(data []byte) (AccessPolicy, error) {
	var file AccessPolicyFile
	if err := yaml.UnmarshalStrict(data, &file); err != nil {
		return AccessPolicy{}, err
	}
	grants, err := validateAccessGrantFiles(file.Grants)
	if err != nil {
		return AccessPolicy{}, err
	}
	return NewAccessPolicy(grants...), nil
}

// SaveAccessPolicyFile writes grants to a YAML static authorization file.
func SaveAccessPolicyFile(path string, grants []AccessGrant) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("access policy file path is empty")
	}
	files, err := accessGrantFilesFromGrants(grants)
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(AccessPolicyFile{Grants: files})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// UpsertAccessPolicyFile inserts or replaces one static access grant.
func UpsertAccessPolicyFile(path string, grant AccessGrant) ([]AccessGrant, error) {
	var grants []AccessGrant
	if _, err := os.Stat(path); err == nil {
		policy, err := LoadAccessPolicyFile(path)
		if err != nil {
			return nil, err
		}
		grants = policy.Grants()
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	normalized, err := validateAccessGrant(grant)
	if err != nil {
		return nil, err
	}
	replaced := false
	for i := range grants {
		if accessGrantSamePolicySlot(grants[i], normalized) {
			grants[i] = normalized
			replaced = true
			break
		}
	}
	if !replaced {
		grants = append(grants, normalized)
	}
	if err := SaveAccessPolicyFile(path, grants); err != nil {
		return nil, err
	}
	return grants, nil
}

// RemoveAccessPolicyFile removes one static access grant identified by subject,
// privilege, and table.
func RemoveAccessPolicyFile(path string, grant AccessGrant) ([]AccessGrant, bool, error) {
	policy, err := LoadAccessPolicyFile(path)
	if err != nil {
		return nil, false, err
	}
	target, err := validateAccessGrant(grant)
	if err != nil {
		return nil, false, err
	}
	grants := policy.Grants()
	remaining := grants[:0]
	removed := false
	for _, current := range grants {
		if accessGrantSamePolicySlot(current, target) {
			removed = true
			continue
		}
		remaining = append(remaining, current)
	}
	if !removed {
		return grants, false, nil
	}
	if len(remaining) == 0 {
		return nil, false, fmt.Errorf("cannot remove the last access policy grant")
	}
	if err := SaveAccessPolicyFile(path, remaining); err != nil {
		return nil, false, err
	}
	return remaining, true, nil
}

func validateAccessGrantFiles(files []AccessGrantFile) ([]AccessGrant, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("access policy file has no grants")
	}
	grants := make([]AccessGrant, 0, len(files))
	for i, file := range files {
		grant, err := validateAccessGrantFile(file)
		if err != nil {
			return nil, fmt.Errorf("access policy grant %d: %w", i+1, err)
		}
		grants = append(grants, grant)
	}
	return grants, nil
}

func validateAccessGrantFile(file AccessGrantFile) (AccessGrant, error) {
	file.PrincipalKind = AccessPrincipalKind(strings.ToLower(strings.TrimSpace(string(file.PrincipalKind))))
	file.Principal = strings.TrimSpace(file.Principal)
	file.Privilege = AccessPrivilege(strings.ToLower(strings.TrimSpace(string(file.Privilege))))
	file.Schema = strings.TrimSpace(file.Schema)
	file.Table = strings.TrimSpace(file.Table)
	if file.PrincipalKind != AccessPrincipalUser && file.PrincipalKind != AccessPrincipalRole {
		return AccessGrant{}, fmt.Errorf("unsupported principal_kind %q", file.PrincipalKind)
	}
	if file.Principal == "" {
		return AccessGrant{}, fmt.Errorf("principal is empty")
	}
	if !validAccessPrivilege(file.Privilege) {
		return AccessGrant{}, fmt.Errorf("unsupported privilege %q", file.Privilege)
	}
	if file.Table == "" {
		return AccessGrant{}, fmt.Errorf("table is empty")
	}
	fields, err := accessGrantFileFields(file.Fields)
	if err != nil {
		return AccessGrant{}, err
	}
	return AccessGrant{
		PrincipalKind: file.PrincipalKind,
		Principal:     file.Principal,
		Privilege:     file.Privilege,
		Table:         TableInstance{Schema: file.Schema, Table: file.Table},
		Fields:        fields,
	}, nil
}

func validateAccessGrant(grant AccessGrant) (AccessGrant, error) {
	file := AccessGrantFile{
		PrincipalKind: grant.PrincipalKind,
		Principal:     grant.Principal,
		Privilege:     grant.Privilege,
		Schema:        grant.Table.Schema,
		Table:         grant.Table.Table,
		Fields:        accessGrantFieldNames(grant.Fields),
	}
	return validateAccessGrantFile(file)
}

func accessGrantFilesFromGrants(grants []AccessGrant) ([]AccessGrantFile, error) {
	normalized, err := validateAccessGrants(grants)
	if err != nil {
		return nil, err
	}
	files := make([]AccessGrantFile, 0, len(normalized))
	for _, grant := range normalized {
		files = append(files, AccessGrantFile{
			PrincipalKind: grant.PrincipalKind,
			Principal:     grant.Principal,
			Privilege:     grant.Privilege,
			Schema:        grant.Table.Schema,
			Table:         grant.Table.Table,
			Fields:        accessGrantFieldNames(grant.Fields),
		})
	}
	return files, nil
}

func validateAccessGrants(grants []AccessGrant) ([]AccessGrant, error) {
	if len(grants) == 0 {
		return nil, fmt.Errorf("access policy file has no grants")
	}
	normalized := make([]AccessGrant, 0, len(grants))
	for i, grant := range grants {
		current, err := validateAccessGrant(grant)
		if err != nil {
			return nil, fmt.Errorf("access policy grant %d: %w", i+1, err)
		}
		normalized = append(normalized, current)
	}
	return normalized, nil
}

func accessGrantFieldNames(fields []FieldRef) []string {
	if len(fields) == 0 {
		return nil
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.PhysicalName != "" {
			names = append(names, field.PhysicalName)
			continue
		}
		names = append(names, field.Name)
	}
	return names
}

func accessGrantSamePolicySlot(left, right AccessGrant) bool {
	return left.PrincipalKind == right.PrincipalKind &&
		left.Principal == right.Principal &&
		left.Privilege == right.Privilege &&
		left.Table.Schema == right.Table.Schema &&
		left.Table.Table == right.Table.Table
}

func accessGrantFileFields(names []string) ([]FieldRef, error) {
	if len(names) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(names))
	fields := make([]FieldRef, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("field is empty")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("field %q is duplicated", name)
		}
		seen[name] = struct{}{}
		fields = append(fields, FieldRef{Name: name})
	}
	return fields, nil
}

func validAccessPrivilege(privilege AccessPrivilege) bool {
	switch privilege {
	case AccessSelect, AccessInsert, AccessUpdate, AccessDelete, AccessTruncate, AccessCreate, AccessDrop:
		return true
	default:
		return false
	}
}
