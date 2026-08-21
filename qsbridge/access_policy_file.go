package qsbridge

import (
	"fmt"
	"os"
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
