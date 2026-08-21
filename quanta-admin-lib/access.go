package admin

import (
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// AccessCmd groups local static SQL access-policy file operations.
type AccessCmd struct {
	List   AccessListCmd   `cmd:"" help:"List grants in a static SQL access policy file."`
	Upsert AccessUpsertCmd `cmd:"" help:"Create or update one static SQL access grant."`
	Remove AccessRemoveCmd `cmd:"" help:"Remove one static SQL access grant."`
}

type AccessListCmd struct {
	PolicyFile string `help:"YAML static SQL access policy file." required:""`
}

type AccessUpsertCmd struct {
	PolicyFile    string `help:"YAML static SQL access policy file." required:""`
	PrincipalKind string `help:"Grant subject kind: user or role." required:""`
	Principal     string `help:"Grant subject name." required:""`
	Privilege     string `help:"SQL privilege: select, insert, update, delete, truncate, create, or drop." required:""`
	Schema        string `help:"Database/schema name."`
	Table         string `help:"Table name." required:""`
	Fields        string `help:"Optional comma-separated field list for column-scoped grants."`
}

type AccessRemoveCmd struct {
	PolicyFile    string `help:"YAML static SQL access policy file." required:""`
	PrincipalKind string `help:"Grant subject kind: user or role." required:""`
	Principal     string `help:"Grant subject name." required:""`
	Privilege     string `help:"SQL privilege." required:""`
	Schema        string `help:"Database/schema name."`
	Table         string `help:"Table name." required:""`
}

func (c *AccessListCmd) Run(ctx *Context) error {
	policy, err := qsbridge.LoadAccessPolicyFile(c.PolicyFile)
	if err != nil {
		return err
	}
	printAccessGrants(c.PolicyFile, policy.Grants())
	return nil
}

func (c *AccessUpsertCmd) Run(ctx *Context) error {
	grant, err := accessCommandGrant(c.PrincipalKind, c.Principal, c.Privilege, c.Schema, c.Table, c.Fields)
	if err != nil {
		return err
	}
	grants, err := qsbridge.UpsertAccessPolicyFile(c.PolicyFile, grant)
	if err != nil {
		return err
	}
	fmt.Printf("access_grant_upserted=%s\n", accessGrantKey(grant))
	printAccessGrants(c.PolicyFile, grants)
	return nil
}

func (c *AccessRemoveCmd) Run(ctx *Context) error {
	grant, err := accessCommandGrant(c.PrincipalKind, c.Principal, c.Privilege, c.Schema, c.Table, "")
	if err != nil {
		return err
	}
	grants, removed, err := qsbridge.RemoveAccessPolicyFile(c.PolicyFile, grant)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("access policy grant %q not found", accessGrantKey(grant))
	}
	fmt.Printf("access_grant_removed=%s\n", accessGrantKey(grant))
	printAccessGrants(c.PolicyFile, grants)
	return nil
}

func accessCommandGrant(principalKind, principal, privilege, schema, table, fields string) (qsbridge.AccessGrant, error) {
	parsedFields, err := parseAccessFields(fields)
	if err != nil {
		return qsbridge.AccessGrant{}, err
	}
	return qsbridge.AccessGrant{
		PrincipalKind: qsbridge.AccessPrincipalKind(strings.ToLower(strings.TrimSpace(principalKind))),
		Principal:     strings.TrimSpace(principal),
		Privilege:     qsbridge.AccessPrivilege(strings.ToLower(strings.TrimSpace(privilege))),
		Table: qsbridge.TableInstance{
			Schema: strings.TrimSpace(schema),
			Table:  strings.TrimSpace(table),
		},
		Fields: parsedFields,
	}, nil
}

func parseAccessFields(value string) ([]qsbridge.FieldRef, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	fields := make([]qsbridge.FieldRef, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field == "" {
			return nil, fmt.Errorf("access field is empty")
		}
		if _, ok := seen[field]; ok {
			return nil, fmt.Errorf("access field %q is duplicated", field)
		}
		seen[field] = struct{}{}
		fields = append(fields, qsbridge.FieldRef{Name: field})
	}
	return fields, nil
}

func printAccessGrants(path string, grants []qsbridge.AccessGrant) {
	fmt.Printf("access_policy_file=%s\n", path)
	fmt.Printf("access_grant_count=%d\n", len(grants))
	for _, grant := range grants {
		fmt.Printf(
			"access_grant principal_kind=%s principal=%s privilege=%s schema=%s table=%s fields=%s\n",
			grant.PrincipalKind,
			grant.Principal,
			grant.Privilege,
			grant.Table.Schema,
			grant.Table.Table,
			accessFieldSummary(grant.Fields),
		)
	}
}

func accessGrantKey(grant qsbridge.AccessGrant) string {
	return fmt.Sprintf(
		"%s/%s/%s/%s.%s",
		grant.PrincipalKind,
		grant.Principal,
		grant.Privilege,
		grant.Table.Schema,
		grant.Table.Table,
	)
}

func accessFieldSummary(fields []qsbridge.FieldRef) string {
	if len(fields) == 0 {
		return ""
	}
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.PhysicalName != "" {
			values = append(values, field.PhysicalName)
			continue
		}
		values = append(values, field.Name)
	}
	return strings.Join(values, ",")
}
