package roadmap

import "strings"

// AdminStatementSQL converts the deprecated roadmap admin shorthand into SQL.
//
// Older suites used commands such as "create customers_qa" when table lifecycle
// operations were routed through quanta-admin. The query engine now owns those
// operations, so SQLRunner keeps the spelling readable while executing SQL.
func AdminStatementSQL(sql string) string {
	sql = strings.TrimSpace(sql)
	fields := strings.Fields(sql)
	if len(fields) < 2 {
		return sql
	}
	keyword := strings.ToLower(strings.Trim(fields[0], "`"))
	second := strings.ToLower(strings.Trim(fields[1], "`"))
	switch keyword {
	case "create":
		if second != "table" {
			return "create table " + strings.TrimSpace(sql[len(fields[0]):])
		}
	case "drop":
		if second != "table" {
			return "drop table " + strings.TrimSpace(sql[len(fields[0]):])
		}
	case "truncate":
		if second != "table" {
			return "truncate table " + strings.TrimSpace(sql[len(fields[0]):])
		}
	}
	return sql
}

func executableSQL(test TestCase) string {
	if test.Kind == "admin" {
		return AdminStatementSQL(test.SQL)
	}
	return test.SQL
}
