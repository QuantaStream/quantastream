package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type smokeQuery struct {
	Label string
	SQL   string
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("mysql-driver-smoke", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	dsn := flags.String("dsn", envString("MYSQL_DSN", "root@tcp(127.0.0.1:4000)/quanta?parseTime=true"), "database/sql MySQL DSN")
	timeout := flags.Duration("timeout", 30*time.Second, "per-smoke timeout")
	maxRows := flags.Int("max-rows", 5, "maximum rows to print per query")
	q3View := flags.Bool("q3-view", true, "run the TPC-H Q3 view smoke when q3_order_line_base is installed")
	prepared := flags.Bool("prepared", true, "run prepared statement smoke queries")
	preparedWrite := flags.Bool("prepared-write", false, "run a cleanup-protected prepared INSERT smoke")
	preparedBatch := flags.Bool("prepared-batch", false, "run a cleanup-protected prepared multi-row INSERT smoke against QA tables when installed")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*dsn) == "" {
		fmt.Fprintln(os.Stderr, "-dsn or MYSQL_DSN is required")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintf(os.Stderr, "-timeout must be positive: %s\n", *timeout)
		return 2
	}
	if *maxRows < 0 {
		fmt.Fprintf(os.Stderr, "-max-rows cannot be negative: %d\n", *maxRows)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open mysql driver: %v\n", err)
		return 1
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping mysql endpoint: %v\n", err)
		return 1
	}
	fmt.Println("connected=true")

	installedViews := map[string]bool{}
	installedTables := map[string]string{}
	queries := []smokeQuery{
		{Label: "version", SQL: "select @@version as version_value, @@version_comment as version_comment"},
		{Label: "database", SQL: "select database() as database_value, schema() as schema_value"},
		{Label: "session_identity", SQL: "select user() as user_value, current_user() as current_user_value, connection_id() as connection_id_value"},
		{Label: "charset_variables", SQL: "show variables where Variable_name like 'character_set_%' or Variable_name = 'collation_connection'"},
		{Label: "client_variables", SQL: "show global variables where Variable_name in ('lower_case_table_names', 'protocol_version', 'version_compile_machine', 'version_compile_os')"},
		{Label: "session_status", SQL: "show session status like 'Threads_connected'"},
		{Label: "information_schema_character_sets", SQL: "select character_set_name, default_collate_name, maxlen from information_schema.character_sets where character_set_name = 'utf8mb4'"},
		{Label: "information_schema_collations", SQL: "select collation_name, character_set_name, is_default, pad_attribute from information_schema.collations where character_set_name in ('utf8mb4') order by collation_name"},
		{Label: "tables", SQL: "show full tables"},
		{Label: "views", SQL: "show full tables where Table_type = 'VIEW'"},
		{Label: "warnings", SQL: "show warnings limit 10"},
		{Label: "errors", SQL: "show errors limit 10"},
		{Label: "warning_count", SQL: "show count(*) warnings"},
		{Label: "error_count", SQL: "show count(*) errors"},
		{Label: "explain", SQL: "explain select count(*) from lineitem"},
		{Label: "lineitem_count", SQL: "select count(*) as lineitem_count from lineitem"},
	}
	for _, query := range queries {
		rows, err := runQuery(ctx, db, query, *maxRows)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s failed: %v\n", query.Label, err)
			return 1
		}
		if query.Label == "tables" {
			for _, row := range rows {
				if len(row) > 0 {
					tableType := ""
					if len(row) > 1 {
						tableType = strings.ToUpper(fmt.Sprint(row[1]))
					}
					installedTables[strings.ToLower(fmt.Sprint(row[0]))] = tableType
				}
			}
		}
		if query.Label == "views" {
			for _, row := range rows {
				if len(row) > 0 {
					installedViews[strings.ToLower(fmt.Sprint(row[0]))] = true
				}
			}
		}
	}

	if *q3View && installedViews["q3_order_line_base"] {
		if _, err := runQuery(ctx, db, smokeQuery{
			Label: "q3_view",
			SQL: `
				select
				  order_key,
				  sum(extended_price * (1 - discount)) as revenue,
				  order_date,
				  ship_priority
				from q3_order_line_base
				where market_segment = 'BUILDING'
				  and order_date < '1995-03-15'
				  and ship_date > '1995-03-15'
				group by order_key, order_date, ship_priority
				order by revenue desc, order_date
				limit 3
			`,
		}, *maxRows); err != nil {
			fmt.Fprintf(os.Stderr, "q3_view failed: %v\n", err)
			return 1
		}
	} else if *q3View {
		fmt.Println()
		fmt.Println("-- q3_view --")
		fmt.Println("q3_order_line_base not installed; skipped")
	}

	if *prepared {
		if err := runPreparedSmoke(ctx, db, *maxRows, *preparedWrite); err != nil {
			fmt.Fprintf(os.Stderr, "prepared smoke failed: %v\n", err)
			return 1
		}
	}
	if *preparedBatch {
		if err := runPreparedBatchSmoke(ctx, db, installedTables); err != nil {
			fmt.Fprintf(os.Stderr, "prepared batch smoke failed: %v\n", err)
			return 1
		}
	}

	return 0
}

func runPreparedBatchSmoke(ctx context.Context, db *sql.DB, installedTables map[string]string) error {
	fmt.Println()
	fmt.Println("-- prepared_qa_batch_insert_cleanup --")
	if !baseTableInstalled(installedTables, "customers_qa") {
		fmt.Println("customers_qa not installed; skipped")
		return nil
	}

	rows := [][]any{
		{"driver-batch-901", "Abe", "901 Driver Way", "Seattle", "WA", "98072", "425-555-0901", "cell;home"},
		{"driver-batch-902", "Abby", "902 Driver Way", "Tacoma", "WA", "98011", "425-555-0902", "cell;business"},
		{"driver-batch-903", "Annie", "903 Driver Way", "Everett", "WA", "98021", "425-555-0903", "home"},
	}
	keys := firstStringColumn(rows)

	preDeleted, err := execPreparedStringKeys(ctx, db, "QA customer pre-clean", "delete from customers_qa where cust_id = ?", keys)
	if err != nil {
		return err
	}
	before, err := countPreparedStringKeys(ctx, db, "QA customer", "select count(*) from customers_qa where cust_id = ?", keys)
	if err != nil {
		return err
	}
	if before != 0 {
		return fmt.Errorf("QA customer smoke rows before insert = %d, want 0", before)
	}

	affected, err := execPreparedArgs(ctx, db, "QA customer batch insert", `
		insert into customers_qa (
			cust_id,
			first_name,
			address,
			city,
			state,
			zip,
			phone,
			phoneType
		) values (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)
	`, flattenRows(rows)...)
	if err != nil {
		return err
	}
	afterInsert, err := countPreparedStringKeys(ctx, db, "QA customer", "select count(*) from customers_qa where cust_id = ?", keys)
	if err != nil {
		_, _ = execPreparedStringKeys(ctx, db, "QA customer cleanup", "delete from customers_qa where cust_id = ?", keys)
		return err
	}
	if afterInsert != int64(len(rows)) {
		_, _ = execPreparedStringKeys(ctx, db, "QA customer cleanup", "delete from customers_qa where cust_id = ?", keys)
		return fmt.Errorf("QA customer smoke rows after insert = %d, want %d", afterInsert, len(rows))
	}
	postDeleted, err := execPreparedStringKeys(ctx, db, "QA customer cleanup", "delete from customers_qa where cust_id = ?", keys)
	if err != nil {
		return err
	}
	afterCleanup, err := countPreparedStringKeys(ctx, db, "QA customer", "select count(*) from customers_qa where cust_id = ?", keys)
	if err != nil {
		return err
	}
	if afterCleanup != 0 {
		return fmt.Errorf("QA customer smoke rows after cleanup = %d, want 0", afterCleanup)
	}

	fmt.Printf("customer_rows=%d affected_rows=%d pre_deleted=%d before=%d after_insert=%d post_deleted=%d after_cleanup=%d\n",
		len(rows), affected, preDeleted, before, afterInsert, postDeleted, afterCleanup)
	return nil
}

func baseTableInstalled(installedTables map[string]string, table string) bool {
	tableType, ok := installedTables[strings.ToLower(table)]
	return ok && (tableType == "" || tableType == "BASE TABLE")
}

func firstStringColumn(rows [][]any) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			keys = append(keys, fmt.Sprint(row[0]))
		}
	}
	return keys
}

func flattenRows(rows [][]any) []any {
	args := make([]any, 0)
	for _, row := range rows {
		args = append(args, row...)
	}
	return args
}

func countPreparedStringKeys(ctx context.Context, db *sql.DB, label string, query string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare %s count: %w", label, err)
	}
	var total int64
	for _, key := range keys {
		var count int64
		if err := stmt.QueryRowContext(ctx, key).Scan(&count); err != nil {
			_ = stmt.Close()
			return 0, fmt.Errorf("count %s key %s: %w", label, key, err)
		}
		total += count
	}
	if err := stmt.Close(); err != nil {
		return 0, fmt.Errorf("close %s count: %w", label, err)
	}
	return total, nil
}

func execPreparedArgs(ctx context.Context, db *sql.DB, label string, query string, args ...any) (int64, error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", label, err)
	}
	result, execErr := stmt.ExecContext(ctx, args...)
	closeErr := stmt.Close()
	if execErr != nil {
		return 0, fmt.Errorf("execute %s: %w", label, execErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close %s: %w", label, closeErr)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read %s affected rows: %w", label, err)
	}
	return affected, nil
}

func execPreparedStringKeys(ctx context.Context, db *sql.DB, label string, query string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", label, err)
	}
	var total int64
	for _, key := range keys {
		result, err := stmt.ExecContext(ctx, key)
		if err != nil {
			_ = stmt.Close()
			return 0, fmt.Errorf("execute %s key %s: %w", label, key, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			_ = stmt.Close()
			return 0, fmt.Errorf("read %s affected rows for key %s: %w", label, key, err)
		}
		total += affected
	}
	if err := stmt.Close(); err != nil {
		return 0, fmt.Errorf("close %s: %w", label, err)
	}
	return total, nil
}

func runPreparedSmoke(ctx context.Context, db *sql.DB, maxRows int, preparedWrite bool) error {
	fmt.Println()
	fmt.Println("-- prepared_lineitem_shipmode_count --")
	stmt, err := db.PrepareContext(ctx, "select count(*) as lineitem_count from lineitem where l_shipmode = ?")
	if err != nil {
		return fmt.Errorf("prepare shipmode count: %w", err)
	}
	for _, shipMode := range []string{"MAIL", "SHIP"} {
		rows, err := runPreparedQuery(ctx, stmt, maxRows, shipMode)
		if err != nil {
			_ = stmt.Close()
			return fmt.Errorf("execute shipmode count for %s: %w", shipMode, err)
		}
		fmt.Printf("param=%s rows=%v\n", shipMode, rows)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("close shipmode statement: %w", err)
	}

	fmt.Println()
	fmt.Println("-- prepared_lineitem_discount_probe --")
	stmt, err = db.PrepareContext(ctx, "select count(*) as discount_count from lineitem where l_discount between ? and ?")
	if err != nil {
		return fmt.Errorf("prepare discount probe: %w", err)
	}
	rows, err := runPreparedQuery(ctx, stmt, maxRows, 0.05, 0.07)
	if closeErr := stmt.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("close discount statement: %w", closeErr)
	}
	if err != nil {
		return err
	}
	fmt.Printf("params=[0.05 0.07] rows=%v\n", rows)
	if preparedWrite {
		if err := runPreparedInsertSmoke(ctx, db); err != nil {
			return err
		}
	}
	return nil
}

func runPreparedInsertSmoke(ctx context.Context, db *sql.DB) error {
	const customerKey int64 = 900000093
	fmt.Println()
	fmt.Println("-- prepared_customer_insert_cleanup --")

	preDeleted, err := deleteCustomer(ctx, db, customerKey)
	if err != nil {
		return fmt.Errorf("pre-clean customer key %d: %w", customerKey, err)
	}
	before, err := customerCount(ctx, db, customerKey)
	if err != nil {
		return fmt.Errorf("count customer before insert: %w", err)
	}
	if before != 0 {
		return fmt.Errorf("customer key %d already exists before prepared insert smoke", customerKey)
	}

	stmt, err := db.PrepareContext(ctx, `
		insert into customer (
			c_custkey,
			c_name,
			c_address,
			c_nationkey,
			c_phone,
			c_acctbal,
			c_mktsegment,
			c_comment
		) values (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare customer insert: %w", err)
	}
	result, err := stmt.ExecContext(ctx,
		customerKey,
		"Driver Prepared Customer",
		"Cleanup Street",
		int64(1),
		"11-222-333-4444",
		42.50,
		"BUILDING",
		"database/sql prepared insert smoke",
	)
	closeErr := stmt.Close()
	if err != nil {
		return fmt.Errorf("execute customer insert: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close customer insert statement: %w", closeErr)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read prepared insert affected rows: %w", err)
	}
	afterInsert, err := customerCount(ctx, db, customerKey)
	if err != nil {
		return fmt.Errorf("count customer after insert: %w", err)
	}
	if afterInsert != 1 {
		_, _ = deleteCustomer(ctx, db, customerKey)
		return fmt.Errorf("customer key %d count after insert = %d, want 1", customerKey, afterInsert)
	}
	postDeleted, err := deleteCustomer(ctx, db, customerKey)
	if err != nil {
		return fmt.Errorf("post-clean customer key %d: %w", customerKey, err)
	}
	afterCleanup, err := customerCount(ctx, db, customerKey)
	if err != nil {
		return fmt.Errorf("count customer after cleanup: %w", err)
	}
	if afterCleanup != 0 {
		return fmt.Errorf("customer key %d count after cleanup = %d, want 0", customerKey, afterCleanup)
	}
	fmt.Printf("customer_key=%d affected_rows=%d pre_deleted=%d before=%d after_insert=%d post_deleted=%d after_cleanup=%d\n", customerKey, affected, preDeleted, before, afterInsert, postDeleted, afterCleanup)
	return nil
}

type customerCounter interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func customerCount(ctx context.Context, queryer customerCounter, customerKey int64) (int64, error) {
	var count int64
	if err := queryer.QueryRowContext(ctx, "select count(*) from customer where c_custkey = ?", customerKey).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func deleteCustomer(ctx context.Context, db *sql.DB, customerKey int64) (int64, error) {
	stmt, err := db.PrepareContext(ctx, "delete from customer where c_custkey = ?")
	if err != nil {
		return 0, err
	}
	result, execErr := stmt.ExecContext(ctx, customerKey)
	closeErr := stmt.Close()
	if execErr != nil {
		return 0, execErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	return result.RowsAffected()
}

func runPreparedQuery(ctx context.Context, stmt *sql.Stmt, maxRows int, args ...any) ([][]any, error) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fmt.Printf("columns=%v\n", columns)

	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}

	captured := make([][]any, 0)
	count := 0
	for rows.Next() {
		for i := range values {
			values[i] = nil
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make([]any, len(values))
		for i, value := range values {
			row[i] = normalizeDriverValue(value)
		}
		captured = append(captured, row)
		if count < maxRows {
			fmt.Println(row)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	fmt.Printf("row_count=%d\n", count)
	return captured, nil
}

func runQuery(ctx context.Context, db *sql.DB, query smokeQuery, maxRows int) ([][]any, error) {
	fmt.Println()
	fmt.Printf("-- %s --\n", query.Label)
	rows, err := db.QueryContext(ctx, query.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fmt.Printf("columns=%v\n", columns)

	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}

	captured := make([][]any, 0)
	count := 0
	for rows.Next() {
		for i := range values {
			values[i] = nil
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make([]any, len(values))
		for i, value := range values {
			row[i] = normalizeDriverValue(value)
		}
		captured = append(captured, row)
		if count < maxRows {
			fmt.Println(row)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	fmt.Printf("row_count=%d\n", count)
	return captured, nil
}

func normalizeDriverValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return v
	}
}

func envString(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
