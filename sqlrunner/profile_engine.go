package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

type sqlRunnerProfileSQLEngine struct {
	Engine roadmap.Engine
}

func (e sqlRunnerProfileSQLEngine) Query(ctx context.Context, statement string) (roadmap.QueryResult, error) {
	return e.Engine.Query(ctx, statement)
}

func (e sqlRunnerProfileSQLEngine) Exec(ctx context.Context, statement string) (int64, error) {
	return e.Engine.Exec(ctx, statement)
}

func (e sqlRunnerProfileSQLEngine) QueryProfile(ctx context.Context) ([]roadmap.ProfileRow, error) {
	result, err := e.Engine.Query(ctx, "show quantastream profile")
	if err != nil {
		return nil, err
	}
	return sqlRunnerProfileRowsFromResult(result), nil
}

func sqlRunnerProfileRowsFromResult(result roadmap.QueryResult) []roadmap.ProfileRow {
	index := map[string]int{}
	for i, column := range result.Columns {
		index[strings.ToLower(strings.TrimSpace(column))] = i
	}
	rows := make([]roadmap.ProfileRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, roadmap.ProfileRow{
			Kind:    sqlRunnerProfileCell(row, sqlRunnerProfileColumn(index, "kind")),
			Section: sqlRunnerProfileCell(row, sqlRunnerProfileColumn(index, "section")),
			Name:    sqlRunnerProfileCell(row, sqlRunnerProfileColumn(index, "name")),
			Value:   sqlRunnerProfileCell(row, sqlRunnerProfileColumn(index, "value")),
			Detail:  sqlRunnerProfileCell(row, sqlRunnerProfileColumn(index, "detail")),
		})
	}
	return rows
}

func sqlRunnerProfileColumn(index map[string]int, name string) int {
	if value, ok := index[name]; ok {
		return value
	}
	return -1
}

func sqlRunnerProfileCell(row []roadmap.Cell, index int) string {
	if index < 0 || index >= len(row) || row[index].Null {
		return ""
	}
	return row[index].Text
}

func sqlRunnerProfileEngine(ctx context.Context, db *sql.DB, capture bool) (roadmap.Engine, func() error, error) {
	if !capture || db == nil {
		return nil, dbCloseFunc(db), nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return sqlRunnerProfileSQLEngine{Engine: roadmap.ConnSQLEngine{Conn: conn}}, func() error {
		return errors.Join(conn.Close(), db.Close())
	}, nil
}

func dbCloseFunc(db *sql.DB) func() error {
	if db == nil {
		return nil
	}
	return db.Close
}
