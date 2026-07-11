package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

const defaultMySQLReferenceDriver = "mysql"

type mysqlReferenceConfig struct {
	Driver string
	DSN    string
}

func (c mysqlReferenceConfig) withDefaults() mysqlReferenceConfig {
	c.Driver = strings.TrimSpace(c.Driver)
	if c.Driver == "" {
		c.Driver = defaultMySQLReferenceDriver
	}
	c.DSN = strings.TrimSpace(c.DSN)
	return c
}

func (c mysqlReferenceConfig) validate() error {
	c = c.withDefaults()
	if c.DSN == "" {
		return fmt.Errorf("mysql-reference engine requires mysql_dsn")
	}
	return nil
}

func buildMySQLReferenceHarness(_ *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	engine, closeFn, err := newMySQLReferenceEngine(mysqlReferenceConfig{
		Driver: cfg.MySQLDriver,
		DSN:    cfg.MySQLDSN,
	})
	if err != nil {
		return runnerHarness{}, err
	}
	return runnerHarness{
		Runner: roadmap.Runner{
			Engine:     engine,
			Admin:      func(context.Context, string) error { return nil },
			Verbose:    cfg.Verbose,
			DumpActual: cfg.DumpActual,
			Logf:       log.Printf,
		},
		Close: closeFn,
	}, nil
}

func newMySQLReferenceEngine(config mysqlReferenceConfig) (roadmap.Engine, func() error, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, nil, err
	}
	db, err := sql.Open(config.Driver, config.DSN)
	if err != nil {
		return nil, nil, err
	}
	return roadmap.SQLEngine{DB: db}, db.Close, nil
}
