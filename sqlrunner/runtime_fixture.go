package main

import (
	"context"

	"github.com/QuantaStream/quantastream/qsfixture"
	"github.com/QuantaStream/quantastream/qsruntime"
)

func newRuntimeFixtureSQLRuntime(ctx context.Context) (qsruntime.SQLRuntime, error) {
	return qsfixture.NewSQLRuntime(ctx)
}
