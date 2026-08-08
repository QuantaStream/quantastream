package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testLoaderCloseable struct {
	err   error
	block chan struct{}
}

func (l testLoaderCloseable) Close() error {
	if l.block != nil {
		<-l.block
	}
	return l.err
}

func TestCloseLoaderWithinReturnsCloseError(t *testing.T) {
	want := errors.New("close failed")

	err := closeLoaderWithin(testLoaderCloseable{err: want}, time.Second)

	require.ErrorIs(t, err, want)
}

func TestCloseLoaderWithinTimesOut(t *testing.T) {
	start := time.Now()

	err := closeLoaderWithin(testLoaderCloseable{block: make(chan struct{})}, 10*time.Millisecond)

	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), time.Second)
}
