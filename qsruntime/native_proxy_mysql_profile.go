package qsruntime

import (
	"strconv"
	"sync"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// NativeProxyMySQLSessionProfile keeps the last execution profile for one MySQL session.
type NativeProxyMySQLSessionProfile struct {
	mu   sync.Mutex
	last ExecutionInstrumentationSnapshot
}

// NewNativeProxyMySQLSessionProfile creates an empty per-session profile store.
func NewNativeProxyMySQLSessionProfile() *NativeProxyMySQLSessionProfile {
	return &NativeProxyMySQLSessionProfile{}
}

// Store replaces the last query profile with a stable copy of snapshot.
func (p *NativeProxyMySQLSessionProfile) Store(snapshot ExecutionInstrumentationSnapshot) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.last = cloneExecutionInstrumentationSnapshot(snapshot)
}

// Snapshot returns the last stored profile.
func (p *NativeProxyMySQLSessionProfile) Snapshot() ExecutionInstrumentationSnapshot {
	if p == nil {
		return ExecutionInstrumentationSnapshot{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneExecutionInstrumentationSnapshot(p.last)
}

func nativeProxyMySQLProfileQueryResponse(command qsmysql.Command, profile *NativeProxyMySQLSessionProfile) (qsmysql.CommandResponse, bool, error) {
	result, ok := nativeProxyMySQLProfileQueryResult(command, profile)
	if !ok {
		return qsmysql.CommandResponse{}, false, nil
	}
	response, err := qsmysql.QueryResponse(result)
	return response, true, err
}

func nativeProxyMySQLProfileQueryResult(command qsmysql.Command, profile *NativeProxyMySQLSessionProfile) (qsbridge.ExecutionResult, bool) {
	switch nativeProxyNormalizeMetadataSQL(command.SQL) {
	case "show quantastream profile",
		"show quantastream probes",
		"select * from quantastream_last_query_profile",
		"select * from quantastream.query_profile":
		return nativeProxyMySQLProfileResult(profile.Snapshot()), true
	default:
		return qsbridge.ExecutionResult{}, false
	}
}

func nativeProxyMySQLProfileResult(snapshot ExecutionInstrumentationSnapshot) qsbridge.ExecutionResult {
	rows := make([]qsbridge.ResultRow, 0, len(snapshot.Timings)+len(snapshot.Counters)+len(snapshot.Events))
	for _, timing := range snapshot.Timings {
		rows = append(rows, nativeProxyMySQLProfileRow("timing", timing.Section, timing.Name, timing.Duration.String(), timing.Detail))
	}
	for _, counter := range snapshot.Counters {
		rows = append(rows, nativeProxyMySQLProfileRow("counter", counter.Section, counter.Name, strconv.FormatUint(counter.Value, 10), counter.Detail))
	}
	for _, event := range snapshot.Events {
		rows = append(rows, nativeProxyMySQLProfileRow("event", event.Section, event.Name, event.Value, event.Detail))
	}
	return qsbridge.ExecutionResult{
		Status: qsbridge.ExecutionComplete,
		Kind:   qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{
			{Name: "kind", Type: qsbridge.DataTypeString},
			{Name: "section", Type: qsbridge.DataTypeString},
			{Name: "name", Type: qsbridge.DataTypeString},
			{Name: "value", Type: qsbridge.DataTypeString},
			{Name: "detail", Type: qsbridge.DataTypeString},
		},
		Chunks: []qsbridge.ResultChunk{{
			Rows:  rows,
			Final: true,
		}},
		Complete:     true,
		RowsReturned: uint64(len(rows)),
	}
}

func nativeProxyMySQLProfileRow(kind, section, name, value, detail string) qsbridge.ResultRow {
	return qsbridge.ResultRow{
		{Kind: qsbridge.ValueString, Value: kind},
		{Kind: qsbridge.ValueString, Value: section},
		{Kind: qsbridge.ValueString, Value: name},
		{Kind: qsbridge.ValueString, Value: value},
		{Kind: qsbridge.ValueString, Value: detail},
	}
}
