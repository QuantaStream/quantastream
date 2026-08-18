package qsruntime

import (
	"strconv"
	"sync"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// NativeProxyMySQLSessionProfile keeps the last execution profile for one MySQL session.
type NativeProxyMySQLSessionProfile struct {
	mu              sync.Mutex
	last            ExecutionInstrumentationSnapshot
	prepared        *qsbridge.MemoryPreparedStatementRegistry
	longData        *qsbridge.MemoryPreparedLongDataRegistry
	longDataPayload map[string][]byte
	parameterTypes  map[qsbridge.PreparedStatementID][]qsmysql.PreparedParameterType
	session         qsbridge.SessionContext
}

// NewNativeProxyMySQLSessionProfile creates an empty per-session profile store.
func NewNativeProxyMySQLSessionProfile() *NativeProxyMySQLSessionProfile {
	return &NativeProxyMySQLSessionProfile{
		prepared:        qsbridge.NewMemoryPreparedStatementRegistry(),
		longData:        qsbridge.NewMemoryPreparedLongDataRegistry(),
		longDataPayload: make(map[string][]byte),
		parameterTypes:  make(map[qsbridge.PreparedStatementID][]qsmysql.PreparedParameterType),
	}
}

// Session returns this MySQL connection's planning session metadata.
func (p *NativeProxyMySQLSessionProfile) Session() qsbridge.SessionContext {
	if p == nil {
		return qsbridge.SessionContext{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session.Clone()
}

// ApplySessionActions applies successful SQL session mutations to this connection.
func (p *NativeProxyMySQLSessionProfile) ApplySessionActions(actions []qsbridge.SessionAction) qsbridge.DiagnosticSet {
	if p == nil || len(actions) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	transition := p.session.PreviewSessionTransition(actions)
	if transition.Diagnostics.BlocksNative() {
		return transition.Diagnostics
	}
	p.session = transition.After.Clone()
	return nil
}

// SetCurrentSchema remembers the default database selected by the protocol connection.
func (p *NativeProxyMySQLSessionProfile) SetCurrentSchema(schema string) {
	if p == nil || schema == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session.CurrentSchema == "" {
		p.session.CurrentSchema = schema
	}
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

// PreparedStatements returns the per-session prepared-statement registry.
func (p *NativeProxyMySQLSessionProfile) PreparedStatements() *qsbridge.MemoryPreparedStatementRegistry {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prepared == nil {
		p.prepared = qsbridge.NewMemoryPreparedStatementRegistry()
	}
	return p.prepared
}

// PreparedLongData returns the per-session long-data metadata registry.
func (p *NativeProxyMySQLSessionProfile) PreparedLongData() *qsbridge.MemoryPreparedLongDataRegistry {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.longData == nil {
		p.longData = qsbridge.NewMemoryPreparedLongDataRegistry()
	}
	return p.longData
}

// StorePreparedParameterTypes remembers the latest binary wire types for one prepared statement.
func (p *NativeProxyMySQLSessionProfile) StorePreparedParameterTypes(handle qsbridge.PreparedStatementHandle, types []qsmysql.PreparedParameterType) {
	if p == nil || handle.ID == 0 || len(types) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.parameterTypes == nil {
		p.parameterTypes = make(map[qsbridge.PreparedStatementID][]qsmysql.PreparedParameterType)
	}
	p.parameterTypes[handle.ID] = append([]qsmysql.PreparedParameterType(nil), types...)
}

// PreparedParameterTypes returns cached binary wire types for one prepared statement.
func (p *NativeProxyMySQLSessionProfile) PreparedParameterTypes(handle qsbridge.PreparedStatementHandle) []qsmysql.PreparedParameterType {
	if p == nil || handle.ID == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.parameterTypes == nil {
		return nil
	}
	return append([]qsmysql.PreparedParameterType(nil), p.parameterTypes[handle.ID]...)
}

// AppendPreparedLongData records payload bytes and metadata for one long-data fragment.
func (p *NativeProxyMySQLSessionProfile) AppendPreparedLongData(handle qsbridge.PreparedStatementHandle, parameter qsbridge.ParameterValue, data []byte) (qsbridge.PreparedLongDataState, bool) {
	if p == nil || handle.Empty() || parameter.Index == 0 {
		return qsbridge.PreparedLongDataState{}, false
	}
	state, ok := p.PreparedLongData().Append(qsbridge.PreparedLongDataFragment{
		Handle:     handle,
		Parameter:  parameter,
		ChunkBytes: uint64(len(data)),
	})
	if !ok {
		return qsbridge.PreparedLongDataState{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.longDataPayload == nil {
		p.longDataPayload = make(map[string][]byte)
	}
	key := nativeProxyPreparedLongDataPayloadKey(handle, parameter.Index)
	p.longDataPayload[key] = append(p.longDataPayload[key], data...)
	return state, true
}

// PreparedLongDataValues returns accumulated long-data payloads keyed by one-based parameter index.
func (p *NativeProxyMySQLSessionProfile) PreparedLongDataValues(handle qsbridge.PreparedStatementHandle) map[int][]byte {
	if p == nil || handle.Empty() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.longDataPayload) == 0 {
		return nil
	}
	values := make(map[int][]byte)
	prefix := nativeProxyPreparedLongDataHandlePayloadKey(handle) + "|"
	for key, data := range p.longDataPayload {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		index, ok := nativeProxyPreparedLongDataPayloadIndex(key[len(prefix):])
		if !ok {
			continue
		}
		values[index] = append([]byte(nil), data...)
	}
	return values
}

// ClearPreparedLongData clears all accumulated long-data state for one prepared statement.
func (p *NativeProxyMySQLSessionProfile) ClearPreparedLongData(handle qsbridge.PreparedStatementHandle) bool {
	if p == nil || handle.Empty() {
		return false
	}
	metadataCleared := p.PreparedLongData().ClearHandle(handle)
	p.mu.Lock()
	defer p.mu.Unlock()
	prefix := nativeProxyPreparedLongDataHandlePayloadKey(handle) + "|"
	payloadCleared := false
	for key := range p.longDataPayload {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(p.longDataPayload, key)
			payloadCleared = true
		}
	}
	return metadataCleared || payloadCleared
}

// ClosePreparedStatement removes a prepared statement and all adapter-owned session state.
func (p *NativeProxyMySQLSessionProfile) ClosePreparedStatement(handle qsbridge.PreparedStatementHandle) bool {
	if p == nil || handle.Empty() {
		return false
	}
	closed := p.PreparedStatements().Close(qsbridge.PreparedStatementCloseRequest{Handle: handle})
	p.ClearPreparedLongData(handle)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.parameterTypes != nil && handle.ID != 0 {
		delete(p.parameterTypes, handle.ID)
	}
	return closed
}

func nativeProxyPreparedLongDataPayloadKey(handle qsbridge.PreparedStatementHandle, index int) string {
	return nativeProxyPreparedLongDataHandlePayloadKey(handle) + "|" + strconv.Itoa(index)
}

func nativeProxyPreparedLongDataHandlePayloadKey(handle qsbridge.PreparedStatementHandle) string {
	if handle.ID != 0 {
		return "id:" + strconv.FormatUint(uint64(handle.ID), 10)
	}
	return "name:" + handle.Name
}

func nativeProxyPreparedLongDataPayloadIndex(value string) (int, bool) {
	index, err := strconv.Atoi(value)
	if err != nil || index <= 0 {
		return 0, false
	}
	return index, true
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
