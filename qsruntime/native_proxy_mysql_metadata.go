package qsruntime

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// nativeProxyMySQLMetadataQueryResponse answers MySQL client metadata probes
// that are legal no-FROM SELECT statements but do not belong in the analytical
// SQL parser. Standard drivers use these during connection setup.
func nativeProxyMySQLMetadataQueryResponse(command qsmysql.Command) (qsmysql.CommandResponse, bool, error) {
	result, ok := nativeProxyMySQLMetadataQueryResult(command)
	if !ok {
		return qsmysql.CommandResponse{}, false, nil
	}
	response, err := qsmysql.QueryResponse(result)
	return response, true, err
}

func nativeProxyMySQLMetadataQueryResult(command qsmysql.Command) (qsbridge.ExecutionResult, bool) {
	normalized := nativeProxyNormalizeMetadataSQL(command.SQL)
	switch normalized {
	case "select @@max_allowed_packet":
		return nativeProxySingleCellMetadataResult("@@max_allowed_packet", qsbridge.DataTypeInt, qsbridge.ValueInt, int64(64*1024*1024)), true
	case "select database()", "select schema()":
		return nativeProxySingleCellMetadataResult("DATABASE()", qsbridge.DataTypeString, qsbridge.ValueString, nativeProxyMetadataDatabase(command)), true
	case "select version()":
		return nativeProxySingleCellMetadataResult("VERSION()", qsbridge.DataTypeString, qsbridge.ValueString, "8.0.0-quantastream"), true
	case "select connection_id()":
		return nativeProxySingleCellMetadataResult("CONNECTION_ID()", qsbridge.DataTypeInt, qsbridge.ValueInt, int64(nativeProxyMetadataConnectionID(command))), true
	default:
		return qsbridge.ExecutionResult{}, false
	}
}

func nativeProxyMetadataDatabase(command qsmysql.Command) string {
	database := strings.TrimSpace(command.Database)
	if database == "" {
		return "quanta"
	}
	return database
}

func nativeProxyMetadataConnectionID(command qsmysql.Command) uint32 {
	if command.ConnectionID == 0 {
		return 1
	}
	return command.ConnectionID
}

func nativeProxySingleCellMetadataResult(name string, dataType qsbridge.DataType, valueKind qsbridge.ValueKind, value any) qsbridge.ExecutionResult {
	return qsbridge.ExecutionResult{
		Status: qsbridge.ExecutionComplete,
		Kind:   qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{
			Name: name,
			Type: dataType,
		}},
		Chunks: []qsbridge.ResultChunk{{
			Rows: []qsbridge.ResultRow{{
				{Kind: valueKind, Value: value},
			}},
			Final: true,
		}},
		Complete:     true,
		RowsReturned: 1,
	}
}

func nativeProxyNormalizeMetadataSQL(sql string) string {
	text := strings.TrimSpace(sql)
	text = strings.TrimSuffix(text, ";")
	fields := strings.Fields(text)
	return strings.ToLower(strings.Join(fields, " "))
}
