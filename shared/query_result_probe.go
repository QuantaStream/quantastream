package shared

import (
	"strings"

	pb "github.com/QuantaStream/quantastream/grpc"
)

const queryResultProbeFieldPrefix = "__quantastream_query_probe:"

// QueryResultProbe is an internal metadata row carried on legacy QueryResult
// sample slots. It lets server-side bitmap code report low-level timings without
// changing the protobuf API or exposing these rows to SQL clients.
type QueryResultProbe struct {
	Section string
	Name    string
	Value   string
	Detail  string
}

// NewQueryResultProbeSample encodes one internal query probe as a sample row.
func NewQueryResultProbeSample(section, name, value, detail string) *pb.BitmapResult {
	return &pb.BitmapResult{
		Field: queryResultProbeFieldPrefix + section + "\x00" + name + "\x00" + value + "\x00" + detail,
	}
}

// QueryResultProbeFromSample decodes an internal query probe sample row.
func QueryResultProbeFromSample(sample *pb.BitmapResult) (QueryResultProbe, bool) {
	if sample == nil || len(sample.Bitmap) != 0 || !strings.HasPrefix(sample.Field, queryResultProbeFieldPrefix) {
		return QueryResultProbe{}, false
	}
	payload := strings.TrimPrefix(sample.Field, queryResultProbeFieldPrefix)
	parts := strings.SplitN(payload, "\x00", 4)
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" {
		return QueryResultProbe{}, false
	}
	return QueryResultProbe{
		Section: parts[0],
		Name:    parts[1],
		Value:   parts[2],
		Detail:  parts[3],
	}, true
}
