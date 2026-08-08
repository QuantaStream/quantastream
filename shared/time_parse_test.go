package shared

import (
	"testing"
	"time"
)

func TestParseFastUTCTimestampCommonIngestFormats(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Time
	}{
		{
			name:  "date only",
			value: "2026-08-08",
			want:  time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "rfc3339 utc",
			value: "2026-08-08T12:34:56Z",
			want:  time.Date(2026, 8, 8, 12, 34, 56, 0, time.UTC),
		},
		{
			name:  "sql timestamp utc",
			value: "2026-08-08 12:34:56Z",
			want:  time.Date(2026, 8, 8, 12, 34, 56, 0, time.UTC),
		},
		{
			name:  "timezone-less utc",
			value: "2026-08-08T12:34:56",
			want:  time.Date(2026, 8, 8, 12, 34, 56, 0, time.UTC),
		},
		{
			name:  "fractional utc",
			value: "2026-08-08T12:34:56.123456Z",
			want:  time.Date(2026, 8, 8, 12, 34, 56, 123456000, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseFastUTCTimestamp(tt.value)
			if !ok {
				t.Fatalf("ParseFastUTCTimestamp(%q) returned !ok", tt.value)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("ParseFastUTCTimestamp(%q) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}

func TestParseFastUTCTimestampRejectsUnsupportedOrInvalidValues(t *testing.T) {
	tests := []string{
		"",
		"2026-02-31",
		"2026-08-08T12:34:56+00:00",
		"2026-08-08T12:34:56.1234567891Z",
		"not a timestamp",
	}

	for _, value := range tests {
		if got, ok := ParseFastUTCTimestamp(value); ok {
			t.Fatalf("ParseFastUTCTimestamp(%q) = %s, want !ok", value, got)
		}
	}
}
