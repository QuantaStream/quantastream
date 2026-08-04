package core

import (
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
)

func TestTimestampBSIMapperUsesConfiguredMicrosecondGranularity(t *testing.T) {
	mapper, err := NewTimestampBSIMapper(map[string]string{"granularity": "microsecond"})
	if err != nil {
		t.Fatalf("NewTimestampBSIMapper returned error: %v", err)
	}
	attr := timestampBSITestAttribute("event_time")
	valueTime := time.Date(2026, 7, 24, 12, 30, 10, 123456000, time.UTC)

	value, err := mapper.MapValue(attr, valueTime, nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	if got, want := value.Int64(), valueTime.UnixMicro(); got != want {
		t.Fatalf("encoded timestamp = %d, want epoch micros %d", got, want)
	}
	if got, want := mapper.Render(attr, value), valueTime.Format(time.RFC3339Nano); got != want {
		t.Fatalf("rendered timestamp = %q, want %q", got, want)
	}
}

func TestTimestampBSIMapperDefaultsToNanosecondGranularity(t *testing.T) {
	mapper, err := NewTimestampBSIMapper(nil)
	if err != nil {
		t.Fatalf("NewTimestampBSIMapper returned error: %v", err)
	}
	attr := timestampBSITestAttribute("event_time")
	valueTime := time.Date(2026, 7, 24, 12, 30, 10, 123456789, time.UTC)

	value, err := mapper.MapValue(attr, valueTime.Format(time.RFC3339Nano), nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	if got, want := value.Int64(), valueTime.UnixNano(); got != want {
		t.Fatalf("encoded timestamp = %d, want epoch nanos %d", got, want)
	}
	if got, want := mapper.Render(attr, value), valueTime.Format(time.RFC3339Nano); got != want {
		t.Fatalf("rendered timestamp = %q, want %q", got, want)
	}
}

func TestUUIDBSIMapperRoundTripsRFC4122String(t *testing.T) {
	mapper, err := NewUUIDBSIMapper(nil)
	if err != nil {
		t.Fatalf("NewUUIDBSIMapper returned error: %v", err)
	}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "event_id", MappingStrategy: "UUIDBSI", Type: "String"},
		Parent:         &Table{BasicTable: &shared.BasicTable{Name: "events"}},
	}
	const id = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

	value, err := mapper.MapValue(attr, id, nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	if value == nil || value.Cmp(big.NewInt(0)) <= 0 {
		t.Fatalf("encoded UUID = %v, want positive 128-bit value", value)
	}
	if got := mapper.Render(attr, value); got != id {
		t.Fatalf("rendered UUID = %q, want %q", got, id)
	}
}

func TestUUIDBSIMapperRoundTripsMiddleEndianString(t *testing.T) {
	mapper, err := NewUUIDBSIMapper(map[string]string{"format": "middle_endian"})
	if err != nil {
		t.Fatalf("NewUUIDBSIMapper returned error: %v", err)
	}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "event_id", MappingStrategy: "UUIDBSI", Type: "String"},
		Parent:         &Table{BasicTable: &shared.BasicTable{Name: "events"}},
	}
	const id = "00112233-4455-6677-8899-aabbccddeeff"

	value, err := mapper.MapValue(attr, id, nil, false)
	if err != nil {
		t.Fatalf("MapValue returned error: %v", err)
	}
	if got := value.Text(16); got != "33221100554477668899aabbccddeeff" {
		t.Fatalf("middle-endian encoded UUID = %q", got)
	}
	if got := mapper.Render(attr, value); got != id {
		t.Fatalf("rendered UUID = %q, want %q", got, id)
	}
}

func TestUUIDBSIMapperRejectsUnknownFormat(t *testing.T) {
	if _, err := NewUUIDBSIMapper(map[string]string{"format": "sideways"}); err == nil {
		t.Fatalf("NewUUIDBSIMapper accepted invalid format")
	}
}

func timestampBSITestAttribute(field string) *Attribute {
	return &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: field, MappingStrategy: "TimestampBSI", Type: "DateTime"},
		Parent:         &Table{BasicTable: &shared.BasicTable{Name: "events"}},
	}
}
