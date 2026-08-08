package shared

import (
	"strings"
	"time"
)

// ParseFastUTCTimestamp parses common ingest timestamp literals without falling
// through to general-purpose date parsing. It accepts YYYY-MM-DD and UTC
// timestamps using a T or space separator, optional fractional seconds, and an
// optional trailing Z.
func ParseFastUTCTimestamp(raw string) (time.Time, bool) {
	value := strings.TrimSpace(raw)
	if len(value) < len("2006-01-02") {
		return time.Time{}, false
	}
	year, ok := parseFixedDigits(value, 0, 4)
	if !ok || value[4] != '-' {
		return time.Time{}, false
	}
	month, ok := parseFixedDigits(value, 5, 2)
	if !ok || value[7] != '-' {
		return time.Time{}, false
	}
	day, ok := parseFixedDigits(value, 8, 2)
	if !ok {
		return time.Time{}, false
	}
	if len(value) == len("2006-01-02") {
		return checkedUTCTime(year, month, day, 0, 0, 0, 0)
	}
	if len(value) < len("2006-01-02T15:04:05") || (value[10] != 'T' && value[10] != ' ') {
		return time.Time{}, false
	}
	hour, ok := parseFixedDigits(value, 11, 2)
	if !ok || value[13] != ':' {
		return time.Time{}, false
	}
	minute, ok := parseFixedDigits(value, 14, 2)
	if !ok || value[16] != ':' {
		return time.Time{}, false
	}
	second, ok := parseFixedDigits(value, 17, 2)
	if !ok {
		return time.Time{}, false
	}
	nanos := 0
	index := len("2006-01-02T15:04:05")
	if index < len(value) && value[index] == '.' {
		var parsed bool
		nanos, index, parsed = parseFractionalNanos(value, index+1)
		if !parsed {
			return time.Time{}, false
		}
	}
	if index < len(value) {
		if index != len(value)-1 || value[index] != 'Z' {
			return time.Time{}, false
		}
	}
	return checkedUTCTime(year, month, day, hour, minute, second, nanos)
}

func parseFixedDigits(value string, start, count int) (int, bool) {
	if start+count > len(value) {
		return 0, false
	}
	result := 0
	for i := start; i < start+count; i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, false
		}
		result = result*10 + int(value[i]-'0')
	}
	return result, true
}

func parseFractionalNanos(value string, start int) (int, int, bool) {
	if start >= len(value) || value[start] < '0' || value[start] > '9' {
		return 0, start, false
	}
	nanos := 0
	digits := 0
	index := start
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		if digits >= 9 {
			return 0, start, false
		}
		nanos = nanos*10 + int(value[index]-'0')
		digits++
		index++
	}
	for digits < 9 {
		nanos *= 10
		digits++
	}
	return nanos, index, true
}

func checkedUTCTime(year, month, day, hour, minute, second, nanos int) (time.Time, bool) {
	if month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 || nanos < 0 {
		return time.Time{}, false
	}
	parsed := time.Date(year, time.Month(month), day, hour, minute, second, nanos, time.UTC)
	if parsed.Year() != year || int(parsed.Month()) != month || parsed.Day() != day ||
		parsed.Hour() != hour || parsed.Minute() != minute || parsed.Second() != second ||
		parsed.Nanosecond() != nanos {
		return time.Time{}, false
	}
	return parsed, true
}
