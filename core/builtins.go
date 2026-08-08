package core

//
// This file defines all of the built in mapping functions.
//

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	endian "github.com/dnaeon/go-uuid-endianness/uuid"
	"github.com/google/uuid"

	"github.com/QuantaStream/quantastream/shared"
)

const stringLexBSIRemainderPath = "lex_remainders"

// StringLexBSIMapper maps UTF-8 bytes into a lexicographically ordered BSI prefix.
type StringLexBSIMapper struct {
	DefaultMapper
	prefixLength int
}

// NewStringLexBSIMapper constructs a lexical string mapper. The length configuration
// is the number of UTF-8 bytes encoded into the BSI prefix; zero or negative encodes
// the entire value into the BSI and does not write a KV remainder.
func NewStringLexBSIMapper(conf map[string]string) (Mapper, error) {
	prefixLength, err := stringLexBSIPrefixLength(conf)
	if err != nil {
		return nil, err
	}
	return StringLexBSIMapper{DefaultMapper: DefaultMapper{StringLexBSI}, prefixLength: prefixLength}, nil
}

// MapValue maps a string value to its lexical BSI integer.
func (m StringLexBSIMapper) MapValue(attr *Attribute, val interface{}, c *Session,
	isUpdate bool) (result *big.Int, err error) {

	strVal, ok, err := stringLexBSIStringValue(attr, val)
	if err != nil {
		return nil, err
	}
	if !ok {
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return nil, err
	}

	result, remainder := encodeStringLexBSI(strVal, m.prefixLength)
	if c != nil {
		tbuf, ok := c.TableBuffers[attr.Parent.Name]
		if !ok {
			return nil, fmt.Errorf("table %s not open for this connection", attr.Parent.Name)
		}
		if attr.Searchable {
			if err = c.StringIndex.Index(strVal); err != nil {
				return nil, err
			}
		}
		if m.prefixLength > 0 {
			remainderPath := indexPath(tbuf, attr.FieldName, stringLexBSIRemainderPath)
			if err = c.BatchBuffer.SetPartitionedString(remainderPath, tbuf.CurrentColumnID, remainder); err != nil {
				return nil, err
			}
		}
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

func (m StringLexBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		return decodeStringLexBSIPrefix(val)
	}
	return "???"
}

func stringLexBSIPrefixLength(conf map[string]string) (int, error) {
	if conf == nil {
		return 0, fmt.Errorf("'length' config param must be supplied for StringLexBSIMapper")
	}
	for _, key := range []string{"length", "prefixLength", "chars", "characters"} {
		if raw, ok := conf[key]; ok {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err != nil {
				return 0, fmt.Errorf("StringLexBSIMapper %s config must be an integer: %w", key, err)
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("'length' config param must be supplied for StringLexBSIMapper")
}

func stringLexBSIStringValue(attr *Attribute, val interface{}) (string, bool, error) {
	switch typed := val.(type) {
	case string:
		if typed == "" {
			return "", false, nil
		}
		return typed, true, nil
	case int64:
		return fmt.Sprintf("%d", typed), true, nil
	case float64:
		return fmt.Sprintf("%.2f", typed), true, nil
	case map[string]interface{}:
		if len(typed) == 0 {
			return "", false, nil
		}
		b, err := json.Marshal(typed)
		if err != nil {
			return "", false, err
		}
		return string(b), true, nil
	case nil:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("StringLexBSIMapper not expecting a '%T' for '%s'", val, attr.FieldName)
	}
}

func encodeStringLexBSI(value string, prefixLength int) (*big.Int, string) {
	if value == "" {
		return nil, ""
	}
	if prefixLength <= 0 {
		return new(big.Int).SetBytes([]byte(value)), ""
	}
	copied := len(value)
	if copied > prefixLength {
		copied = prefixLength
	}
	remainder := ""
	if len(value) > copied {
		remainder = value[copied:]
	}
	if prefixLength <= 8 {
		var encoded uint64
		for i := 0; i < copied; i++ {
			encoded |= uint64(value[i]) << uint(8*(prefixLength-i-1))
		}
		return new(big.Int).SetUint64(encoded), remainder
	}
	prefix := make([]byte, prefixLength)
	copy(prefix, value)
	return new(big.Int).SetBytes(prefix), remainder
}

func decodeStringLexBSIPrefix(value *big.Int) string {
	if value == nil {
		return ""
	}
	encoded := value.Bytes()
	if zero := strings.IndexByte(string(encoded), 0); zero >= 0 {
		encoded = encoded[:zero]
	}
	return string(encoded)
}

// BoolDirectMapper - Map values 0/1 to true/false rowID 0 = false, rowID 1 = true
type BoolDirectMapper struct {
	DefaultMapper
}

// NewBoolDirectMapper - Construct a new BoolDirectMapper
func NewBoolDirectMapper(conf map[string]string) (Mapper, error) {
	return BoolDirectMapper{DefaultMapper{BoolDirect}}, nil
}

// MapValue - Map boolean values true/false to rowid = 0 false rowid = 1 true
func (m BoolDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	result = big.NewInt(0)
	switch val.(type) {
	case bool:
		if val.(bool) == true {
			result = big.NewInt(1)
		}
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" || strings.ContainsAny(str, `Nn`) {
			break
		}
		if strings.ContainsAny(str, `Yy`) {
			result = big.NewInt(1)
			break
		}
		v, e := strconv.ParseBool(str)
		if e != nil {
			err = e
			return
		}
		if v {
			result = big.NewInt(1)
		}
	case int:
		if val.(int) == 1 {
			result = big.NewInt(1)
		}
	case int64:
		if val.(int64) == 1 {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
			return
		}
	default:
		err = fmt.Errorf("%v: No handling for type '%T'", val, val)
		return
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (true/false)
func (m BoolDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = false
	if id == uint64(1) {
		result = true
	}
	return
}

// IntDirectMapper - Take the input integer value and use it as the row ID.
type IntDirectMapper struct {
	DefaultMapper
}

// NewIntDirectMapper - Construct a new IntDirectMapper
func NewIntDirectMapper(conf map[string]string) (Mapper, error) {
	return IntDirectMapper{DefaultMapper{IntDirect}}, nil
}

// MapValue - Map a value to a row ID.
func (m IntDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case uint64:
		result = big.NewInt(int64(val.(uint64)))
	case int64:
		result = big.NewInt(val.(int64))
	case uint32:
		result = big.NewInt(int64(val.(uint32)))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case int:
		result = big.NewInt(int64(val.(int)))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = big.NewInt(0)
			return
		}
		if v <= 0 {
			err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
			return
		}
		result = big.NewInt(v)
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
			return
		}
	default:
		err = fmt.Errorf("%v: No handling for type '%T'", val, val)
		return
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (row ID value taken literally)
func (m IntDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = id
	return
}

// StringToIntDirectMapper - Maps a string containing a number directly as a row ID.
type StringToIntDirectMapper struct {
	DefaultMapper
}

// NewStringToIntDirectMapper - Construct a new mapper
func NewStringToIntDirectMapper(conf map[string]string) (Mapper, error) {
	return StringToIntDirectMapper{DefaultMapper{IntDirect}}, nil
}

// MapValue - Map a value to a row ID.
func (m StringToIntDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	if val == nil && c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
	}

	var v int64
	v, err = strconv.ParseInt(strings.TrimSpace(val.(string)), 10, 64)
	if v <= 0 {
		err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
		return
	}
	result = big.NewInt(v)
	if err == nil && c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// FloatScaleBSIMapper - Maps floating point values by scaling an integer BSI.
type FloatScaleBSIMapper struct {
	DefaultMapper
}

// NewFloatScaleBSIMapper - Constructs a floating point mapper.
func NewFloatScaleBSIMapper(conf map[string]string) (Mapper, error) {
	return FloatScaleBSIMapper{DefaultMapper{FloatScaleBSI}}, nil
}

// MapValue - Map a value to an big.Int
func (m FloatScaleBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	var floatVal float64
	switch val.(type) {
	case float64:
		floatVal = val.(float64)
	case float32:
		floatVal = float64(val.(float32))
	case uint64:
		floatVal = float64(val.(uint64))
	case int64:
		floatVal = float64(val.(int64))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0.0"
		}
		floatVal, err = strconv.ParseFloat(str, 64)
		if err != nil {
			return
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("type passed for '%s' is of type '%T' which in unsupported", attr.FieldName, val)
		return
	}
	if floatVal != 0 {
		// don't let 7509.999999999999 become 75.09 instead of 75.1
		scaled := floatVal * float64(math.Pow10(attr.Scale)) // this will change 75.1 to 7509.999999999999
		// adjustment := .000000000001 * math.Log10(scaled) // the slow way
		// adjusted := scaled + adjustment
		// read math.Nextafter. It's a hoot. They operate on the int64 representation of the float !
		var adjusted float64
		if floatVal > 0 {
			adjusted = math.Nextafter(scaled, float64(math.MaxFloat64)) // this will change 7509.999999999999 to 7510.00000000000
		} else {
			adjusted = math.Nextafter(scaled, float64(-math.MaxFloat64))
		}
		result = big.NewInt(int64(adjusted))
		checkRound := float64(result.Int64()) / float64(math.Pow10(attr.Scale))
		if checkRound != floatVal {
			err = fmt.Errorf("this would result in rounding error for field '%s', value should have %d decimal places",
				attr.FieldName, attr.Scale)
		}
	} else {
		result = big.NewInt(0)
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m FloatScaleBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.Float:
			f := fmt.Sprintf("%%10.%df", attr.Scale)
			return fmt.Sprintf(f, float64(val.Int64())/math.Pow10(attr.Scale))
		}
	}
	return "???"
}

// IntBSIMapper - Maps integer values to a BSI.
type IntBSIMapper struct {
	DefaultMapper
}

// NewIntBSIMapper - Construct a NewIntBSIMapper
func NewIntBSIMapper(conf map[string]string) (Mapper, error) {
	return IntBSIMapper{DefaultMapper{IntBSI}}, nil
}

// MapValue - Map a value to an int64.
func (m IntBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case int64:
		result = big.NewInt(val.(int64))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case uint32:
		result = big.NewInt(int64(val.(uint32)))
	case uint64:
		result = big.NewInt(int64(val.(uint64)))
	case int:
		result = big.NewInt(int64(val.(int)))
	case uint:
		result = big.NewInt(int64(val.(uint)))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = big.NewInt(0)
			return
		}
		result = big.NewInt(v)
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m IntBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.Integer:
			return fmt.Sprintf("%d", val.Int64())
		}
	}
	return "???"
}

// StringEnumMapper - Maps low cardinality strings to standard bitmaps using the metadata db to assign row ids.
type StringEnumMapper struct {
	DefaultMapper
	delim string
}

// NewStringEnumMapper - Construct a NewStringEnumMapper.
func NewStringEnumMapper(conf map[string]string) (Mapper, error) {
	if conf != nil {
		if d, ok := conf["delim"]; ok {
			return StringEnumMapper{DefaultMapper: DefaultMapper{StringEnum}, delim: d}, nil
		}
		return nil, fmt.Errorf("'delim' config param must be supplied for StringEnumMapper")
	}
	return StringEnumMapper{DefaultMapper: DefaultMapper{StringEnum}, delim: ""}, nil
}

// MapValue - Map a value to a row id.
func (m StringEnumMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	var multi []string
	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" {
			return
		}
		if m.delim != "" {
			multi = strings.Split(strVal, m.delim)
		} else {
			multi = []string{strVal}
		}
	case []string:
		multi = val.([]string)
	case int32:
		strVal := fmt.Sprintf("%d", val.(int32))
		multi = []string{strVal}
	case int64:
		strVal := fmt.Sprintf("%d", val.(int64))
		multi = []string{strVal}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	var rv uint64
	if c != nil && err == nil {
		for _, v := range multi {
			val := strings.TrimSpace(v)
			if val == "" {
				continue
			}
			if rv, err = attr.GetValue(val); err != nil {
				return
			}
			if err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, rv, isUpdate); err != nil {
				return
			}
		}
	} else {
		rv, err = attr.GetValue(multi[0])
	}
	result = big.NewInt(int64(rv))
	return
}

// MapValueReverse - Return the original value given a row id.
func (m StringEnumMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result, err = attr.GetValueForID(id)
	return
}

// GetMultiDelimiter - Return the delimiter used for multiple value support.
func (m StringEnumMapper) GetMultiDelimiter() string {
	return m.delim
}

// BoolRegexMapper - Maps a string pattern to a boolean value.
type BoolRegexMapper struct {
	DefaultMapper
	regex *regexp.Regexp
}

// NewBoolRegexMapper - Construct a NewBoolRegexMapper
func NewBoolRegexMapper(conf map[string]string) (Mapper, error) {

	if conf != nil {
		if pattern, ok := conf["regex"]; ok {
			r, err := regexp.Compile(pattern)
			if err == nil {
				return BoolRegexMapper{DefaultMapper: DefaultMapper{BoolRegex}, regex: r}, nil
			}
			return nil, err
		}
	}
	return nil, fmt.Errorf("'regex' config param must be supplied for BoolRegexMapper")
}

// MapValue - Map a value to a row id.
func (m BoolRegexMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case bool:
		result = big.NewInt(0)
		if val.(bool) {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// Transform - Perform a transformation on a value.
func (m BoolRegexMapper) Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error) {

	var value string
	switch val.(type) {
	case []byte:
		value = string(val.([]byte))
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	newVal = m.regex.MatchString(value)
	return
}

func extractSecondsAndNanosToBigInt(mapv map[string]interface{}, secsSource,
	nanoSource string) (*big.Int, error) {

	secsVal, sok := mapv[secsSource]
	nanoVal, nok := mapv[nanoSource]
	if !sok {
		return nil, fmt.Errorf("'seconds' is missing from source")
	}
	var seconds, nanos int64
	var err error
	if v, ok := secsVal.(float64); ok {
		seconds = int64(v)
	} else { // Gotta be a string
		seconds, err = strconv.ParseInt(secsVal.(string), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("extractSecondsAndNanos parsing 'seconds' - %v", err)
		}
	}
	if nok {
		if v, ok := nanoVal.(float64); ok {
			nanos = int64(v)
		} else { // String
			nanos, _ = strconv.ParseInt(nanoVal.(string), 10, 64)
		}
	}
	bigTime := big.NewInt(int64(seconds*1000000000 + nanos))
	return bigTime, nil
}

func parseTimestampMapperString(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if parsed, ok := shared.ParseFastUTCTimestamp(trimmed); ok {
		return parsed, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z",
		"2006-01-02 15:04:05.000Z",
	} {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return parsed, nil
		}
	}
	return dateparse.ParseIn(trimmed, time.UTC)
}

// TimestampBSIMapper maps timestamps to a BSI at a configured granularity.
type TimestampBSIMapper struct {
	DefaultMapper
	granularity   string
	nanosPerValue int64
	secondsSource string
	nanosSource   string
}

// NewTimestampBSIMapper constructs a timestamp mapper. The granularity
// configuration accepts second, millisecond, microsecond, or nanosecond.
func NewTimestampBSIMapper(conf map[string]string) (Mapper, error) {
	granularity, nanosPerValue, err := timestampBSIGranularity(conf)
	if err != nil {
		return nil, err
	}
	mapper := TimestampBSIMapper{
		DefaultMapper: DefaultMapper{TimestampBSI},
		granularity:   granularity,
		nanosPerValue: nanosPerValue,
		secondsSource: "seconds",
		nanosSource:   "nanos",
	}
	if conf != nil {
		if v, ok := conf["seconds"]; ok {
			mapper.secondsSource = v
		}
		if v, ok := conf["nanos"]; ok {
			mapper.nanosSource = v
		}
	}
	return mapper, nil
}

// MapValue maps a value to a timestamp BSI value at the configured granularity.
func (m TimestampBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case map[string]interface{}: // composite seconds/nanos
		if m.secondsSource == "" || m.nanosSource == "" {
			err = fmt.Errorf("'seconds' or 'nanos' configuration not specified")
			return
		}
		mapv := val.(map[string]interface{})
		result, err = extractSecondsAndNanosToBigInt(mapv, m.secondsSource, m.nanosSource)
		if err == nil {
			result.Div(result, big.NewInt(m.nanosPerValue))
		}
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = big.NewInt(0)
			return
		}
		var t time.Time
		t, err = parseTimestampMapperString(strVal)
		if err == nil {
			result = big.NewInt(t.UnixNano() / m.nanosPerValue)
		}
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		if err == nil {
			result = big.NewInt(t.UnixNano() / m.nanosPerValue)
		}
	case time.Time:
		result = big.NewInt(val.(time.Time).UnixNano() / m.nanosPerValue)
	case int64:
		result = big.NewInt(val.(int64))
	case int:
		result = big.NewInt(int64(val.(int)))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case float64:
		result = big.NewInt(int64(val.(float64)))
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m TimestampBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.DateTime, shared.Date:
			t := time.Unix(0, val.Int64()*m.nanosPerValue).UTC()
			if shared.TypeFromString(attr.Type) == shared.DateTime {
				return t.Format(time.RFC3339Nano)
			}
			if shared.TypeFromString(attr.Type) == shared.Date {
				return t.Format("2006-01-02")
			}
		}
	}
	return "???"
}

func timestampBSIGranularity(conf map[string]string) (string, int64, error) {
	raw := "nanosecond"
	if conf != nil {
		for _, key := range []string{"granularity", "precision", "unit"} {
			if v, ok := conf[key]; ok && strings.TrimSpace(v) != "" {
				raw = v
				break
			}
		}
	}
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "s", "sec", "second", "seconds":
		return "second", int64(time.Second), nil
	case "ms", "milli", "millisecond", "milliseconds":
		return "millisecond", int64(time.Millisecond), nil
	case "us", "micro", "microsecond", "microseconds":
		return "microsecond", int64(time.Microsecond), nil
	case "ns", "nano", "nanosecond", "nanoseconds":
		return "nanosecond", int64(time.Nanosecond), nil
	default:
		return "", 0, fmt.Errorf("TimestampBSI granularity must be second, millisecond, microsecond, or nanosecond, got %q", raw)
	}
}

// IntToBoolDirectMapper - Maps 0/1 integer to boolean
type IntToBoolDirectMapper struct {
	DefaultMapper
}

// NewIntToBoolDirectMapper - Construct a NewIntToBoolDirectMapper
func NewIntToBoolDirectMapper(conf map[string]string) (Mapper, error) {
	return IntToBoolDirectMapper{DefaultMapper{IntToBoolDirect}}, nil
}

// MapValue - Map a value to a row id.
func (m IntToBoolDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case int:
		result = big.NewInt(0)
		if val.(int) != 0 {
			result = big.NewInt(1)
		}
	case bool:
		result = big.NewInt(0)
		if val.(bool) {
			result = big.NewInt(1)
		}
	case string:
		result = big.NewInt(0)
		if val.(string) == "true" {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a boolean", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// Transform - Perform a transformation on a value.
func (m IntToBoolDirectMapper) Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error) {

	switch val.(type) {
	case int:
		newVal = false
		if val.(int) != 0 {
			newVal = true
		}
	case string:
		newVal = false
		if val.(string) != "0" {
			newVal = true
		}
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a boolean", attr.FieldName, val)
	}

	return
}

func extractUpperAndLowerBitsToBigInt(mapv map[string]interface{}, upperSrc, lowerSrc string, format string) *big.Int {

	upperVal, uok := mapv[upperSrc]
	lowerVal, lok := mapv[lowerSrc]
	if !uok || !lok {
		return nil
	}
	upperBits, err := int64FromUUIDPart(upperVal)
	if err != nil {
		return nil
	}
	lowerBits, err := int64FromUUIDPart(lowerVal)
	if err != nil {
		return nil
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], uint64(upperBits))
	binary.BigEndian.PutUint64(b[8:], uint64(lowerBits))
	return uuidBytesToBigInt(b, format)
}

func int64FromUUIDPart(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return 0, fmt.Errorf("UUID part has unsupported type %T", value)
	}
}

// UUIDBSIMapper maps UUID values to a 128 bit BSI.
type UUIDBSIMapper struct {
	DefaultMapper
	upperSrc string
	lowerSrc string
	format   string
}

// NewUUIDBSIMapper - Construct a NewUUIDBSIMapper
func NewUUIDBSIMapper(conf map[string]string) (Mapper, error) {
	mapper := UUIDBSIMapper{
		DefaultMapper: DefaultMapper{UUIDBSI},
		upperSrc:      "upperBits",
		lowerSrc:      "lowerBits",
		format:        "rfc4122",
	}
	if conf != nil {
		if v, ok := conf["upperSource"]; ok {
			mapper.upperSrc = v
		}
		if v, ok := conf["lowerSource"]; ok {
			mapper.lowerSrc = v
		}
		for _, key := range []string{"format", "byteOrder", "encoding"} {
			if v, ok := conf[key]; ok && strings.TrimSpace(v) != "" {
				format, err := normalizeUUIDBSIFormat(v)
				if err != nil {
					return nil, err
				}
				mapper.format = format
				break
			}
		}
	}
	return mapper, nil
}

// MapValue maps a UUID value to a BSI integer.
func (m UUIDBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case map[string]interface{}: // composite upper/lower bits
		if m.upperSrc == "" || m.lowerSrc == "" {
			err = fmt.Errorf("'upperSource' or 'lowerSource' configuration not specified")
			return
		}
		mapv := val.(map[string]interface{})
		result = extractUpperAndLowerBitsToBigInt(mapv, m.upperSrc, m.lowerSrc, m.format)
		if result == nil {
			err = fmt.Errorf("UUIDBSIMapper could not map upper/lower UUID bits for '%s'", attr.FieldName)
		}
	case string:
		if uuidVal, errx := uuid.Parse(val.(string)); errx == nil {
			b, _ := uuidVal.MarshalBinary()
			result = uuidBytesToBigInt(b, m.format)
		} else {
			err = errx
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	case int64:
		v, _ := val.(int64)
		result = big.NewInt(v)
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m UUIDBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.String:
			b := uuidBigIntBytes(val)
			if m.format == "middle_endian" {
				nuuid, _ := endian.FromBytes(b)
				b, _ = nuuid.MiddleEndianBytes()
			}
			if newUUID, err := uuid.FromBytes(b); err != nil {
				return fmt.Sprintf("ERR = %v", err)
			} else {
				return newUUID.String()
			}
		}
	}
	return "???"
}

func normalizeUUIDBSIFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "canonical", "rfc4122", "big", "big_endian", "big-endian", "standard":
		return "rfc4122", nil
	case "middle", "middle_endian", "middle-endian", "guid", "legacy":
		return "middle_endian", nil
	default:
		return "", fmt.Errorf("UUIDBSI format must be rfc4122 or middle_endian, got %q", raw)
	}
}

func uuidBytesToBigInt(b []byte, format string) *big.Int {
	if format == "middle_endian" {
		nuuid, _ := endian.FromBytes(b)
		middleEndian, _ := nuuid.MiddleEndianBytes()
		return new(big.Int).SetBytes(middleEndian)
	}
	return new(big.Int).SetBytes(uuidBigIntBytes(new(big.Int).SetBytes(b)))
}

func uuidBigIntBytes(value *big.Int) []byte {
	b := value.Bytes()
	if len(b) >= 16 {
		if len(b) == 16 {
			return b
		}
		return b[len(b)-16:]
	}
	padded := make([]byte, 16)
	copy(padded[16-len(b):], b)
	return padded
}
