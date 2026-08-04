package core

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"
)

type canonicalPayloadWriter interface {
	Write([]byte) (int, error)
}

// HashIngestPayload returns a deterministic 64-bit hash for an ingest payload.
// Map keys are sorted recursively so equivalent parsed payloads produce the
// same hash regardless of Go map iteration order.
func HashIngestPayload(payload map[string]interface{}) (uint64, error) {
	hash := fnv.New64a()
	if err := writeCanonicalPayloadValue(hash, payload); err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

func writeCanonicalPayloadValue(writer canonicalPayloadWriter, value interface{}) error {
	if value == nil {
		return writeCanonicalToken(writer, "null", "")
	}
	switch typed := value.(type) {
	case string:
		return writeCanonicalToken(writer, "string", typed)
	case bool:
		if typed {
			return writeCanonicalToken(writer, "bool", "true")
		}
		return writeCanonicalToken(writer, "bool", "false")
	case int:
		return writeCanonicalToken(writer, "int", strconv.FormatInt(int64(typed), 10))
	case int8:
		return writeCanonicalToken(writer, "int", strconv.FormatInt(int64(typed), 10))
	case int16:
		return writeCanonicalToken(writer, "int", strconv.FormatInt(int64(typed), 10))
	case int32:
		return writeCanonicalToken(writer, "int", strconv.FormatInt(int64(typed), 10))
	case int64:
		return writeCanonicalToken(writer, "int", strconv.FormatInt(typed, 10))
	case uint:
		return writeCanonicalToken(writer, "uint", strconv.FormatUint(uint64(typed), 10))
	case uint8:
		return writeCanonicalToken(writer, "uint", strconv.FormatUint(uint64(typed), 10))
	case uint16:
		return writeCanonicalToken(writer, "uint", strconv.FormatUint(uint64(typed), 10))
	case uint32:
		return writeCanonicalToken(writer, "uint", strconv.FormatUint(uint64(typed), 10))
	case uint64:
		return writeCanonicalToken(writer, "uint", strconv.FormatUint(typed, 10))
	case float32:
		return writeCanonicalToken(writer, "float32", strconv.FormatUint(uint64(math.Float32bits(typed)), 10))
	case float64:
		return writeCanonicalToken(writer, "float64", strconv.FormatUint(math.Float64bits(typed), 10))
	case json.Number:
		return writeCanonicalToken(writer, "number", typed.String())
	case time.Time:
		return writeCanonicalToken(writer, "time", typed.UTC().Format(time.RFC3339Nano))
	case map[string]interface{}:
		return writeCanonicalPayloadMap(writer, typed)
	case []interface{}:
		return writeCanonicalPayloadSlice(writer, typed)
	}
	return writeCanonicalReflectValue(writer, reflect.ValueOf(value))
}

func writeCanonicalReflectValue(writer canonicalPayloadWriter, value reflect.Value) error {
	if !value.IsValid() {
		return writeCanonicalToken(writer, "null", "")
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return writeCanonicalToken(writer, "null", "")
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("payload map key type %s is not supported", value.Type().Key())
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		if _, err := writer.Write([]byte("map:" + strconv.Itoa(len(keys)) + "{")); err != nil {
			return err
		}
		for _, key := range keys {
			if err := writeCanonicalToken(writer, "key", key.String()); err != nil {
				return err
			}
			if err := writeCanonicalPayloadValue(writer, value.MapIndex(key).Interface()); err != nil {
				return err
			}
		}
		_, err := writer.Write([]byte("}"))
		return err
	case reflect.Slice, reflect.Array:
		if _, err := writer.Write([]byte("list:" + strconv.Itoa(value.Len()) + "[")); err != nil {
			return err
		}
		for i := 0; i < value.Len(); i++ {
			if err := writeCanonicalPayloadValue(writer, value.Index(i).Interface()); err != nil {
				return err
			}
		}
		_, err := writer.Write([]byte("]"))
		return err
	default:
		return fmt.Errorf("payload value type %s is not supported", value.Type())
	}
}

func writeCanonicalPayloadMap(writer canonicalPayloadWriter, payload map[string]interface{}) error {
	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if _, err := writer.Write([]byte("map:" + strconv.Itoa(len(keys)) + "{")); err != nil {
		return err
	}
	for _, key := range keys {
		if err := writeCanonicalToken(writer, "key", key); err != nil {
			return err
		}
		if err := writeCanonicalPayloadValue(writer, payload[key]); err != nil {
			return err
		}
	}
	_, err := writer.Write([]byte("}"))
	return err
}

func writeCanonicalPayloadSlice(writer canonicalPayloadWriter, values []interface{}) error {
	if _, err := writer.Write([]byte("list:" + strconv.Itoa(len(values)) + "[")); err != nil {
		return err
	}
	for _, value := range values {
		if err := writeCanonicalPayloadValue(writer, value); err != nil {
			return err
		}
	}
	_, err := writer.Write([]byte("]"))
	return err
}

func writeCanonicalToken(writer canonicalPayloadWriter, tag, value string) error {
	_, err := writer.Write([]byte(tag + ":" + strconv.Itoa(len(value)) + ":" + value + ";"))
	return err
}
