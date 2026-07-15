package otters

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// JSONLOptions provides options for JSONL reading
type JSONLOptions struct {
	SkipRows int // Number of lines to skip at the beginning
	MaxRows  int // Maximum number of rows to read (0 = unlimited)
}

// ReadJSONL reads a JSON Lines file (one flat JSON object per line) and
// returns a DataFrame with automatic type inference.
//
// The schema is the union of keys across all lines, in first-seen order.
// Missing keys and JSON nulls fill with the column type's zero value, the
// same convention as empty CSV cells. JSON's native types are respected: a
// JSON string "123" stays a string, and integer-valued numbers produce
// Int64Type columns. A string column where every non-empty value parses with
// the shared time formats becomes TimeType. Columns whose values mix types
// across lines, or contain nested objects/arrays, become StringType (nested
// values are stringified as compact JSON).
func ReadJSONL(filename string) (*DataFrame, error) {
	return ReadJSONLWithOptions(filename, JSONLOptions{})
}

// ReadJSONLWithOptions reads a JSON Lines file with custom options
func ReadJSONLWithOptions(filename string, options JSONLOptions) (*DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, wrapError("ReadJSONL", err)
	}
	defer file.Close()

	return readJSONL(file, options, "ReadJSONL")
}

// ReadJSONLFromString reads JSON Lines data from a string
func ReadJSONLFromString(data string) (*DataFrame, error) {
	return ReadJSONLFromStringWithOptions(data, JSONLOptions{})
}

// ReadJSONLFromStringWithOptions reads JSON Lines data from a string with options
func ReadJSONLFromStringWithOptions(data string, options JSONLOptions) (*DataFrame, error) {
	return readJSONL(strings.NewReader(data), options, "ReadJSONLFromString")
}

// maxJSONLLineSize bounds a single JSONL line (16 MB).
const maxJSONLLineSize = 16 * 1024 * 1024

// readJSONL parses JSONL from r into a DataFrame.
func readJSONL(r io.Reader, options JSONLOptions, operation string) (*DataFrame, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxJSONLLineSize)

	var rows []map[string]any
	var order []string
	seen := make(map[string]bool)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum <= options.SkipRows {
			continue
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		obj, keys, err := decodeJSONLine(line)
		if err != nil {
			return nil, &OtterError{
				Op:      operation,
				Row:     lineNum,
				Message: fmt.Sprintf("invalid JSONL line: %v", err),
				Cause:   err,
			}
		}

		for _, key := range keys {
			if !seen[key] {
				seen[key] = true
				order = append(order, key)
			}
		}
		rows = append(rows, obj)

		if options.MaxRows > 0 && len(rows) >= options.MaxRows {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, wrapError(operation, err)
	}

	return buildDataFrameFromJSONLRows(order, rows, operation)
}

// decodeJSONLine decodes one JSONL line into a value map plus the object's
// keys in their order of appearance (a plain map decode would lose it).
func decodeJSONLine(line string) (map[string]any, []string, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return nil, nil, err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, nil, fmt.Errorf("line is not a JSON object")
	}

	obj := make(map[string]any)
	var keys []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		key := keyTok.(string)

		var value any
		if err := dec.Decode(&value); err != nil {
			return nil, nil, err
		}

		if _, duplicate := obj[key]; !duplicate {
			keys = append(keys, key)
		}
		obj[key] = value
	}
	if _, err := dec.Token(); err != nil { // consume closing '}'
		return nil, nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, nil, fmt.Errorf("unexpected data after JSON object")
	}

	return obj, keys, nil
}

// buildDataFrameFromJSONLRows constructs a DataFrame from decoded JSONL rows
func buildDataFrameFromJSONLRows(order []string, rows []map[string]any, operation string) (*DataFrame, error) {
	if len(order) == 0 {
		return NewDataFrame(), nil
	}

	series := make([]*Series, 0, len(order))
	for _, name := range order {
		values := make([]any, len(rows))
		for i, row := range rows {
			values[i] = row[name] // missing key yields nil, same as JSON null
		}

		colType := inferJSONLColumnType(values)
		s, err := buildJSONLSeries(name, values, colType)
		if err != nil {
			return nil, wrapColumnError(operation, name, err)
		}
		series = append(series, s)
	}

	return NewDataFrameFromSeries(series...)
}

// inferJSONLColumnType picks a column type from decoded JSON values.
// Unlike CSV inference, JSON values carry their own types, so strings are
// never reinterpreted as numbers or bools; the only string promotion is to
// TimeType when every non-empty value parses as a time.
func inferJSONLColumnType(values []any) ColumnType {
	sawNumber := false
	sawFloat := false
	sawBool := false
	sawString := false
	sawNested := false
	allStringsAreTime := true
	sawNonEmptyString := false

	for _, v := range values {
		switch t := v.(type) {
		case nil:
			continue
		case json.Number:
			sawNumber = true
			if _, err := strconv.ParseInt(string(t), 10, 64); err != nil {
				sawFloat = true
			}
		case bool:
			sawBool = true
		case string:
			sawString = true
			trimmed := strings.TrimSpace(t)
			if trimmed != "" {
				sawNonEmptyString = true
				if !isTimeValue(trimmed) {
					allStringsAreTime = false
				}
			}
		default: // map[string]any or []any
			sawNested = true
		}
	}

	categories := 0
	for _, saw := range []bool{sawNumber, sawBool, sawString, sawNested} {
		if saw {
			categories++
		}
	}

	// No values, mixed types, or nested values: fall back to string.
	if categories != 1 || sawNested {
		return StringType
	}
	if sawNumber {
		if sawFloat {
			return Float64Type
		}
		return Int64Type
	}
	if sawBool {
		return BoolType
	}
	if sawNonEmptyString && allStringsAreTime {
		return TimeType
	}
	return StringType
}

// buildJSONLSeries converts decoded JSON values into a typed Series.
// nil (JSON null or missing key) becomes the column type's zero value.
func buildJSONLSeries(name string, values []any, colType ColumnType) (*Series, error) {
	switch colType {
	case Int64Type:
		data := make([]int64, len(values))
		for i, v := range values {
			if v == nil {
				continue
			}
			n, err := v.(json.Number).Int64()
			if err != nil {
				return nil, wrapError("buildJSONLSeries", err)
			}
			data[i] = n
		}
		return newSeriesOwned(name, data)

	case Float64Type:
		data := make([]float64, len(values))
		for i, v := range values {
			if v == nil {
				continue
			}
			f, err := v.(json.Number).Float64()
			if err != nil {
				return nil, wrapError("buildJSONLSeries", err)
			}
			data[i] = f
		}
		return newSeriesOwned(name, data)

	case BoolType:
		data := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				continue
			}
			data[i] = v.(bool)
		}
		return newSeriesOwned(name, data)

	case TimeType:
		data := make([]time.Time, len(values))
		for i, v := range values {
			if v == nil {
				continue
			}
			trimmed := strings.TrimSpace(v.(string))
			if trimmed == "" {
				continue
			}
			t, err := parseTimeValue(trimmed)
			if err != nil {
				return nil, wrapError("buildJSONLSeries", err)
			}
			data[i] = t
		}
		return newSeriesOwned(name, data)

	default: // StringType
		data := make([]string, len(values))
		for i, v := range values {
			s, err := formatJSONValueAsString(v)
			if err != nil {
				return nil, wrapError("buildJSONLSeries", err)
			}
			data[i] = s
		}
		return newSeriesOwned(name, data)
	}
}

// formatJSONValueAsString renders a decoded JSON value canonically for a
// string column; nested values become compact JSON.
func formatJSONValueAsString(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", nil
	case string:
		return t, nil
	case json.Number:
		return string(t), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	}
}

// WriteJSONL writes a DataFrame to a JSON Lines file, one object per row
// with keys in column order. Times are written as RFC3339 strings; zero
// times, NaN, and ±Inf are written as null.
func (df *DataFrame) WriteJSONL(filename string) error {
	if df.err != nil {
		return df.err
	}

	file, err := os.Create(filename)
	if err != nil {
		return wrapError("WriteJSONL", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var buf bytes.Buffer

	for i := 0; i < df.length; i++ {
		buf.Reset()
		buf.WriteByte('{')

		for j, colName := range df.order {
			if j > 0 {
				buf.WriteByte(',')
			}
			key, err := json.Marshal(colName)
			if err != nil {
				return wrapColumnError("WriteJSONL", colName, err)
			}
			buf.Write(key)
			buf.WriteByte(':')

			value, err := df.columns[colName].Get(i)
			if err != nil {
				return wrapColumnError("WriteJSONL", colName, err)
			}
			formatted, err := formatValueForJSONL(value)
			if err != nil {
				return wrapColumnError("WriteJSONL", colName, err)
			}
			buf.WriteString(formatted)
		}

		buf.WriteByte('}')
		buf.WriteByte('\n')

		if _, err := writer.Write(buf.Bytes()); err != nil {
			return wrapError("WriteJSONL", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return wrapError("WriteJSONL", err)
	}
	return nil
}

// formatValueForJSONL formats a single cell as a JSON value
func formatValueForJSONL(value any) (string, error) {
	switch v := value.(type) {
	case string:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "null", nil
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	case bool:
		return strconv.FormatBool(v), nil
	case time.Time:
		if v.IsZero() {
			return "null", nil
		}
		return `"` + v.Format(time.RFC3339) + `"`, nil
	default:
		return "", fmt.Errorf("unsupported value type: %T", value)
	}
}
