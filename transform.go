package otters

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Apply derives a column by evaluating fn once per row. fn receives the row's
// values keyed by column name and returns the new cell value.
//
// The derived column's type is inferred from the returned values: all-integer
// results become Int64Type, a mix of integers and floats becomes Float64Type,
// and uniform string/bool/time results keep their type. Any other mix falls
// back to StringType with canonical formatting. A nil return fills with the
// inferred type's zero value; a column of only nils is StringType.
//
// If newColumn already exists it is replaced in place (same position,
// possibly a new type); otherwise the column is appended. The receiver is
// never mutated, and a panic inside fn is captured as an error on the
// returned frame.
func (df *DataFrame) Apply(newColumn string, fn func(row map[string]any) any) (result *DataFrame) {
	const op = "Apply"
	if df.err != nil {
		return df
	}
	if newColumn == "" {
		return df.setError(newOpError(op, "new column name must not be empty"))
	}
	if fn == nil {
		return df.setError(newOpError(op, "fn must not be nil"))
	}

	// User code runs inside fn: convert a panic into a carried error instead
	// of letting it escape ("no panics" design promise).
	defer func() {
		if r := recover(); r != nil {
			result = df.setError(panicToError(op, r))
		}
	}()

	seriesList := make([]*Series, len(df.order))
	for i, name := range df.order {
		seriesList[i] = df.columns[name]
	}

	values := make([]any, df.length)
	for i := 0; i < df.length; i++ {
		row := make(map[string]any, len(seriesList))
		for _, s := range seriesList {
			v, err := s.Get(i)
			if err != nil {
				return df.setError(wrapColumnError(op, s.Name, err))
			}
			row[s.Name] = v
		}
		values[i] = fn(row)
	}

	return df.withDerivedColumn(op, newColumn, values)
}

// Map derives a column by transforming srcColumn element-wise with fn.
// newColumn may equal srcColumn to replace it. Type inference, replacement,
// immutability, and panic capture behave exactly like Apply.
func (df *DataFrame) Map(srcColumn, newColumn string, fn func(v any) any) (result *DataFrame) {
	const op = "Map"
	if df.err != nil {
		return df
	}
	if err := df.validateColumnExists(srcColumn); err != nil {
		return df.setError(err)
	}
	if newColumn == "" {
		return df.setError(newOpError(op, "new column name must not be empty"))
	}
	if fn == nil {
		return df.setError(newOpError(op, "fn must not be nil"))
	}

	defer func() {
		if r := recover(); r != nil {
			result = df.setError(panicToError(op, r))
		}
	}()

	src := df.columns[srcColumn]
	values := make([]any, df.length)
	for i := 0; i < df.length; i++ {
		v, err := src.Get(i)
		if err != nil {
			return df.setError(wrapColumnError(op, srcColumn, err))
		}
		values[i] = fn(v)
	}

	return df.withDerivedColumn(op, newColumn, values)
}

// FillNA returns a DataFrame with the column's missing values (the column
// type's zero value: "", 0, 0.0, false, zero time) replaced by value. The
// fill value must fit the column type; ints coerce to int64/float64 columns,
// and an integral float may fill an int64 column. Note that a legitimate
// zero (e.g. an actual 0 in an int column) is indistinguishable from missing
// under this convention.
func (df *DataFrame) FillNA(column string, value any) *DataFrame {
	const op = "FillNA"
	if df.err != nil {
		return df
	}
	if err := df.validateColumnExists(column); err != nil {
		return df.setError(err)
	}

	newDf := df.Copy()
	s := newDf.columns[column]

	switch s.Type {
	case Int64Type:
		fill, ok := fillValueInt64(value)
		if !ok {
			return df.setError(newColumnError(op, column, fmt.Sprintf("cannot use %T value %v as an int64 fill", value, value)))
		}
		data := s.Data.([]int64)
		for i, v := range data {
			if v == 0 {
				data[i] = fill
			}
		}
	case Float64Type:
		fill, ok := toFloat64(value)
		if !ok {
			return df.setError(newColumnError(op, column, fmt.Sprintf("cannot use %T value %v as a float64 fill", value, value)))
		}
		data := s.Data.([]float64)
		for i, v := range data {
			if v == 0 {
				data[i] = fill
			}
		}
	case StringType:
		fill, ok := value.(string)
		if !ok {
			return df.setError(newColumnError(op, column, fmt.Sprintf("cannot use %T value %v as a string fill", value, value)))
		}
		data := s.Data.([]string)
		for i, v := range data {
			if v == "" {
				data[i] = fill
			}
		}
	case BoolType:
		fill, ok := value.(bool)
		if !ok {
			return df.setError(newColumnError(op, column, fmt.Sprintf("cannot use %T value %v as a bool fill", value, value)))
		}
		data := s.Data.([]bool)
		for i, v := range data {
			if !v {
				data[i] = fill
			}
		}
	case TimeType:
		fill, ok := value.(time.Time)
		if !ok {
			return df.setError(newColumnError(op, column, fmt.Sprintf("cannot use %T value %v as a time fill", value, value)))
		}
		data := s.Data.([]time.Time)
		for i, v := range data {
			if v.IsZero() {
				data[i] = fill
			}
		}
	default:
		return df.setError(newColumnError(op, column, "unsupported column type"))
	}

	return newDf
}

// DropNA returns a DataFrame without the rows holding a missing value (the
// column type's zero value) in any of the given columns. With no columns
// given, every column is checked. Row order is preserved.
func (df *DataFrame) DropNA(columns ...string) *DataFrame {
	const op = "DropNA"
	if df.err != nil {
		return df
	}
	cols := columns
	if len(cols) == 0 {
		cols = df.order
	}
	if err := df.validateColumnsExist(cols); err != nil {
		return df.setError(err)
	}
	if df.length == 0 {
		return df.Copy()
	}

	masks := make([][]bool, len(cols))
	for j, c := range cols {
		masks[j] = seriesNullMask(df.columns[c])
	}

	keep := make([]int, 0, df.length)
	for i := 0; i < df.length; i++ {
		hasNull := false
		for _, m := range masks {
			if m[i] {
				hasNull = true
				break
			}
		}
		if !hasNull {
			keep = append(keep, i)
		}
	}

	return df.selectRows(keep, op)
}

// seriesNullMask marks each row whose value is the column type's zero value.
func seriesNullMask(s *Series) []bool {
	switch s.Type {
	case Int64Type:
		return nullMaskInt64(s.Data.([]int64))
	case Float64Type:
		return nullMaskFloat64(s.Data.([]float64))
	case StringType:
		return nullMaskString(s.Data.([]string))
	case BoolType:
		return nullMaskBool(s.Data.([]bool))
	case TimeType:
		return nullMaskTime(s.Data.([]time.Time))
	default:
		return make([]bool, s.Length)
	}
}

// withDerivedColumn builds a typed series from boxed values and returns a
// copy of df with it added, or replacing an existing column of the same name.
func (df *DataFrame) withDerivedColumn(op, name string, values []any) *DataFrame {
	data, err := anyValuesToTypedSlice(op, values)
	if err != nil {
		return df.setError(err)
	}
	series, err := newSeriesOwned(name, data)
	if err != nil {
		return df.setError(wrapColumnError(op, name, err))
	}

	newDf := df.Copy()
	if _, exists := newDf.columns[name]; exists {
		newDf.columns[name] = series // replaced in place: order position kept
	} else {
		newDf.columns[name] = series
		newDf.order = append(newDf.order, name)
	}
	return newDf
}

// anyValuesToTypedSlice infers the best column type for boxed values and
// materializes the matching typed slice. See Apply for the inference rules.
func anyValuesToTypedSlice(op string, values []any) (any, error) {
	var sawInt, sawFloat, sawString, sawBool, sawTime bool

	for i, v := range values {
		switch v.(type) {
		case nil:
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
			sawInt = true
		case float32, float64:
			sawFloat = true
		case string:
			sawString = true
		case bool:
			sawBool = true
		case time.Time:
			sawTime = true
		default:
			return nil, newRowError(op, i, fmt.Sprintf("unsupported derived value type %T", v))
		}
	}

	kinds := 0
	for _, saw := range []bool{sawInt || sawFloat, sawString, sawBool, sawTime} {
		if saw {
			kinds++
		}
	}

	switch {
	case kinds > 1:
		return derivedStrings(values), nil
	case sawFloat:
		return derivedFloats(values), nil
	case sawInt:
		return derivedInts(values), nil
	case sawBool:
		return derivedBools(values), nil
	case sawTime:
		return derivedTimes(values), nil
	default:
		// Only strings, or only nils: a column of no values carries no type
		// information, matching InferType's behavior.
		return derivedStrings(values), nil
	}
}

func derivedInts(values []any) []int64 {
	out := make([]int64, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		out[i], _ = anyToInt64(v)
	}
	return out
}

func derivedFloats(values []any) []float64 {
	out := make([]float64, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		out[i], _ = anyToFloat64(v)
	}
	return out
}

func derivedBools(values []any) []bool {
	out := make([]bool, len(values))
	for i, v := range values {
		if b, ok := v.(bool); ok {
			out[i] = b
		}
	}
	return out
}

func derivedTimes(values []any) []time.Time {
	out := make([]time.Time, len(values))
	for i, v := range values {
		if t, ok := v.(time.Time); ok {
			out[i] = t
		}
	}
	return out
}

// derivedStrings renders mixed-kind values with the library's canonical
// formatting (the same rendering WriteCSV and String use).
func derivedStrings(values []any) []string {
	out := make([]string, len(values))
	for i, v := range values {
		switch x := v.(type) {
		case nil:
			out[i] = ""
		case string:
			out[i] = x
		case bool:
			if x {
				out[i] = "true"
			} else {
				out[i] = "false"
			}
		case float32:
			out[i] = strconv.FormatFloat(float64(x), 'g', -1, 64)
		case float64:
			out[i] = strconv.FormatFloat(x, 'g', -1, 64)
		case time.Time:
			out[i] = x.String()
		default:
			n, _ := anyToInt64(x)
			out[i] = strconv.FormatInt(n, 10)
		}
	}
	return out
}

// anyToInt64 widens any supported integer kind to int64.
func anyToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	}
	return 0, false
}

// anyToFloat64 widens any supported numeric kind to float64.
func anyToFloat64(v any) (float64, bool) {
	if f, ok := v.(float64); ok {
		return f, true
	}
	if f, ok := v.(float32); ok {
		return float64(f), true
	}
	if n, ok := anyToInt64(v); ok {
		return float64(n), true
	}
	return 0, false
}

// fillValueInt64 coerces a FillNA value for an int64 column: ints pass
// through, an integral float converts, a fractional float is rejected.
func fillValueInt64(value any) (int64, bool) {
	if f, isFloat := value.(float64); isFloat {
		if f != math.Trunc(f) {
			return 0, false
		}
		return int64(f), true
	}
	return anyToInt64(value)
}
