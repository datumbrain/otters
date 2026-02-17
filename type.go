package otters

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ColumnType represents the data type of a column
type ColumnType int

const (
	StringType ColumnType = iota
	Int64Type
	Float64Type
	BoolType
	TimeType
)

// String returns the string representation of a ColumnType
func (ct ColumnType) String() string {
	switch ct {
	case StringType:
		return "string"
	case Int64Type:
		return "int64"
	case Float64Type:
		return "float64"
	case BoolType:
		return "bool"
	case TimeType:
		return "time"
	default:
		return "unknown"
	}
}

// Series represents a single column of data with a specific type
type Series struct {
	Name   string      // Column name
	Type   ColumnType  // Data type
	Data   interface{} // Actual data: []string, []int64, []float64, []bool, []time.Time
	Length int         // Number of elements
}

// NewSeries creates a new Series with the given name and data
func NewSeries(name string, data interface{}) (*Series, error) {
	s := &Series{
		Name: name,
		Data: data,
	}

	// Determine type and length based on data
	switch d := data.(type) {
	case []string:
		s.Type = StringType
		s.Length = len(d)
	case []int64:
		s.Type = Int64Type
		s.Length = len(d)
	case []float64:
		s.Type = Float64Type
		s.Length = len(d)
	case []bool:
		s.Type = BoolType
		s.Length = len(d)
	case []time.Time:
		s.Type = TimeType
		s.Length = len(d)
	default:
		return nil, &OtterError{
			Op:      "NewSeries",
			Message: fmt.Sprintf("unsupported data type: %T", data),
		}
	}

	return s, nil
}

// Get returns the value at the specified index
func (s *Series) Get(index int) (interface{}, error) {
	if index < 0 || index >= s.Length {
		return nil, &OtterError{
			Op:      "Series.Get",
			Column:  s.Name,
			Message: fmt.Sprintf("index %d out of range [0:%d]", index, s.Length),
		}
	}

	switch s.Type {
	case StringType:
		return s.Data.([]string)[index], nil
	case Int64Type:
		return s.Data.([]int64)[index], nil
	case Float64Type:
		return s.Data.([]float64)[index], nil
	case BoolType:
		return s.Data.([]bool)[index], nil
	case TimeType:
		return s.Data.([]time.Time)[index], nil
	default:
		return nil, &OtterError{
			Op:      "Series.Get",
			Column:  s.Name,
			Message: "unknown column type",
		}
	}
}

// GetInt64 returns the int64 value at the specified index without boxing.
// Returns 0 and error if index is out of range or type mismatch.
func (s *Series) GetInt64(index int) (int64, error) {
	if index < 0 || index >= s.Length {
		return 0, &OtterError{Op: "Series.GetInt64", Column: s.Name,
			Message: fmt.Sprintf("index %d out of range [0:%d]", index, s.Length)}
	}
	if s.Type != Int64Type {
		return 0, &OtterError{Op: "Series.GetInt64", Column: s.Name,
			Message: fmt.Sprintf("type mismatch: expected int64, got %s", s.Type)}
	}
	return s.Data.([]int64)[index], nil
}

// GetFloat64 returns the float64 value at the specified index without boxing.
func (s *Series) GetFloat64(index int) (float64, error) {
	if index < 0 || index >= s.Length {
		return 0, &OtterError{Op: "Series.GetFloat64", Column: s.Name,
			Message: fmt.Sprintf("index %d out of range [0:%d]", index, s.Length)}
	}
	if s.Type != Float64Type {
		return 0, &OtterError{Op: "Series.GetFloat64", Column: s.Name,
			Message: fmt.Sprintf("type mismatch: expected float64, got %s", s.Type)}
	}
	return s.Data.([]float64)[index], nil
}

// GetString returns the string value at the specified index without boxing.
func (s *Series) GetString(index int) (string, error) {
	if index < 0 || index >= s.Length {
		return "", &OtterError{Op: "Series.GetString", Column: s.Name,
			Message: fmt.Sprintf("index %d out of range [0:%d]", index, s.Length)}
	}
	if s.Type != StringType {
		return "", &OtterError{Op: "Series.GetString", Column: s.Name,
			Message: fmt.Sprintf("type mismatch: expected string, got %s", s.Type)}
	}
	return s.Data.([]string)[index], nil
}

// Int64Slice returns the underlying []int64 data directly (no copy).
// Returns nil if type is not Int64Type.
func (s *Series) Int64Slice() []int64 {
	if s.Type == Int64Type {
		return s.Data.([]int64)
	}
	return nil
}

// Float64Slice returns the underlying []float64 data directly (no copy).
func (s *Series) Float64Slice() []float64 {
	if s.Type == Float64Type {
		return s.Data.([]float64)
	}
	return nil
}

// StringSlice returns the underlying []string data directly (no copy).
func (s *Series) StringSlice() []string {
	if s.Type == StringType {
		return s.Data.([]string)
	}
	return nil
}

// Set updates the value at the specified index
func (s *Series) Set(index int, value interface{}) error {
	if index < 0 || index >= s.Length {
		return &OtterError{
			Op:      "Series.Set",
			Column:  s.Name,
			Message: fmt.Sprintf("index %d out of range [0:%d]", index, s.Length),
		}
	}

	switch s.Type {
	case StringType:
		if v, ok := value.(string); ok {
			s.Data.([]string)[index] = v
		} else {
			return &OtterError{
				Op:      "Series.Set",
				Column:  s.Name,
				Message: fmt.Sprintf("expected string, got %T", value),
			}
		}
	case Int64Type:
		if v, ok := value.(int64); ok {
			s.Data.([]int64)[index] = v
		} else {
			return &OtterError{
				Op:      "Series.Set",
				Column:  s.Name,
				Message: fmt.Sprintf("expected int64, got %T", value),
			}
		}
	case Float64Type:
		if v, ok := value.(float64); ok {
			s.Data.([]float64)[index] = v
		} else {
			return &OtterError{
				Op:      "Series.Set",
				Column:  s.Name,
				Message: fmt.Sprintf("expected float64, got %T", value),
			}
		}
	case BoolType:
		if v, ok := value.(bool); ok {
			s.Data.([]bool)[index] = v
		} else {
			return &OtterError{
				Op:      "Series.Set",
				Column:  s.Name,
				Message: fmt.Sprintf("expected bool, got %T", value),
			}
		}
	case TimeType:
		if v, ok := value.(time.Time); ok {
			s.Data.([]time.Time)[index] = v
		} else {
			return &OtterError{
				Op:      "Series.Set",
				Column:  s.Name,
				Message: fmt.Sprintf("expected time.Time, got %T", value),
			}
		}
	default:
		return &OtterError{
			Op:      "Series.Set",
			Column:  s.Name,
			Message: "unknown column type",
		}
	}

	return nil
}

// Copy creates a deep copy of the Series
func (s *Series) Copy() *Series {
	newSeries := &Series{
		Name:   s.Name,
		Type:   s.Type,
		Length: s.Length,
	}

	// Deep copy the data slice
	switch s.Type {
	case StringType:
		data := make([]string, s.Length)
		copy(data, s.Data.([]string))
		newSeries.Data = data
	case Int64Type:
		data := make([]int64, s.Length)
		copy(data, s.Data.([]int64))
		newSeries.Data = data
	case Float64Type:
		data := make([]float64, s.Length)
		copy(data, s.Data.([]float64))
		newSeries.Data = data
	case BoolType:
		data := make([]bool, s.Length)
		copy(data, s.Data.([]bool))
		newSeries.Data = data
	case TimeType:
		data := make([]time.Time, s.Length)
		copy(data, s.Data.([]time.Time))
		newSeries.Data = data
	}

	return newSeries
}

// DataFrame represents a collection of Series with aligned indices
type DataFrame struct {
	columns map[string]*Series // Column name -> Series mapping
	order   []string           // Maintains column order
	length  int                // Number of rows
	err     error              // Error state for chaining operations
}

// NewDataFrame creates a new empty DataFrame
func NewDataFrame() *DataFrame {
	return &DataFrame{
		columns: make(map[string]*Series),
		order:   make([]string, 0),
		length:  0,
		err:     nil,
	}
}

// InferType attempts to infer the best type for a slice of string values
func InferType(values []string) ColumnType {
	if len(values) == 0 {
		return StringType
	}

	// Track what types we can convert to
	canBeInt := true
	canBeFloat := true
	canBeBool := true
	canBeTime := true

	for _, value := range values {
		value = strings.TrimSpace(value)

		// Skip empty values in type inference
		if value == "" {
			continue
		}

		// Check int64
		if canBeInt {
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				canBeInt = false
			}
		}

		// Check float64
		if canBeFloat {
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				canBeFloat = false
			}
		}

		// Check bool
		if canBeBool {
			if _, err := strconv.ParseBool(value); err != nil {
				canBeBool = false
			}
		}

		// Check time (common formats)
		if canBeTime {
			if !isTimeValue(value) {
				canBeTime = false
			}
		}
	}

	// Return the most specific type possible
	if canBeBool {
		return BoolType
	}
	if canBeInt {
		return Int64Type
	}
	if canBeFloat {
		return Float64Type
	}
	if canBeTime {
		return TimeType
	}
	return StringType
}

// isTimeValue checks if a string can be parsed as a time
func isTimeValue(value string) bool {
	// Common time formats to try
	timeFormats := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"01/02/2006",
		"01-02-2006",
		"2006/01/02",
		time.RFC3339,
		time.RFC822,
	}

	for _, format := range timeFormats {
		if _, err := time.Parse(format, value); err == nil {
			return true
		}
	}
	return false
}

// ConvertValue converts a string value to the specified type
func ConvertValue(value string, targetType ColumnType) (interface{}, error) {
	value = strings.TrimSpace(value)

	// Handle empty values
	if value == "" {
		switch targetType {
		case StringType:
			return "", nil
		case Int64Type:
			return int64(0), nil
		case Float64Type:
			return float64(0), nil
		case BoolType:
			return false, nil
		case TimeType:
			return time.Time{}, nil
		}
	}

	switch targetType {
	case StringType:
		return value, nil

	case Int64Type:
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, &OtterError{
				Op:      "ConvertValue",
				Message: fmt.Sprintf("cannot convert '%s' to int64: %v", value, err),
				Cause:   err,
			}
		}
		return val, nil

	case Float64Type:
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, &OtterError{
				Op:      "ConvertValue",
				Message: fmt.Sprintf("cannot convert '%s' to float64: %v", value, err),
				Cause:   err,
			}
		}
		return val, nil

	case BoolType:
		val, err := strconv.ParseBool(value)
		if err != nil {
			return nil, &OtterError{
				Op:      "ConvertValue",
				Message: fmt.Sprintf("cannot convert '%s' to bool: %v", value, err),
				Cause:   err,
			}
		}
		return val, nil

	case TimeType:
		val, err := parseTimeValue(value)
		if err != nil {
			return nil, &OtterError{
				Op:      "ConvertValue",
				Message: fmt.Sprintf("cannot convert '%s' to time: %v", value, err),
				Cause:   err,
			}
		}
		return val, nil

	default:
		return nil, &OtterError{
			Op:      "ConvertValue",
			Message: fmt.Sprintf("unknown target type: %v", targetType),
		}
	}
}

// parseTimeValue attempts to parse a time string using common formats
func parseTimeValue(value string) (time.Time, error) {
	timeFormats := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"01/02/2006",
		"01-02-2006",
		"2006/01/02",
		time.RFC3339,
		time.RFC822,
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, value); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("no matching time format found")
}

// CSVOptions provides options for CSV reading/writing
type CSVOptions struct {
	HasHeader bool // Whether the first row contains headers
	Delimiter rune // Field delimiter (default: ',')
	SkipRows  int  // Number of rows to skip at the beginning
	MaxRows   int  // Maximum number of rows to read (0 = unlimited)
}
