package otters

import (
	"fmt"
	"strings"
	"time"
)

// NewDataFrameFromSeries creates a DataFrame from a collection of Series
func NewDataFrameFromSeries(series ...*Series) (*DataFrame, error) {
	if len(series) == 0 {
		return NewDataFrame(), nil
	}

	// Validate all series have the same length
	if err := validateSameLength(series); err != nil {
		return nil, err
	}

	df := NewDataFrame()
	df.length = series[0].Length

	for _, s := range series {
		if err := df.addSeriesUnsafe(s); err != nil {
			return nil, err
		}
	}

	return df, nil
}

// NewDataFrameFromMap creates a DataFrame from a map of column data
func NewDataFrameFromMap(data map[string]interface{}) (*DataFrame, error) {
	if len(data) == 0 {
		return NewDataFrame(), nil
	}

	var series []*Series
	for name, columnData := range data {
		s, err := NewSeries(name, columnData)
		if err != nil {
			return nil, wrapColumnError("NewDataFrameFromMap", name, err)
		}
		series = append(series, s)
	}

	return NewDataFrameFromSeries(series...)
}

// Basic DataFrame Information Methods

// Shape returns the dimensions of the DataFrame (rows, columns)
func (df *DataFrame) Shape() (int, int) {
	if df.err != nil {
		return 0, 0
	}
	return df.length, len(df.columns)
}

// Columns returns the column names in their defined order
func (df *DataFrame) Columns() []string {
	if df.err != nil {
		return []string{}
	}
	// Return a copy to prevent external modification
	result := make([]string, len(df.order))
	copy(result, df.order)
	return result
}

// Len returns the number of rows in the DataFrame
func (df *DataFrame) Len() int {
	if df.err != nil {
		return 0
	}
	return df.length
}

// Width returns the number of columns in the DataFrame
func (df *DataFrame) Width() int {
	if df.err != nil {
		return 0
	}
	return len(df.columns)
}

// IsEmpty returns true if the DataFrame has no rows or columns
func (df *DataFrame) IsEmpty() bool {
	return df.length == 0 || len(df.columns) == 0
}

// HasColumn returns true if the specified column exists
func (df *DataFrame) HasColumn(name string) bool {
	if df.err != nil {
		return false
	}
	_, exists := df.columns[name]
	return exists
}

// GetColumnType returns the type of the specified column
func (df *DataFrame) GetColumnType(name string) (ColumnType, error) {
	if df.err != nil {
		return StringType, df.err
	}

	if err := df.validateColumnExists(name); err != nil {
		return StringType, err
	}

	return df.columns[name].Type, nil
}

// Data Access Methods

// Head returns the first n rows of the DataFrame
func (df *DataFrame) Head(n int) *DataFrame {
	if df.err != nil {
		return df
	}

	if n <= 0 {
		return df.setError(newOpError("Head", "n must be positive"))
	}

	if n >= df.length {
		return df.Copy() // Return copy of entire DataFrame
	}

	return df.slice(0, n, "Head")
}

// Tail returns the last n rows of the DataFrame
func (df *DataFrame) Tail(n int) *DataFrame {
	if df.err != nil {
		return df
	}

	if n <= 0 {
		return df.setError(newOpError("Tail", "n must be positive"))
	}

	if n >= df.length {
		return df.Copy() // Return copy of entire DataFrame
	}

	start := df.length - n
	return df.slice(start, df.length, "Tail")
}

// Get returns the value at the specified row and column
func (df *DataFrame) Get(row int, column string) (interface{}, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateRowIndex(row); err != nil {
		return nil, err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	return df.columns[column].Get(row)
}

// Set updates the value at the specified row and column
func (df *DataFrame) Set(row int, column string, value interface{}) error {
	if df.err != nil {
		return df.err
	}

	if err := df.validateRowIndex(row); err != nil {
		return err
	}

	if err := df.validateColumnExists(column); err != nil {
		return err
	}

	return df.columns[column].Set(row, value)
}

// GetSeries returns a copy of the specified column as a Series
func (df *DataFrame) GetSeries(name string) (*Series, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(name); err != nil {
		return nil, err
	}

	return df.columns[name].Copy(), nil
}

// DataFrame Manipulation Methods

// Copy creates a deep copy of the DataFrame
func (df *DataFrame) Copy() *DataFrame {
	if df.err != nil {
		// Return a new DataFrame with the same error
		newDf := NewDataFrame()
		newDf.err = df.err
		return newDf
	}

	newDf := NewDataFrame()
	newDf.length = df.length

	// Deep copy all series
	for _, colName := range df.order {
		series := df.columns[colName]
		newDf.columns[colName] = series.Copy()
		newDf.order = append(newDf.order, colName)
	}

	return newDf
}

// AddColumn adds a new Series as a column to the DataFrame
func (df *DataFrame) AddColumn(series *Series) *DataFrame {
	if df.err != nil {
		return df
	}

	// Check if this is the first column
	if len(df.columns) == 0 {
		df.length = series.Length
	} else if series.Length != df.length {
		return df.setError(newColumnError("AddColumn", series.Name,
			fmt.Sprintf("series length %d does not match DataFrame length %d", series.Length, df.length)))
	}

	// Check for duplicate column names
	if _, exists := df.columns[series.Name]; exists {
		return df.setError(newColumnError("AddColumn", series.Name, "column already exists"))
	}

	if err := df.addSeriesUnsafe(series.Copy()); err != nil {
		return df.setError(err)
	}

	return df
}

// DropColumn removes a column from the DataFrame
func (df *DataFrame) DropColumn(name string) *DataFrame {
	if df.err != nil {
		return df
	}

	if err := df.validateColumnExists(name); err != nil {
		return df.setError(err)
	}

	newDf := df.Copy()
	delete(newDf.columns, name)

	// Remove from order slice
	for i, colName := range newDf.order {
		if colName == name {
			newDf.order = append(newDf.order[:i], newDf.order[i+1:]...)
			break
		}
	}

	return newDf
}

// RenameColumn renames a column in the DataFrame
func (df *DataFrame) RenameColumn(oldName, newName string) *DataFrame {
	if df.err != nil {
		return df
	}

	if err := df.validateColumnExists(oldName); err != nil {
		return df.setError(err)
	}

	// Check if new name already exists
	if _, exists := df.columns[newName]; exists && newName != oldName {
		return df.setError(newColumnError("RenameColumn", newName, "column already exists"))
	}

	newDf := df.Copy()

	// Update the series name
	series := newDf.columns[oldName]
	series.Name = newName

	// Update maps and order
	newDf.columns[newName] = series
	delete(newDf.columns, oldName)

	// Update order slice
	for i, colName := range newDf.order {
		if colName == oldName {
			newDf.order[i] = newName
			break
		}
	}

	return newDf
}

// Display and String Methods

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	if df.err != nil {
		return fmt.Sprintf("DataFrame(error: %v)", df.err)
	}

	if df.IsEmpty() {
		return "DataFrame(empty)"
	}

	var sb strings.Builder

	// Write header
	sb.WriteString(strings.Join(df.order, "\t"))
	sb.WriteString("\n")

	// Write data (show first 10 rows max for display)
	maxRows := df.length
	if maxRows > 10 {
		maxRows = 10
	}

	for i := 0; i < maxRows; i++ {
		var row []string
		for _, colName := range df.order {
			value, _ := df.columns[colName].Get(i)
			row = append(row, fmt.Sprintf("%v", value))
		}
		sb.WriteString(strings.Join(row, "\t"))
		sb.WriteString("\n")
	}

	if df.length > 10 {
		sb.WriteString(fmt.Sprintf("... (%d more rows)\n", df.length-10))
	}

	return sb.String()
}

// Info returns basic information about the DataFrame
func (df *DataFrame) Info() string {
	if df.err != nil {
		return fmt.Sprintf("DataFrame Info: Error - %v", df.err)
	}

	var sb strings.Builder
	sb.WriteString("DataFrame Info:\n")
	sb.WriteString(fmt.Sprintf("  Shape: (%d, %d)\n", df.length, len(df.columns)))
	sb.WriteString("  Columns:\n")

	for _, colName := range df.order {
		series := df.columns[colName]
		sb.WriteString(fmt.Sprintf("    %s: %s\n", colName, series.Type.String()))
	}

	return sb.String()
}

// Internal helper methods

// addSeriesUnsafe adds a series without validation (internal use only)
func (df *DataFrame) addSeriesUnsafe(series *Series) error {
	df.columns[series.Name] = series
	df.order = append(df.order, series.Name)
	return nil
}

// slice creates a new DataFrame with rows from start to end (exclusive)
func (df *DataFrame) slice(start, end int, operation string) *DataFrame {
	if start < 0 || end > df.length || start >= end {
		return df.setError(newOpError(operation,
			fmt.Sprintf("invalid slice range [%d:%d] for length %d", start, end, df.length)))
	}

	newDf := NewDataFrame()
	newDf.length = end - start

	for _, colName := range df.order {
		series := df.columns[colName]
		var newData interface{}

		// Slice the appropriate data type
		switch series.Type {
		case StringType:
			data := series.Data.([]string)
			newData = make([]string, end-start)
			copy(newData.([]string), data[start:end])
		case Int64Type:
			data := series.Data.([]int64)
			newData = make([]int64, end-start)
			copy(newData.([]int64), data[start:end])
		case Float64Type:
			data := series.Data.([]float64)
			newData = make([]float64, end-start)
			copy(newData.([]float64), data[start:end])
		case BoolType:
			data := series.Data.([]bool)
			newData = make([]bool, end-start)
			copy(newData.([]bool), data[start:end])
		case TimeType:
			data := series.Data.([]time.Time)
			newData = make([]time.Time, end-start)
			copy(newData.([]time.Time), data[start:end])
		default:
			return df.setError(newOpError(operation, "unsupported column type for slicing"))
		}

		newSeries, err := NewSeries(series.Name, newData)
		if err != nil {
			return df.setError(wrapError(operation, err))
		}

		newDf.addSeriesUnsafe(newSeries)
	}

	return newDf
}

// reset clears all data in the DataFrame
func (df *DataFrame) reset() {
	df.columns = make(map[string]*Series)
	df.order = make([]string, 0)
	df.length = 0
	df.err = nil
}
