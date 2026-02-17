package otters

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Filter creates a new DataFrame with rows that match the condition
func (df *DataFrame) Filter(column, operator string, value interface{}) *DataFrame {
	if df.err != nil {
		return df
	}

	if err := df.validateColumnExists(column); err != nil {
		return df.setError(err)
	}

	if err := df.validateNotEmpty(); err != nil {
		return df.setError(err)
	}

	series := df.columns[column]

	// Try optimized typed path first
	matchingIndices, err := filterIndicesTyped(series, operator, value)
	if err != nil {
		return df.setError(wrapColumnError("Filter", column, err))
	}

	return df.selectRows(matchingIndices, "Filter")
}

// filterIndicesTyped returns matching indices using typed slice access to avoid boxing.
func filterIndicesTyped(series *Series, operator string, value interface{}) ([]int, error) {
	indices := make([]int, 0, series.Length/4) // pre-allocate for ~25% selectivity

	switch series.Type {
	case Int64Type:
		data := series.Data.([]int64)
		cmp, ok := toInt64(value)
		if !ok {
			return nil, newOpError("Filter", fmt.Sprintf("cannot convert %T to int64", value))
		}
		for i, v := range data {
			if matchInt64(v, operator, cmp) {
				indices = append(indices, i)
			}
		}

	case Float64Type:
		data := series.Data.([]float64)
		cmp, ok := toFloat64(value)
		if !ok {
			return nil, newOpError("Filter", fmt.Sprintf("cannot convert %T to float64", value))
		}
		for i, v := range data {
			if matchFloat64(v, operator, cmp) {
				indices = append(indices, i)
			}
		}

	case StringType:
		data := series.Data.([]string)
		cmp, ok := value.(string)
		if !ok {
			cmp = fmt.Sprintf("%v", value)
		}
		for i, v := range data {
			if matchString(v, operator, cmp) {
				indices = append(indices, i)
			}
		}

	case BoolType:
		data := series.Data.([]bool)
		cmp, ok := value.(bool)
		if !ok {
			return nil, newOpError("Filter", fmt.Sprintf("cannot convert %T to bool", value))
		}
		for i, v := range data {
			if matchBool(v, operator, cmp) {
				indices = append(indices, i)
			}
		}

	case TimeType:
		data := series.Data.([]time.Time)
		cmp, ok := value.(time.Time)
		if !ok {
			return nil, newOpError("Filter", fmt.Sprintf("cannot convert %T to time.Time", value))
		}
		for i, v := range data {
			if matchTime(v, operator, cmp) {
				indices = append(indices, i)
			}
		}
	}

	return indices, nil
}

func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	}
	return 0, false
}

func matchInt64(v int64, op string, cmp int64) bool {
	switch op {
	case "==", "=":
		return v == cmp
	case "!=", "<>":
		return v != cmp
	case ">":
		return v > cmp
	case ">=":
		return v >= cmp
	case "<":
		return v < cmp
	case "<=":
		return v <= cmp
	}
	return false
}

func matchFloat64(v float64, op string, cmp float64) bool {
	switch op {
	case "==", "=":
		return v == cmp
	case "!=", "<>":
		return v != cmp
	case ">":
		return v > cmp
	case ">=":
		return v >= cmp
	case "<":
		return v < cmp
	case "<=":
		return v <= cmp
	}
	return false
}

func matchString(v, op, cmp string) bool {
	switch op {
	case "==", "=":
		return v == cmp
	case "!=", "<>":
		return v != cmp
	case ">":
		return v > cmp
	case ">=":
		return v >= cmp
	case "<":
		return v < cmp
	case "<=":
		return v <= cmp
	case "contains":
		return strings.Contains(v, cmp)
	case "startswith":
		return strings.HasPrefix(v, cmp)
	case "endswith":
		return strings.HasSuffix(v, cmp)
	}
	return false
}

func matchBool(v bool, op string, cmp bool) bool {
	switch op {
	case "==", "=":
		return v == cmp
	case "!=", "<>":
		return v != cmp
	}
	return false
}

func matchTime(v time.Time, op string, cmp time.Time) bool {
	switch op {
	case "==", "=":
		return v.Equal(cmp)
	case "!=", "<>":
		return !v.Equal(cmp)
	case ">":
		return v.After(cmp)
	case ">=":
		return v.After(cmp) || v.Equal(cmp)
	case "<":
		return v.Before(cmp)
	case "<=":
		return v.Before(cmp) || v.Equal(cmp)
	}
	return false
}

// Select creates a new DataFrame with only the specified columns
func (df *DataFrame) Select(columns ...string) *DataFrame {
	if df.err != nil {
		return df
	}

	if len(columns) == 0 {
		return df.setError(newOpError("Select", "at least one column must be specified"))
	}

	if err := df.validateColumnsExist(columns); err != nil {
		return df.setError(err)
	}

	newDf := NewDataFrame()
	newDf.length = df.length

	// Add selected columns in the order specified
	for _, colName := range columns {
		series := df.columns[colName].Copy()
		if err := newDf.addSeriesUnsafe(series); err != nil {
			return df.setError(wrapColumnError("Select", colName, err))
		}
	}

	return newDf
}

// Drop creates a new DataFrame without the specified columns
func (df *DataFrame) Drop(columns ...string) *DataFrame {
	if df.err != nil {
		return df
	}

	if len(columns) == 0 {
		return df.Copy() // No columns to drop, return copy
	}

	// Validate all columns exist
	if err := df.validateColumnsExist(columns); err != nil {
		return df.setError(err)
	}

	// Create set of columns to drop for O(1) lookup
	dropSet := make(map[string]bool)
	for _, col := range columns {
		dropSet[col] = true
	}

	// Select all columns except the ones to drop
	var keepColumns []string
	for _, colName := range df.order {
		if !dropSet[colName] {
			keepColumns = append(keepColumns, colName)
		}
	}

	if len(keepColumns) == 0 {
		return df.setError(newOpError("Drop", "cannot drop all columns"))
	}

	return df.Select(keepColumns...)
}

// Sort creates a new DataFrame sorted by the specified column
func (df *DataFrame) Sort(column string, ascending bool) *DataFrame {
	return df.SortBy([]string{column}, []bool{ascending})
}

// SortBy creates a new DataFrame sorted by multiple columns
func (df *DataFrame) SortBy(columns []string, ascending []bool) *DataFrame {
	if df.err != nil {
		return df
	}

	if len(columns) == 0 {
		return df.setError(newOpError("SortBy", "at least one column must be specified"))
	}

	if len(columns) != len(ascending) {
		return df.setError(newOpError("SortBy", "columns and ascending arrays must have the same length"))
	}

	if err := df.validateColumnsExist(columns); err != nil {
		return df.setError(err)
	}

	if err := df.validateNotEmpty(); err != nil {
		return df.setError(err)
	}

	// Create index array to sort
	indices := make([]int, df.length)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on column values
	sort.Slice(indices, func(i, j int) bool {
		rowI, rowJ := indices[i], indices[j]

		// Compare by each column in order
		for k, colName := range columns {
			series := df.columns[colName]

			valueI, err := series.Get(rowI)
			if err != nil {
				return false // Handle error gracefully in sort
			}

			valueJ, err := series.Get(rowJ)
			if err != nil {
				return false
			}

			cmp := compareValues(valueI, valueJ, series.Type)
			if cmp != 0 {
				if ascending[k] {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false // Equal values
	})

	// Create new DataFrame with sorted rows
	return df.selectRows(indices, "SortBy")
}

// Unique returns unique values from a specified column
func (df *DataFrame) Unique(column string) ([]interface{}, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	series := df.columns[column]
	seen := make(map[string]bool, series.Length/4) // pre-size for ~25% cardinality
	unique := make([]interface{}, 0, series.Length/4)

	// Type-switch once, iterate typed slice directly to avoid per-row boxing
	switch series.Type {
	case StringType:
		data := series.Data.([]string)
		for _, v := range data {
			if !seen[v] {
				seen[v] = true
				unique = append(unique, v)
			}
		}
	case Int64Type:
		data := series.Data.([]int64)
		for _, v := range data {
			key := strconv.FormatInt(v, 10)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, v)
			}
		}
	case Float64Type:
		data := series.Data.([]float64)
		for _, v := range data {
			key := strconv.FormatFloat(v, 'g', -1, 64)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, v)
			}
		}
	case BoolType:
		data := series.Data.([]bool)
		for _, v := range data {
			var key string
			if v {
				key = "true"
			} else {
				key = "false"
			}
			if !seen[key] {
				seen[key] = true
				unique = append(unique, v)
			}
		}
	case TimeType:
		data := series.Data.([]time.Time)
		for _, v := range data {
			key := v.String()
			if !seen[key] {
				seen[key] = true
				unique = append(unique, v)
			}
		}
	}

	return unique, nil
}

// GroupBy groups the DataFrame by the specified column(s)
func (df *DataFrame) GroupBy(columns ...string) *GroupBy {
	if df.err != nil {
		return &GroupBy{df: df, err: df.err}
	}

	if len(columns) == 0 {
		return &GroupBy{df: df, err: newOpError("GroupBy", "at least one column must be specified")}
	}

	if err := df.validateColumnsExist(columns); err != nil {
		return &GroupBy{df: df, err: err}
	}

	return &GroupBy{
		df:      df,
		columns: columns,
		err:     nil,
	}
}

// Where is an alias for Filter (Pandas compatibility)
func (df *DataFrame) Where(column, operator string, value interface{}) *DataFrame {
	return df.Filter(column, operator, value)
}

// Query applies a simple query string to filter the DataFrame
func (df *DataFrame) Query(query string) *DataFrame {
	if df.err != nil {
		return df
	}

	// Parse simple queries like "age > 25" or "name == 'John'"
	parts := strings.Fields(query)
	if len(parts) != 3 {
		return df.setError(newOpError("Query", "query must be in format 'column operator value'"))
	}

	column := parts[0]
	operator := parts[1]
	valueStr := parts[2]

	// Remove quotes if present
	if strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'") {
		valueStr = strings.Trim(valueStr, "'")
	}
	if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
		valueStr = strings.Trim(valueStr, "\"")
	}

	// Convert value to appropriate type based on column type
	if !df.HasColumn(column) {
		return df.setError(newColumnError("Query", column, "column does not exist"))
	}

	columnType, _ := df.GetColumnType(column)
	value, err := ConvertValue(valueStr, columnType)
	if err != nil {
		return df.setError(wrapColumnError("Query", column, err))
	}

	return df.Filter(column, operator, value)
}

// Reset index (currently a no-op, but maintains Pandas compatibility)
func (df *DataFrame) ResetIndex() *DataFrame {
	if df.err != nil {
		return df
	}
	return df.Copy()
}

// GroupBy represents a grouped DataFrame for aggregation operations
type GroupBy struct {
	df      *DataFrame
	columns []string
	err     error
}

// Sum calculates the sum for each group
func (gb *GroupBy) Sum() (*DataFrame, error) {
	return gb.aggregate("sum")
}

// Mean calculates the average for each group
func (gb *GroupBy) Mean() (*DataFrame, error) {
	return gb.aggregate("mean")
}

// Count calculates the count for each group
func (gb *GroupBy) Count() (*DataFrame, error) {
	return gb.aggregate("count")
}

// Min calculates the minimum for each group
func (gb *GroupBy) Min() (*DataFrame, error) {
	return gb.aggregate("min")
}

// Max calculates the maximum for each group
func (gb *GroupBy) Max() (*DataFrame, error) {
	return gb.aggregate("max")
}

// Internal helper methods

// selectRows creates a new DataFrame with rows at the specified indices
func (df *DataFrame) selectRows(indices []int, operation string) *DataFrame {
	if len(indices) == 0 {
		// Return empty DataFrame with same structure
		newDf := NewDataFrame()
		for _, colName := range df.order {
			series := df.columns[colName]
			var emptyData interface{}

			switch series.Type {
			case StringType:
				emptyData = []string{}
			case Int64Type:
				emptyData = []int64{}
			case Float64Type:
				emptyData = []float64{}
			case BoolType:
				emptyData = []bool{}
			case TimeType:
				emptyData = []time.Time{}
			}

			newSeries, err := NewSeries(series.Name, emptyData)
			if err != nil {
				return df.setError(wrapError(operation, err))
			}
			newDf.addSeriesUnsafe(newSeries)
		}
		return newDf
	}

	newDf := NewDataFrame()
	newDf.length = len(indices)

	// Create new series with selected rows
	for _, colName := range df.order {
		series := df.columns[colName]
		var newData interface{}

		switch series.Type {
		case StringType:
			data := series.Data.([]string)
			newSlice := make([]string, len(indices))
			for i, idx := range indices {
				newSlice[i] = data[idx]
			}
			newData = newSlice

		case Int64Type:
			data := series.Data.([]int64)
			newSlice := make([]int64, len(indices))
			for i, idx := range indices {
				newSlice[i] = data[idx]
			}
			newData = newSlice

		case Float64Type:
			data := series.Data.([]float64)
			newSlice := make([]float64, len(indices))
			for i, idx := range indices {
				newSlice[i] = data[idx]
			}
			newData = newSlice

		case BoolType:
			data := series.Data.([]bool)
			newSlice := make([]bool, len(indices))
			for i, idx := range indices {
				newSlice[i] = data[idx]
			}
			newData = newSlice

		case TimeType:
			data := series.Data.([]time.Time)
			newSlice := make([]time.Time, len(indices))
			for i, idx := range indices {
				newSlice[i] = data[idx]
			}
			newData = newSlice

		default:
			return df.setError(newOpError(operation, fmt.Sprintf("unsupported type for column %s", colName)))
		}

		newSeries, err := NewSeries(series.Name, newData)
		if err != nil {
			return df.setError(wrapColumnError(operation, colName, err))
		}

		if err := newDf.addSeriesUnsafe(newSeries); err != nil {
			return df.setError(wrapError(operation, err))
		}
	}

	return newDf
}

// evaluateCondition evaluates a condition against a value
func evaluateCondition(cellValue interface{}, operator string, compareValue interface{}, columnType ColumnType) (bool, error) {
	// Convert compareValue to the same type as cellValue if needed
	convertedValue, err := convertCompareValue(compareValue, columnType)
	if err != nil {
		return false, err
	}

	switch operator {
	case "==", "=":
		return compareValues(cellValue, convertedValue, columnType) == 0, nil
	case "!=", "<>":
		return compareValues(cellValue, convertedValue, columnType) != 0, nil
	case ">":
		return compareValues(cellValue, convertedValue, columnType) > 0, nil
	case ">=":
		return compareValues(cellValue, convertedValue, columnType) >= 0, nil
	case "<":
		return compareValues(cellValue, convertedValue, columnType) < 0, nil
	case "<=":
		return compareValues(cellValue, convertedValue, columnType) <= 0, nil
	case "contains":
		if columnType != StringType {
			return false, newOpError("evaluateCondition", "contains operator only works with string columns")
		}
		cellStr := cellValue.(string)
		compareStr := convertedValue.(string)
		return strings.Contains(cellStr, compareStr), nil
	case "startswith":
		if columnType != StringType {
			return false, newOpError("evaluateCondition", "startswith operator only works with string columns")
		}
		cellStr := cellValue.(string)
		compareStr := convertedValue.(string)
		return strings.HasPrefix(cellStr, compareStr), nil
	case "endswith":
		if columnType != StringType {
			return false, newOpError("evaluateCondition", "endswith operator only works with string columns")
		}
		cellStr := cellValue.(string)
		compareStr := convertedValue.(string)
		return strings.HasSuffix(cellStr, compareStr), nil
	default:
		return false, newOpError("evaluateCondition", fmt.Sprintf("unsupported operator: %s", operator))
	}
}

// compareValues compares two values of the same type, returns -1, 0, or 1
func compareValues(a, b interface{}, columnType ColumnType) int {
	switch columnType {
	case StringType:
		aStr := a.(string)
		bStr := b.(string)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0

	case Int64Type:
		aInt := a.(int64)
		bInt := b.(int64)
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0

	case Float64Type:
		aFloat := a.(float64)
		bFloat := b.(float64)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0

	case BoolType:
		aBool := a.(bool)
		bBool := b.(bool)
		if !aBool && bBool {
			return -1
		} else if aBool && !bBool {
			return 1
		}
		return 0

	case TimeType:
		aTime := a.(time.Time)
		bTime := b.(time.Time)
		if aTime.Before(bTime) {
			return -1
		} else if aTime.After(bTime) {
			return 1
		}
		return 0

	default:
		return 0
	}
}

// convertCompareValue converts a value to the target column type
func convertCompareValue(value interface{}, targetType ColumnType) (interface{}, error) {
	// If already the correct type, return as-is
	switch targetType {
	case StringType:
		if v, ok := value.(string); ok {
			return v, nil
		}
		return fmt.Sprintf("%v", value), nil

	case Int64Type:
		switch v := value.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		default:
			return int64(0), newOpError("convertCompareValue", fmt.Sprintf("cannot convert %v to int64", value))
		}

	case Float64Type:
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case int:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return float64(0), newOpError("convertCompareValue", fmt.Sprintf("cannot convert %v to float64", value))
		}

	case BoolType:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		default:
			return false, newOpError("convertCompareValue", fmt.Sprintf("cannot convert %v to bool", value))
		}

	case TimeType:
		if v, ok := value.(time.Time); ok {
			return v, nil
		}
		if v, ok := value.(string); ok {
			return parseTimeValue(v)
		}
		return time.Time{}, newOpError("convertCompareValue", fmt.Sprintf("cannot convert %v to time", value))

	default:
		return value, nil
	}
}

// aggregate performs aggregation operations for GroupBy
func (gb *GroupBy) aggregate(operation string) (*DataFrame, error) {
	if gb.err != nil {
		return nil, gb.err
	}

	// Create groups; store both the dedup key and the original string values.
	type group struct {
		values  []string
		indices []int
	}
	groups := make(map[string]*group)

	// Pre-cache series pointers for grouping columns (avoids map lookup per row)
	groupSeries := make([]*Series, len(gb.columns))
	for j, col := range gb.columns {
		groupSeries[j] = gb.df.columns[col]
	}

	// Pre-allocate key builder with reasonable capacity
	var key strings.Builder
	key.Grow(64)

	for i := 0; i < gb.df.length; i++ {
		key.Reset()
		values := make([]string, len(gb.columns))
		for j, series := range groupSeries {
			if j > 0 {
				key.WriteByte(0) // null byte — cannot appear in normal string data
			}
			// Type-switch to avoid interface{} boxing and fmt.Sprintf
			var part string
			switch series.Type {
			case StringType:
				part = series.Data.([]string)[i]
			case Int64Type:
				part = strconv.FormatInt(series.Data.([]int64)[i], 10)
			case Float64Type:
				part = strconv.FormatFloat(series.Data.([]float64)[i], 'g', -1, 64)
			case BoolType:
				if series.Data.([]bool)[i] {
					part = "true"
				} else {
					part = "false"
				}
			case TimeType:
				part = series.Data.([]time.Time)[i].String()
			}
			values[j] = part
			// Length-prefix for collision resistance
			key.WriteString(strconv.Itoa(len(part)))
			key.WriteByte(':')
			key.WriteString(part)
		}
		k := key.String()
		if _, exists := groups[k]; !exists {
			groups[k] = &group{values: values}
		}
		groups[k].indices = append(groups[k].indices, i)
	}

	numGroups := len(groups)

	// Sort group keys for deterministic output order
	sortedKeys := make([]string, 0, numGroups)
	for k := range groups {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Pre-allocate result slices with exact capacity
	groupColData := make([][]string, len(gb.columns))
	for j := range gb.columns {
		groupColData[j] = make([]string, 0, numGroups)
	}

	// Identify numeric columns and pre-allocate their result slices
	type numericCol struct {
		name string
		data []float64
	}
	var numericCols []numericCol
	for _, colName := range gb.df.order {
		if contains(gb.columns, colName) {
			continue
		}
		colType, _ := gb.df.GetColumnType(colName)
		if colType == Int64Type || colType == Float64Type {
			numericCols = append(numericCols, numericCol{
				name: colName,
				data: make([]float64, 0, numGroups),
			})
		}
	}

	// Process each group
	for _, k := range sortedKeys {
		g := groups[k]
		// Add group key values
		for j := range gb.columns {
			groupColData[j] = append(groupColData[j], g.values[j])
		}

		// Calculate aggregations for numeric columns
		for i := range numericCols {
			aggValue, err := gb.calculateAggregation(numericCols[i].name, g.indices, operation)
			if err != nil {
				return nil, err
			}
			numericCols[i].data = append(numericCols[i].data, aggValue)
		}
	}

	// Build result DataFrame directly with NewDataFrameFromSeries (avoids map overhead)
	resultSeries := make([]*Series, 0, len(gb.columns)+len(numericCols))
	for j, col := range gb.columns {
		s, err := NewSeries(col, groupColData[j])
		if err != nil {
			return nil, err
		}
		resultSeries = append(resultSeries, s)
	}
	for _, nc := range numericCols {
		s, err := NewSeries(nc.name, nc.data)
		if err != nil {
			return nil, err
		}
		resultSeries = append(resultSeries, s)
	}

	return NewDataFrameFromSeries(resultSeries...)
}

// calculateAggregation calculates aggregation for a column and indices.
// Optimized to access typed slices directly, avoiding per-row interface{} boxing.
func (gb *GroupBy) calculateAggregation(column string, indices []int, operation string) (float64, error) {
	series := gb.df.columns[column]
	n := len(indices)
	if n == 0 {
		return 0, nil
	}

	// Fast path: access typed slice directly, compute aggregation in one pass
	switch series.Type {
	case Int64Type:
		data := series.Data.([]int64)
		return aggregateInt64(data, indices, operation)
	case Float64Type:
		data := series.Data.([]float64)
		return aggregateFloat64(data, indices, operation)
	default:
		return 0, nil // Non-numeric column
	}
}

// aggregateInt64 computes aggregation on int64 slice for given indices.
func aggregateInt64(data []int64, indices []int, operation string) (float64, error) {
	n := len(indices)
	switch operation {
	case "sum":
		var sum int64
		for _, idx := range indices {
			sum += data[idx]
		}
		return float64(sum), nil
	case "mean":
		var sum int64
		for _, idx := range indices {
			sum += data[idx]
		}
		return float64(sum) / float64(n), nil
	case "count":
		return float64(n), nil
	case "min":
		minVal := data[indices[0]]
		for _, idx := range indices[1:] {
			if data[idx] < minVal {
				minVal = data[idx]
			}
		}
		return float64(minVal), nil
	case "max":
		maxVal := data[indices[0]]
		for _, idx := range indices[1:] {
			if data[idx] > maxVal {
				maxVal = data[idx]
			}
		}
		return float64(maxVal), nil
	default:
		return 0, newOpError("aggregateInt64", fmt.Sprintf("unsupported operation: %s", operation))
	}
}

// aggregateFloat64 computes aggregation on float64 slice for given indices.
func aggregateFloat64(data []float64, indices []int, operation string) (float64, error) {
	n := len(indices)
	switch operation {
	case "sum":
		var sum float64
		for _, idx := range indices {
			sum += data[idx]
		}
		return sum, nil
	case "mean":
		var sum float64
		for _, idx := range indices {
			sum += data[idx]
		}
		return sum / float64(n), nil
	case "count":
		return float64(n), nil
	case "min":
		minVal := data[indices[0]]
		for _, idx := range indices[1:] {
			if data[idx] < minVal {
				minVal = data[idx]
			}
		}
		return minVal, nil
	case "max":
		maxVal := data[indices[0]]
		for _, idx := range indices[1:] {
			if data[idx] > maxVal {
				maxVal = data[idx]
			}
		}
		return maxVal, nil
	default:
		return 0, newOpError("aggregateFloat64", fmt.Sprintf("unsupported operation: %s", operation))
	}
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
