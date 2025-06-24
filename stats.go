package otters

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Statistical Functions for DataFrames

// Count returns the number of non-null rows in the DataFrame
func (df *DataFrame) Count() int {
	if df.err != nil {
		return 0
	}
	return df.length
}

// Sum calculates the sum of a numeric column
func (df *DataFrame) Sum(column string) (float64, error) {
	if df.err != nil {
		return 0, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return 0, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return 0, newColumnError("Sum", column, "column must be numeric (int64 or float64)")
	}

	sum := 0.0
	for i := 0; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return 0, wrapColumnError("Sum", column, err)
		}

		switch v := value.(type) {
		case int64:
			sum += float64(v)
		case float64:
			sum += v
		}
	}

	return sum, nil
}

// Mean calculates the average of a numeric column
func (df *DataFrame) Mean(column string) (float64, error) {
	if df.err != nil {
		return 0, df.err
	}

	if err := df.validateNotEmpty(); err != nil {
		return 0, err
	}

	sum, err := df.Sum(column)
	if err != nil {
		return 0, err
	}

	return sum / float64(df.length), nil
}

// Min finds the minimum value in a numeric column
func (df *DataFrame) Min(column string) (interface{}, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	if err := df.validateNotEmpty(); err != nil {
		return nil, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return nil, newColumnError("Min", column, "column must be numeric (int64 or float64)")
	}

	firstValue, err := series.Get(0)
	if err != nil {
		return nil, wrapColumnError("Min", column, err)
	}

	min := convertToFloat64(firstValue)
	for i := 1; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return nil, wrapColumnError("Min", column, err)
		}

		floatValue := convertToFloat64(value)
		if floatValue < min {
			min = floatValue
		}
	}

	// Return in original type
	if series.Type == Int64Type {
		return int64(min), nil
	}
	return min, nil
}

// Max finds the maximum value in a numeric column
func (df *DataFrame) Max(column string) (interface{}, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	if err := df.validateNotEmpty(); err != nil {
		return nil, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return nil, newColumnError("Max", column, "column must be numeric (int64 or float64)")
	}

	firstValue, err := series.Get(0)
	if err != nil {
		return nil, wrapColumnError("Max", column, err)
	}

	max := convertToFloat64(firstValue)
	for i := 1; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return nil, wrapColumnError("Max", column, err)
		}

		floatValue := convertToFloat64(value)
		if floatValue > max {
			max = floatValue
		}
	}

	// Return in original type
	if series.Type == Int64Type {
		return int64(max), nil
	}
	return max, nil
}

// Std calculates the standard deviation of a numeric column
func (df *DataFrame) Std(column string) (float64, error) {
	if df.err != nil {
		return 0, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return 0, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return 0, newColumnError("Std", column, "column must be numeric (int64 or float64)")
	}

	if series.Length <= 1 {
		return 0, newColumnError("Std", column, "need at least 2 values to calculate standard deviation")
	}

	// Calculate mean
	mean, err := df.Mean(column)
	if err != nil {
		return 0, err
	}

	// Calculate variance
	variance := 0.0
	for i := 0; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return 0, wrapColumnError("Std", column, err)
		}

		floatValue := convertToFloat64(value)
		diff := floatValue - mean
		variance += diff * diff
	}

	variance /= float64(series.Length - 1) // Sample standard deviation
	return math.Sqrt(variance), nil
}

// Var calculates the variance of a numeric column
func (df *DataFrame) Var(column string) (float64, error) {
	std, err := df.Std(column)
	if err != nil {
		return 0, err
	}
	return std * std, nil
}

// Median calculates the median of a numeric column
func (df *DataFrame) Median(column string) (float64, error) {
	if df.err != nil {
		return 0, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return 0, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return 0, newColumnError("Median", column, "column must be numeric (int64 or float64)")
	}

	if err := df.validateNotEmpty(); err != nil {
		return 0, err
	}

	// Extract and sort values
	values := make([]float64, series.Length)
	for i := 0; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return 0, wrapColumnError("Median", column, err)
		}
		values[i] = convertToFloat64(value)
	}

	sort.Float64s(values)

	n := len(values)
	if n%2 == 0 {
		// Even number of elements - average of middle two
		return (values[n/2-1] + values[n/2]) / 2.0, nil
	}
	// Odd number of elements - middle element
	return values[n/2], nil
}

// Quantile calculates the specified quantile of a numeric column
func (df *DataFrame) Quantile(column string, q float64) (float64, error) {
	if df.err != nil {
		return 0, df.err
	}

	if q < 0 || q > 1 {
		return 0, newOpError("Quantile", "quantile must be between 0 and 1")
	}

	if err := df.validateColumnExists(column); err != nil {
		return 0, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return 0, newColumnError("Quantile", column, "column must be numeric (int64 or float64)")
	}

	if err := df.validateNotEmpty(); err != nil {
		return 0, err
	}

	// Extract and sort values
	values := make([]float64, series.Length)
	for i := 0; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return 0, wrapColumnError("Quantile", column, err)
		}
		values[i] = convertToFloat64(value)
	}

	sort.Float64s(values)

	// Calculate quantile using linear interpolation
	n := float64(len(values))
	index := q * (n - 1)

	if index == math.Trunc(index) {
		return values[int(index)], nil
	}

	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	weight := index - math.Floor(index)

	return values[lower]*(1-weight) + values[upper]*weight, nil
}

// Describe generates summary statistics for all numeric columns (like Pandas describe())
func (df *DataFrame) Describe() (*DataFrame, error) {
	if df.err != nil {
		return nil, df.err
	}

	// Find numeric columns
	var numericColumns []string
	for _, colName := range df.order {
		series := df.columns[colName]
		if series.Type == Int64Type || series.Type == Float64Type {
			numericColumns = append(numericColumns, colName)
		}
	}

	if len(numericColumns) == 0 {
		return nil, newOpError("Describe", "no numeric columns found")
	}

	// Statistics to calculate
	stats := []string{"count", "mean", "std", "min", "25%", "50%", "75%", "max"}

	// Create result data
	resultData := make(map[string]interface{})
	resultData["statistic"] = stats

	// Calculate statistics for each numeric column
	for _, colName := range numericColumns {
		values := make([]string, len(stats))

		// Count
		values[0] = strconv.Itoa(df.length)

		// Mean
		if mean, err := df.Mean(colName); err == nil {
			values[1] = fmt.Sprintf("%.6f", mean)
		} else {
			values[1] = "NaN"
		}

		// Standard deviation
		if std, err := df.Std(colName); err == nil {
			values[2] = fmt.Sprintf("%.6f", std)
		} else {
			values[2] = "NaN"
		}

		// Min
		if min, err := df.Min(colName); err == nil {
			values[3] = fmt.Sprintf("%.6f", convertToFloat64(min))
		} else {
			values[3] = "NaN"
		}

		// 25th percentile
		if q25, err := df.Quantile(colName, 0.25); err == nil {
			values[4] = fmt.Sprintf("%.6f", q25)
		} else {
			values[4] = "NaN"
		}

		// Median (50th percentile)
		if median, err := df.Median(colName); err == nil {
			values[5] = fmt.Sprintf("%.6f", median)
		} else {
			values[5] = "NaN"
		}

		// 75th percentile
		if q75, err := df.Quantile(colName, 0.75); err == nil {
			values[6] = fmt.Sprintf("%.6f", q75)
		} else {
			values[6] = "NaN"
		}

		// Max
		if max, err := df.Max(colName); err == nil {
			values[7] = fmt.Sprintf("%.6f", convertToFloat64(max))
		} else {
			values[7] = "NaN"
		}

		resultData[colName] = values
	}

	return NewDataFrameFromMap(resultData)
}

// ValueCounts returns the frequency of each unique value in a column
func (df *DataFrame) ValueCounts(column string) (*DataFrame, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	series := df.columns[column]
	counts := make(map[string]int)

	// Count occurrences
	for i := 0; i < series.Length; i++ {
		value, err := series.Get(i)
		if err != nil {
			return nil, wrapColumnError("ValueCounts", column, err)
		}

		key := fmt.Sprintf("%v", value)
		counts[key]++
	}

	// Create result DataFrame
	var values []string
	var frequencies []int64

	// Sort by count (descending)
	type countPair struct {
		value string
		count int
	}

	var pairs []countPair
	for value, count := range counts {
		pairs = append(pairs, countPair{value, count})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].count > pairs[j].count
	})

	for _, pair := range pairs {
		values = append(values, pair.value)
		frequencies = append(frequencies, int64(pair.count))
	}

	resultData := map[string]interface{}{
		column:  values,
		"count": frequencies,
	}

	return NewDataFrameFromMap(resultData)
}

// Correlation calculates correlation matrix for numeric columns
func (df *DataFrame) Correlation() (*DataFrame, error) {
	if df.err != nil {
		return nil, df.err
	}

	// Find numeric columns
	var numericColumns []string
	for _, colName := range df.order {
		series := df.columns[colName]
		if series.Type == Int64Type || series.Type == Float64Type {
			numericColumns = append(numericColumns, colName)
		}
	}

	if len(numericColumns) < 2 {
		return nil, newOpError("Correlation", "need at least 2 numeric columns for correlation")
	}

	// Calculate correlation matrix
	n := len(numericColumns)
	resultData := make(map[string]interface{})
	resultData["column"] = numericColumns

	for _, col1 := range numericColumns {
		correlations := make([]float64, n)

		for j, col2 := range numericColumns {
			corr, err := df.calculateCorrelation(col1, col2)
			if err != nil {
				return nil, err
			}
			correlations[j] = corr
		}

		resultData[col1] = correlations
	}

	return NewDataFrameFromMap(resultData)
}

// Helper functions

// convertToFloat64 converts numeric values to float64
func convertToFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	default:
		return 0.0
	}
}

// calculateCorrelation calculates Pearson correlation between two columns
func (df *DataFrame) calculateCorrelation(col1, col2 string) (float64, error) {
	if col1 == col2 {
		return 1.0, nil
	}

	series1 := df.columns[col1]
	series2 := df.columns[col2]

	if series1.Length != series2.Length {
		return 0, newOpError("calculateCorrelation", "columns must have the same length")
	}

	// Calculate means
	mean1, err := df.Mean(col1)
	if err != nil {
		return 0, err
	}

	mean2, err := df.Mean(col2)
	if err != nil {
		return 0, err
	}

	// Calculate correlation
	var numerator, sumSq1, sumSq2 float64

	for i := 0; i < series1.Length; i++ {
		val1, err := series1.Get(i)
		if err != nil {
			return 0, err
		}

		val2, err := series2.Get(i)
		if err != nil {
			return 0, err
		}

		x := convertToFloat64(val1) - mean1
		y := convertToFloat64(val2) - mean2

		numerator += x * y
		sumSq1 += x * x
		sumSq2 += y * y
	}

	denominator := math.Sqrt(sumSq1 * sumSq2)
	if denominator == 0 {
		return 0, nil
	}

	return numerator / denominator, nil
}

// NumericSummary provides a quick summary of a numeric column
func (df *DataFrame) NumericSummary(column string) (*NumericStats, error) {
	if df.err != nil {
		return nil, df.err
	}

	if err := df.validateColumnExists(column); err != nil {
		return nil, err
	}

	series := df.columns[column]
	if series.Type != Int64Type && series.Type != Float64Type {
		return nil, newColumnError("NumericSummary", column, "column must be numeric")
	}

	stats := &NumericStats{
		Column: column,
		Count:  df.length,
	}

	// Calculate all statistics
	var err error
	stats.Sum, err = df.Sum(column)
	if err != nil {
		return nil, err
	}

	stats.Mean, err = df.Mean(column)
	if err != nil {
		return nil, err
	}

	minVal, err := df.Min(column)
	if err != nil {
		return nil, err
	}
	stats.Min = convertToFloat64(minVal)

	maxVal, err := df.Max(column)
	if err != nil {
		return nil, err
	}
	stats.Max = convertToFloat64(maxVal)

	stats.Std, err = df.Std(column)
	if err != nil {
		return nil, err
	}

	stats.Median, err = df.Median(column)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// NumericStats holds summary statistics for a numeric column
type NumericStats struct {
	Column string
	Count  int
	Sum    float64
	Mean   float64
	Min    float64
	Max    float64
	Std    float64
	Median float64
}

// String returns a formatted string representation of NumericStats
func (ns *NumericStats) String() string {
	return fmt.Sprintf(`Numeric Summary for %s:
  Count:  %d
  Sum:    %.6f
  Mean:   %.6f
  Std:    %.6f
  Min:    %.6f
  Max:    %.6f
  Median: %.6f`,
		ns.Column, ns.Count, ns.Sum, ns.Mean, ns.Std, ns.Min, ns.Max, ns.Median)
}
