package otters

import (
	"strings"
	"testing"
)

func TestDataFrame_Count(t *testing.T) {
	data := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
		"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
	}
	df, _ := NewDataFrameFromMap(data)

	count := df.Count()
	if count != 5 {
		t.Errorf("Count() = %v, want 5", count)
	}

	emptyDf := NewDataFrame()
	if emptyDf.Count() != 0 {
		t.Error("Count() should return 0 for empty DataFrame")
	}
}

func TestNumericStats_String(t *testing.T) {
	stats := &NumericStats{
		Count:  10,
		Mean:   5.5,
		Std:    2.87,
		Min:    1.0,
		Median: 5.5,
		Max:    10.0,
	}

	str := stats.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
	if !strings.Contains(str, "Count") || !strings.Contains(str, "Mean") {
		t.Error("String() should contain stat names")
	}
}

func TestDataFrame_SumEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Sum("col1")
	if err == nil {
		t.Error("Sum() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"text": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)
	_, err = df.Sum("text")
	if err == nil {
		t.Error("Sum() should error on non-numeric column")
	}
}

func TestDataFrame_MeanEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Mean("col1")
	if err == nil {
		t.Error("Mean() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)
	mean, err := df.Mean("col1")
	if err != nil {
		t.Errorf("Mean() error = %v", err)
	}
	if mean != 2.0 {
		t.Errorf("Mean() = %v, want 2.0", mean)
	}
}

func TestDataFrame_MinEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Min("col1")
	if err == nil {
		t.Error("Min() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []float64{5.5, 2.2, 8.8, 1.1},
	}
	df, _ := NewDataFrameFromMap(data)
	minVal, err := df.Min("col1")
	if err != nil {
		t.Errorf("Min() error = %v", err)
	}
	if minVal != 1.1 {
		t.Errorf("Min() = %v, want 1.1", minVal)
	}
}

func TestDataFrame_MaxEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Max("col1")
	if err == nil {
		t.Error("Max() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []float64{5.5, 2.2, 8.8, 1.1},
	}
	df, _ := NewDataFrameFromMap(data)
	maxVal, err := df.Max("col1")
	if err != nil {
		t.Errorf("Max() error = %v", err)
	}
	if maxVal != 8.8 {
		t.Errorf("Max() = %v, want 8.8", maxVal)
	}
}

func TestDataFrame_StdEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Std("col1")
	if err == nil {
		t.Error("Std() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)
	_, err = df.Std("col1")
	if err != nil {
		t.Errorf("Std() error = %v", err)
	}
}

func TestDataFrame_VarEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Var("col1")
	if err == nil {
		t.Error("Var() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
	}
	df, _ := NewDataFrameFromMap(data)
	_, err = df.Var("col1")
	if err != nil {
		t.Errorf("Var() error = %v", err)
	}
}

func TestDataFrame_MedianEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Median("col1")
	if err == nil {
		t.Error("Median() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	}
	df, _ := NewDataFrameFromMap(data)
	median, err := df.Median("col1")
	if err != nil {
		t.Errorf("Median() error = %v", err)
	}
	if median != 3.0 {
		t.Errorf("Median() = %v, want 3", median)
	}
}

func TestDataFrame_QuantileEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Quantile("col1", 0.5)
	if err == nil {
		t.Error("Quantile() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err = df.Quantile("col1", -0.1)
	if err == nil {
		t.Error("Quantile() should error on invalid q < 0")
	}

	_, err = df.Quantile("col1", 1.1)
	if err == nil {
		t.Error("Quantile() should error on invalid q > 1")
	}

	_, err = df.Quantile("col1", 0.75)
	if err != nil {
		t.Errorf("Quantile() error = %v", err)
	}
}

func TestDataFrame_DescribeEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Describe()
	if err == nil {
		t.Error("Describe() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
		"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
	}
	df, _ := NewDataFrameFromMap(data)
	result, err := df.Describe()
	if err != nil {
		t.Errorf("Describe() error = %v", err)
	}

	if result.Len() < 5 {
		t.Errorf("Describe() should return multiple rows, got %d", result.Len())
	}
}

func TestDataFrame_ValueCountsEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.ValueCounts("col1")
	if err == nil {
		t.Error("ValueCounts() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []string{"a", "b", "a", "c", "b", "a"},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err = df.ValueCounts("nonexistent")
	if err == nil {
		t.Error("ValueCounts() should error on nonexistent column")
	}

	result, err := df.ValueCounts("col1")
	if err != nil {
		t.Errorf("ValueCounts() error = %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("ValueCounts() should return 3 unique values, got %d", result.Len())
	}
}

func TestDataFrame_CorrelationEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.Correlation()
	if err == nil {
		t.Error("Correlation() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"text": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)
	_, err = df.Correlation()
	if err == nil {
		t.Error("Correlation() should error when no numeric columns")
	}

	data2 := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
		"col2": []float64{2.0, 4.0, 6.0, 8.0, 10.0},
	}
	df2, _ := NewDataFrameFromMap(data2)
	_, err = df2.Correlation()
	if err != nil {
		t.Errorf("Correlation() error = %v", err)
	}
}

func TestDataFrame_NumericSummaryEdgeCases(t *testing.T) {
	emptyDf := NewDataFrame()
	_, err := emptyDf.NumericSummary("col1")
	if err == nil {
		t.Error("NumericSummary() should error on empty DataFrame")
	}

	data := map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err = df.NumericSummary("nonexistent")
	if err == nil {
		t.Error("NumericSummary() should error on nonexistent column")
	}

	stats, err := df.NumericSummary("col1")
	if err != nil {
		t.Errorf("NumericSummary() error = %v", err)
	}
	if stats.Count != 5 {
		t.Errorf("NumericSummary() Count = %v, want 5", stats.Count)
	}
}
