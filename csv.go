package otters

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// ReadCSV reads a CSV file and returns a DataFrame with automatic type inference
func ReadCSV(filename string) (*DataFrame, error) {
	return ReadCSVWithOptions(filename, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
		SkipRows:  0,
		MaxRows:   0, // unlimited
	})
}

// ReadCSVWithOptions reads a CSV file with custom options
func ReadCSVWithOptions(filename string, options CSVOptions) (*DataFrame, error) {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, wrapError("ReadCSV", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = options.Delimiter
	reader.TrimLeadingSpace = true

	// Skip initial rows if specified
	for i := 0; i < options.SkipRows; i++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return NewDataFrame(), nil // Empty file after skipping
			}
			return nil, wrapError("ReadCSV", err)
		}
	}

	// Read headers
	var headers []string
	if options.HasHeader {
		headers, err = reader.Read()
		if err != nil {
			if err == io.EOF {
				return NewDataFrame(), nil // Empty file
			}
			return nil, wrapError("ReadCSV", err)
		}

		// Clean headers (remove BOM, trim spaces)
		for i, header := range headers {
			headers[i] = cleanHeader(header)
		}
	} else {
		// Read first row to determine number of columns
		firstRow, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return NewDataFrame(), nil
			}
			return nil, wrapError("ReadCSV", err)
		}

		// Generate column names
		for i := 0; i < len(firstRow); i++ {
			headers = append(headers, fmt.Sprintf("Column_%d", i))
		}

		// Put the first row back (we'll read it again)
		// Note: CSV reader doesn't support seeking, so we'll handle this differently
		allRows := [][]string{firstRow}
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, wrapError("ReadCSV", err)
			}
			allRows = append(allRows, row)

			// Check max rows limit
			if options.MaxRows > 0 && len(allRows) >= options.MaxRows {
				break
			}
		}

		return buildDataFrameFromRows(headers, allRows)
	}

	// Read all data rows
	var rows [][]string
	rowCount := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, wrapError("ReadCSV", err)
		}

		// Validate row length matches headers
		if len(row) != len(headers) {
			return nil, newOpError("ReadCSV",
				fmt.Sprintf("row %d has %d columns, expected %d", rowCount+1, len(row), len(headers)))
		}

		rows = append(rows, row)
		rowCount++

		// Check max rows limit
		if options.MaxRows > 0 && rowCount >= options.MaxRows {
			break
		}
	}

	return buildDataFrameFromRows(headers, rows)
}

// WriteCSV writes a DataFrame to a CSV file
func (df *DataFrame) WriteCSV(filename string) error {
	return df.WriteCSVWithOptions(filename, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
	})
}

// WriteCSVWithOptions writes a DataFrame to CSV with custom options
func (df *DataFrame) WriteCSVWithOptions(filename string, options CSVOptions) error {
	if df.err != nil {
		return df.err
	}

	// Create the file
	file, err := os.Create(filename)
	if err != nil {
		return wrapError("WriteCSV", err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	writer.Comma = options.Delimiter
	defer writer.Flush()

	// Write headers if requested
	if options.HasHeader {
		if err := writer.Write(df.order); err != nil {
			return wrapError("WriteCSV", err)
		}
	}

	// Write data rows
	for i := 0; i < df.length; i++ {
		var row []string
		for _, colName := range df.order {
			value, err := df.columns[colName].Get(i)
			if err != nil {
				return wrapColumnError("WriteCSV", colName, err)
			}
			row = append(row, formatValueForCSV(value))
		}

		if err := writer.Write(row); err != nil {
			return wrapError("WriteCSV", err)
		}
	}

	return nil
}

// ReadCSVFromString reads CSV data from a string
func ReadCSVFromString(data string) (*DataFrame, error) {
	return ReadCSVFromStringWithOptions(data, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
		SkipRows:  0,
		MaxRows:   0,
	})
}

// ReadCSVFromStringWithOptions reads CSV data from a string with options
func ReadCSVFromStringWithOptions(data string, options CSVOptions) (*DataFrame, error) {
	reader := csv.NewReader(strings.NewReader(data))
	reader.Comma = options.Delimiter
	reader.TrimLeadingSpace = true

	// Skip initial rows if specified
	for i := 0; i < options.SkipRows; i++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return NewDataFrame(), nil
			}
			return nil, wrapError("ReadCSVFromString", err)
		}
	}

	// Read headers
	var headers []string
	if options.HasHeader {
		var err error
		headers, err = reader.Read()
		if err != nil {
			if err == io.EOF {
				return NewDataFrame(), nil
			}
			return nil, wrapError("ReadCSVFromString", err)
		}

		// Clean headers
		for i, header := range headers {
			headers[i] = cleanHeader(header)
		}
	}

	// Read all data rows
	var rows [][]string
	rowCount := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, wrapError("ReadCSVFromString", err)
		}

		// Generate headers if needed
		if !options.HasHeader && headers == nil {
			for i := 0; i < len(row); i++ {
				headers = append(headers, fmt.Sprintf("Column_%d", i))
			}
		}

		// Validate row length
		if len(row) != len(headers) {
			return nil, newOpError("ReadCSVFromString",
				fmt.Sprintf("row %d has %d columns, expected %d", rowCount+1, len(row), len(headers)))
		}

		rows = append(rows, row)
		rowCount++

		// Check max rows limit
		if options.MaxRows > 0 && rowCount >= options.MaxRows {
			break
		}
	}

	return buildDataFrameFromRows(headers, rows)
}

// Helper functions

// buildDataFrameFromRows constructs a DataFrame from headers and string data rows
func buildDataFrameFromRows(headers []string, rows [][]string) (*DataFrame, error) {
	if len(headers) == 0 {
		return NewDataFrame(), nil
	}

	if len(rows) == 0 {
		// Create empty DataFrame with columns
		df := NewDataFrame()
		for _, header := range headers {
			series, err := NewSeries(header, []string{})
			if err != nil {
				return nil, wrapColumnError("buildDataFrame", header, err)
			}
			if err := df.addSeriesUnsafe(series); err != nil {
				return nil, err
			}
		}
		return df, nil
	}

	// Transpose data: from rows to columns
	columnData := make([][]string, len(headers))
	for i := range columnData {
		columnData[i] = make([]string, len(rows))
	}

	for rowIdx, row := range rows {
		for colIdx, value := range row {
			columnData[colIdx][rowIdx] = value
		}
	}

	// Infer types and convert data
	var series []*Series
	for i, header := range headers {
		colValues := columnData[i]

		// Infer the best type for this column
		columnType := InferType(colValues)

		// Convert string data to inferred type
		convertedData, err := convertStringSliceToType(colValues, columnType)
		if err != nil {
			return nil, wrapColumnError("buildDataFrame", header, err)
		}

		// Create series
		s, err := NewSeries(header, convertedData)
		if err != nil {
			return nil, wrapColumnError("buildDataFrame", header, err)
		}

		series = append(series, s)
	}

	return NewDataFrameFromSeries(series...)
}

// convertStringSliceToType converts a slice of strings to the specified type
func convertStringSliceToType(values []string, targetType ColumnType) (interface{}, error) {
	switch targetType {
	case StringType:
		// Return a copy to avoid external modification
		result := make([]string, len(values))
		copy(result, values)
		return result, nil

	case Int64Type:
		result := make([]int64, len(values))
		for i, value := range values {
			converted, err := ConvertValue(value, Int64Type)
			if err != nil {
				return nil, err
			}
			result[i] = converted.(int64)
		}
		return result, nil

	case Float64Type:
		result := make([]float64, len(values))
		for i, value := range values {
			converted, err := ConvertValue(value, Float64Type)
			if err != nil {
				return nil, err
			}
			result[i] = converted.(float64)
		}
		return result, nil

	case BoolType:
		result := make([]bool, len(values))
		for i, value := range values {
			converted, err := ConvertValue(value, BoolType)
			if err != nil {
				return nil, err
			}
			result[i] = converted.(bool)
		}
		return result, nil

	case TimeType:
		result := make([]time.Time, len(values))
		for i, value := range values {
			converted, err := ConvertValue(value, TimeType)
			if err != nil {
				return nil, err
			}
			result[i] = converted.(time.Time)
		}
		return result, nil

	default:
		return nil, newOpError("convertStringSliceToType",
			fmt.Sprintf("unsupported target type: %v", targetType))
	}
}

// cleanHeader removes BOM and trims whitespace from column headers
func cleanHeader(header string) string {
	// Remove UTF-8 BOM if present
	if len(header) >= 3 {
		if header[0] == 0xEF && header[1] == 0xBB && header[2] == 0xBF {
			header = header[3:]
		}
	}

	// Trim whitespace and normalize
	header = strings.TrimSpace(header)

	// Replace problematic characters with underscores
	header = strings.ReplaceAll(header, " ", "_")
	header = strings.ReplaceAll(header, "-", "_")

	return header
}

// formatValueForCSV formats a value for CSV output
func formatValueForCSV(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		if v.IsZero() {
			return ""
		}
		return v.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", value)
	}
}

// CSV utility functions for advanced use cases

// DetectDelimiter attempts to detect the delimiter used in a CSV file
func DetectDelimiter(filename string) (rune, error) {
	file, err := os.Open(filename)
	if err != nil {
		return ',', wrapError("DetectDelimiter", err)
	}
	defer file.Close()

	// Read a sample of the file
	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return ',', wrapError("DetectDelimiter", err)
	}

	sample := string(buffer[:n])

	// Count common delimiters
	delimiters := []rune{',', '\t', ';', '|'}
	counts := make(map[rune]int)

	for _, delimiter := range delimiters {
		counts[delimiter] = strings.Count(sample, string(delimiter))
	}

	// Return the most frequent delimiter
	maxCount := 0
	bestDelimiter := ','
	for delimiter, count := range counts {
		if count > maxCount {
			maxCount = count
			bestDelimiter = delimiter
		}
	}

	return bestDelimiter, nil
}

// ValidateCSV checks if a CSV file is valid and returns basic info
func ValidateCSV(filename string) (*CSVInfo, error) {
	delimiter, err := DetectDelimiter(filename)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, wrapError("ValidateCSV", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = delimiter

	info := &CSVInfo{
		Filename:  filename,
		Delimiter: delimiter,
		Rows:      0,
		Columns:   0,
		HasHeader: true, // We'll detect this
	}

	// Read first row (potential header)
	firstRow, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return info, nil // Empty file
		}
		return nil, wrapError("ValidateCSV", err)
	}

	info.Columns = len(firstRow)
	info.Rows = 1

	// Read remaining rows and validate consistency
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, wrapError("ValidateCSV", err)
		}

		if len(row) != info.Columns {
			return nil, newOpError("ValidateCSV",
				fmt.Sprintf("inconsistent column count at row %d: expected %d, got %d",
					info.Rows+1, info.Columns, len(row)))
		}

		info.Rows++
	}

	return info, nil
}

// CSVInfo contains information about a CSV file
type CSVInfo struct {
	Filename  string
	Delimiter rune
	Rows      int
	Columns   int
	HasHeader bool
}
