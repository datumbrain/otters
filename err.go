package otters

import (
	"fmt"
)

// OtterError represents an error that occurred during DataFrame operations
type OtterError struct {
	Op      string // Operation that caused the error
	Column  string // Column name (if applicable)
	Row     int    // Row number (if applicable, -1 if not applicable)
	Message string // Human-readable error message
	Cause   error  // Underlying error (if any)
}

// Error implements the error interface
func (e *OtterError) Error() string {
	if e.Column != "" && e.Row >= 0 {
		return fmt.Sprintf("otters.%s: %s (column: %s, row: %d)", e.Op, e.Message, e.Column, e.Row)
	} else if e.Column != "" {
		return fmt.Sprintf("otters.%s: %s (column: %s)", e.Op, e.Message, e.Column)
	} else if e.Row >= 0 {
		return fmt.Sprintf("otters.%s: %s (row: %d)", e.Op, e.Message, e.Row)
	}
	return fmt.Sprintf("otters.%s: %s", e.Op, e.Message)
}

// Unwrap returns the underlying error (for Go 1.13+ error wrapping)
func (e *OtterError) Unwrap() error {
	return e.Cause
}

// Is checks if the error matches a target error (for Go 1.13+ error handling)
func (e *OtterError) Is(target error) bool {
	if otherErr, ok := target.(*OtterError); ok {
		return e.Op == otherErr.Op && e.Message == otherErr.Message
	}
	return false
}

// Error constructors for common scenarios

// newOpError creates a new error for a failed operation
func newOpError(op, message string) *OtterError {
	return &OtterError{
		Op:      op,
		Message: message,
		Row:     -1,
	}
}

// newColumnError creates a new error for a column-related operation
func newColumnError(op, column, message string) *OtterError {
	return &OtterError{
		Op:      op,
		Column:  column,
		Message: message,
		Row:     -1,
	}
}

// newRowError creates a new error for a row-related operation
func newRowError(op string, row int, message string) *OtterError {
	return &OtterError{
		Op:      op,
		Row:     row,
		Message: message,
	}
}

// newCellError creates a new error for a specific cell operation
func newCellError(op, column string, row int, message string) *OtterError {
	return &OtterError{
		Op:      op,
		Column:  column,
		Row:     row,
		Message: message,
	}
}

// wrapError wraps an existing error with operation context
func wrapError(op string, cause error) *OtterError {
	return &OtterError{
		Op:      op,
		Message: cause.Error(),
		Cause:   cause,
		Row:     -1,
	}
}

// wrapColumnError wraps an error with column context
func wrapColumnError(op, column string, cause error) *OtterError {
	return &OtterError{
		Op:      op,
		Column:  column,
		Message: cause.Error(),
		Cause:   cause,
		Row:     -1,
	}
}

// Common error types for better error handling

// ErrColumnNotFound is returned when a requested column doesn't exist
var ErrColumnNotFound = &OtterError{
	Op:      "ColumnAccess",
	Message: "column not found",
	Row:     -1,
}

// ErrIndexOutOfRange is returned when accessing an invalid row index
var ErrIndexOutOfRange = &OtterError{
	Op:      "IndexAccess",
	Message: "index out of range",
	Row:     -1,
}

// ErrTypeMismatch is returned when there's a type conversion error
var ErrTypeMismatch = &OtterError{
	Op:      "TypeConversion",
	Message: "type mismatch",
	Row:     -1,
}

// ErrEmptyDataFrame is returned when operating on an empty DataFrame
var ErrEmptyDataFrame = &OtterError{
	Op:      "Operation",
	Message: "cannot operate on empty DataFrame",
	Row:     -1,
}

// ErrInvalidOperation is returned for invalid operations
var ErrInvalidOperation = &OtterError{
	Op:      "Operation",
	Message: "invalid operation",
	Row:     -1,
}

// Helper functions for common error scenarios

// isColumnNotFound checks if an error is a "column not found" error
func isColumnNotFound(err error) bool {
	if otterErr, ok := err.(*OtterError); ok {
		return otterErr.Op == "ColumnAccess" || otterErr.Message == "column not found"
	}
	return false
}

// isIndexOutOfRange checks if an error is an "index out of range" error
func isIndexOutOfRange(err error) bool {
	if otterErr, ok := err.(*OtterError); ok {
		return otterErr.Op == "IndexAccess" || otterErr.Message == "index out of range"
	}
	return false
}

// isTypeMismatch checks if an error is a type mismatch error
func isTypeMismatch(err error) bool {
	if otterErr, ok := err.(*OtterError); ok {
		return otterErr.Op == "TypeConversion" || otterErr.Message == "type mismatch"
	}
	return false
}

// validateColumnExists checks if a column exists in the DataFrame
func (df *DataFrame) validateColumnExists(columnName string) error {
	if df.err != nil {
		return df.err
	}

	if _, exists := df.columns[columnName]; !exists {
		return newColumnError("ColumnAccess", columnName, "column does not exist")
	}
	return nil
}

// validateRowIndex checks if a row index is valid
func (df *DataFrame) validateRowIndex(index int) error {
	if df.err != nil {
		return df.err
	}

	if index < 0 || index >= df.length {
		return newRowError("IndexAccess", index, fmt.Sprintf("index %d out of range [0:%d]", index, df.length))
	}
	return nil
}

// validateNotEmpty checks if the DataFrame is not empty
func (df *DataFrame) validateNotEmpty() error {
	if df.err != nil {
		return df.err
	}

	if df.length == 0 {
		return newOpError("Operation", "cannot operate on empty DataFrame")
	}
	return nil
}

// validateColumnsExist checks if all specified columns exist
func (df *DataFrame) validateColumnsExist(columns []string) error {
	if df.err != nil {
		return df.err
	}

	for _, col := range columns {
		if err := df.validateColumnExists(col); err != nil {
			return err
		}
	}
	return nil
}

// validateSameLength checks if all series have the same length
func validateSameLength(series []*Series) error {
	if len(series) == 0 {
		return nil
	}

	expectedLength := series[0].Length
	for i, s := range series {
		if s.Length != expectedLength {
			return newOpError("DataValidation",
				fmt.Sprintf("series %d has length %d, expected %d", i, s.Length, expectedLength))
		}
	}
	return nil
}

// setError returns a new DataFrame carrying the error, leaving the receiver untouched.
func (df *DataFrame) setError(err error) *DataFrame {
	newDf := NewDataFrame()
	newDf.err = err
	return newDf
}

// clearError clears the error state (used internally)
func (df *DataFrame) clearError() {
	df.err = nil
}

// hasError checks if the DataFrame has an error state
func (df *DataFrame) hasError() bool {
	return df.err != nil
}

// Error returns the current error state of the DataFrame
func (df *DataFrame) Error() error {
	return df.err
}

// recoverFromPanic recovers from panics and converts them to OtterErrors
func recoverFromPanic(op string) error {
	if r := recover(); r != nil {
		switch v := r.(type) {
		case error:
			return wrapError(op, v)
		case string:
			return newOpError(op, v)
		default:
			return newOpError(op, fmt.Sprintf("panic: %v", r))
		}
	}
	return nil
}

// SafeOperation wraps a function to handle panics and convert them to errors
func SafeOperation(op string, fn func() error) (err error) {
	defer func() {
		if panicErr := recoverFromPanic(op); panicErr != nil {
			err = panicErr
		}
	}()
	return fn()
}

// MustOperation executes an operation and panics if it fails (for testing/debugging)
func MustOperation(op string, fn func() error) {
	if err := fn(); err != nil {
		panic(fmt.Sprintf("otters.%s failed: %v", op, err))
	}
}
