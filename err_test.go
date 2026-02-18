package otters

import (
	"errors"
	"strings"
	"testing"
)

func TestOtterError_Error(t *testing.T) {
	err := &OtterError{
		Op:      "TestOp",
		Message: "test error",
		Row:     -1,
	}
	got := err.Error()
	if !strings.Contains(got, "TestOp") || !strings.Contains(got, "test error") {
		t.Errorf("Error() = %v, should contain TestOp and test error", got)
	}

	errWithCause := &OtterError{
		Op:      "TestOp",
		Message: "test error",
		Cause:   errors.New("cause error"),
		Row:     -1,
	}
	gotWithCause := errWithCause.Error()
	if !strings.Contains(gotWithCause, "TestOp") {
		t.Errorf("Error() = %v, should contain TestOp", gotWithCause)
	}
}

func TestOtterError_Unwrap(t *testing.T) {
	cause := errors.New("cause error")
	err := &OtterError{
		Op:      "TestOp",
		Message: "test error",
		Cause:   cause,
	}
	if got := err.Unwrap(); got != cause {
		t.Errorf("Unwrap() = %v, want %v", got, cause)
	}
}

func TestOtterError_Is(t *testing.T) {
	err1 := &OtterError{Op: "Op1", Message: "msg1"}
	err2 := &OtterError{Op: "Op1", Message: "msg1"}
	err3 := &OtterError{Op: "Op2", Message: "msg2"}

	if !err1.Is(err2) {
		t.Error("Is() should return true for matching errors")
	}
	if err1.Is(err3) {
		t.Error("Is() should return false for different errors")
	}
	if err1.Is(errors.New("other")) {
		t.Error("Is() should return false for non-OtterError")
	}
}

func TestNewOpError(t *testing.T) {
	err := newOpError("TestOp", "test message")
	if err.Op != "TestOp" || err.Message != "test message" {
		t.Errorf("newOpError() = %v, want Op=TestOp, Message=test message", err)
	}
}

func TestNewRowError(t *testing.T) {
	err := newRowError("TestOp", 5, "test message")
	if err.Op != "TestOp" || err.Row != 5 || err.Message != "test message" {
		t.Errorf("newRowError() failed")
	}
}

func TestNewCellError(t *testing.T) {
	err := newCellError("TestOp", "col1", 5, "test message")
	if err.Op != "TestOp" || err.Row != 5 || err.Column != "col1" {
		t.Errorf("newCellError() failed")
	}
}

func TestWrapColumnError(t *testing.T) {
	cause := errors.New("cause")
	err := wrapColumnError("TestOp", "col1", cause)
	if err.Op != "TestOp" || err.Column != "col1" || err.Cause != cause {
		t.Errorf("wrapColumnError() failed")
	}
}

func TestIsColumnNotFound(t *testing.T) {
	err := &OtterError{
		Op:      "TestOp",
		Column:  "col1",
		Message: "column not found",
		Row:     -1,
	}
	if !isColumnNotFound(err) {
		t.Error("isColumnNotFound() should return true")
	}
	if isColumnNotFound(errors.New("other")) {
		t.Error("isColumnNotFound() should return false for non-OtterError")
	}
}

func TestIsIndexOutOfRange(t *testing.T) {
	err := &OtterError{
		Op:      "TestOp",
		Row:     5,
		Message: "index out of range",
	}
	if !isIndexOutOfRange(err) {
		t.Error("isIndexOutOfRange() should return true")
	}
	if isIndexOutOfRange(errors.New("other")) {
		t.Error("isIndexOutOfRange() should return false for non-OtterError")
	}
}

func TestIsTypeMismatch(t *testing.T) {
	err := newOpError("TestOp", "type mismatch")
	if !isTypeMismatch(err) {
		t.Error("isTypeMismatch() should return true")
	}
	if isTypeMismatch(errors.New("other")) {
		t.Error("isTypeMismatch() should return false for non-OtterError")
	}
}

func TestDataFrame_ErrorMethods(t *testing.T) {
	df := NewDataFrame()

	if df.err != nil {
		t.Error("err should be nil for new DataFrame")
	}

	testErr := errors.New("test error")
	df.err = testErr

	if df.err == nil {
		t.Error("err should be set after assignment")
	}

	if df.Error() != testErr {
		t.Error("Error() should return the set error")
	}

	df.err = nil
	if df.err != nil {
		t.Error("err should be nil after clearing")
	}
}

func TestRecoverFromPanic(t *testing.T) {
	// Test that recoverFromPanic works when there's no panic
	err := recoverFromPanic("TestOp")
	if err != nil {
		t.Error("recoverFromPanic should return nil when no panic")
	}
}

func TestSafeOperation(t *testing.T) {
	err := SafeOperation("TestOp", func() error {
		return nil
	})
	if err != nil {
		t.Error("SafeOperation should return success")
	}

	err = SafeOperation("TestOp", func() error {
		return errors.New("test error")
	})
	if err == nil {
		t.Error("SafeOperation should return error")
	}
}
