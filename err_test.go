package otters

import (
	"errors"
	"fmt"
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

func TestWrapColumnError(t *testing.T) {
	cause := errors.New("cause")
	err := wrapColumnError("TestOp", "col1", cause)
	if err.Op != "TestOp" || err.Column != "col1" || err.Cause != cause {
		t.Errorf("wrapColumnError() failed")
	}
}

// TestSentinelErrorsMatchWithErrorsIs verifies that errors produced by the
// library match the exported sentinel errors under errors.Is.
func TestSentinelErrorsMatchWithErrorsIs(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]any{
		"a": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := df.Get(0, "missing"); !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("missing column error should match ErrColumnNotFound, got %v", err)
	}

	if _, err := df.Get(99, "a"); !errors.Is(err, ErrIndexOutOfRange) {
		t.Errorf("out-of-range error should match ErrIndexOutOfRange, got %v", err)
	}

	empty := NewDataFrame()
	if _, err := empty.Mean("a"); !errors.Is(err, ErrEmptyDataFrame) && !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("empty DataFrame error should match a sentinel, got %v", err)
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

// TestErrorHandling covers error propagation through chained operations.
func TestErrorHandling(t *testing.T) {
	df := NewDataFrame()

	// Test operations on empty DataFrame
	result := df.Filter("nonexistent", "==", "value")
	if result.Error() == nil {
		t.Error("Expected error when filtering nonexistent column")
	}

	// Test chaining with errors
	chained := df.Filter("bad", "==", 1).Sort("bad", true).Head(5)
	if chained.Error() == nil {
		t.Error("Expected error to propagate through chain")
	}
}

// TestSentinelErrors covers sentinel errors, SafeOperation, and MustOperation.
func TestSentinelErrors(t *testing.T) {
	// Each sentinel error must be non-nil
	sentinels := map[string]error{
		"ErrColumnNotFound":   ErrColumnNotFound,
		"ErrIndexOutOfRange":  ErrIndexOutOfRange,
		"ErrTypeMismatch":     ErrTypeMismatch,
		"ErrEmptyDataFrame":   ErrEmptyDataFrame,
		"ErrInvalidOperation": ErrInvalidOperation,
	}
	for name, sentinel := range sentinels {
		if sentinel == nil {
			t.Errorf("%s is nil", name)
		}
	}

	// SafeOperation: wraps error returned by function
	err := SafeOperation("test", func() error {
		return fmt.Errorf("something went wrong")
	})
	if err == nil {
		t.Error("SafeOperation: expected error from function, got nil")
	}

	// SafeOperation: successful function returns nil
	err = SafeOperation("test", func() error {
		return nil
	})
	if err != nil {
		t.Errorf("SafeOperation: expected nil error for success, got %v", err)
	}

	// MustOperation: must panic on error
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustOperation: expected panic on error, got none")
			}
		}()
		MustOperation("test", func() error {
			return fmt.Errorf("forced error")
		})
	}()

	// MustOperation: must not panic on success
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("MustOperation: unexpected panic on success: %v", r)
			}
		}()
		MustOperation("test", func() error {
			return nil
		})
	}()
}
