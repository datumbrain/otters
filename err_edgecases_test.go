package otters

import (
	"testing"
)

func TestErr_OtterError_AllBranches(t *testing.T) {
	// With row and column
	err1 := &OtterError{Op: "TestOp", Column: "col1", Row: 5, Message: "test error"}
	if err1.Error() == "" {
		t.Error("Error() should return message with row and column")
	}

	// With row, no column
	err2 := &OtterError{Op: "TestOp", Row: 5, Message: "test error"}
	if err2.Error() == "" {
		t.Error("Error() should return message with row only")
	}

	// With column, no row (row = -1)
	err3 := &OtterError{Op: "TestOp", Column: "col1", Row: -1, Message: "test error"}
	if err3.Error() == "" {
		t.Error("Error() should return message with column only")
	}

	// No row, no column
	err4 := &OtterError{Op: "TestOp", Row: -1, Message: "test error"}
	if err4.Error() == "" {
		t.Error("Error() should return message with no row/column")
	}

	// With cause
	err5 := &OtterError{
		Op:      "TestOp",
		Message: "test error",
		Cause:   &OtterError{Op: "Cause", Message: "cause error"},
		Row:     -1,
	}
	if err5.Error() == "" {
		t.Error("Error() should return message with cause")
	}
}

func TestErr_SafeOperation_SuccessAndError(t *testing.T) {
	err1 := SafeOperation("TestOp", func() error {
		return nil
	})
	if err1 != nil {
		t.Error("SafeOperation should succeed")
	}

	err2 := SafeOperation("TestOp", func() error {
		return newOpError("TestOp", "test error")
	})
	if err2 == nil {
		t.Error("SafeOperation should return error")
	}
}

func TestErr_MustOperation_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustOperation should panic on error")
		}
	}()
	MustOperation("TestOp", func() error {
		return newOpError("test", "error")
	})
}

func TestErr_RecoverFromPanic_NoPanic(t *testing.T) {
	err := recoverFromPanic("TestOp")
	if err != nil {
		t.Error("recoverFromPanic should return nil when no panic")
	}
}
