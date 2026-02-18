package otters

import (
	"testing"
	"time"
)

func TestSeries_Set_AllTypes(t *testing.T) {
	s1, _ := NewSeries("test", []int64{1, 2, 3})
	if err := s1.Set(1, int64(99)); err != nil {
		t.Errorf("Set int64 failed: %v", err)
	}
	v1, _ := s1.GetInt64(1)
	if v1 != 99 {
		t.Error("Set int64 didn't update value")
	}

	s2, _ := NewSeries("test", []float64{1.1, 2.2, 3.3})
	if err := s2.Set(1, 9.9); err != nil {
		t.Errorf("Set float64 failed: %v", err)
	}
	v2, _ := s2.GetFloat64(1)
	if v2 != 9.9 {
		t.Error("Set float64 didn't update value")
	}

	s3, _ := NewSeries("test", []string{"a", "b", "c"})
	if err := s3.Set(1, "z"); err != nil {
		t.Errorf("Set string failed: %v", err)
	}
	v3, _ := s3.GetString(1)
	if v3 != "z" {
		t.Error("Set string didn't update value")
	}

	s4, _ := NewSeries("test", []bool{true, false, true})
	if err := s4.Set(1, true); err != nil {
		t.Errorf("Set bool failed: %v", err)
	}

	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("test", []time.Time{tm, tm, tm})
	newTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s5.Set(1, newTime); err != nil {
		t.Errorf("Set time failed: %v", err)
	}
}

func TestSeries_Set_ErrorCases(t *testing.T) {
	s, _ := NewSeries("test", []int64{1, 2, 3})

	if err := s.Set(-1, int64(99)); err == nil {
		t.Error("Set should error on negative index")
	}
	if err := s.Set(10, int64(99)); err == nil {
		t.Error("Set should error on out of bounds index")
	}
	if err := s.Set(0, "wrong type"); err == nil {
		t.Error("Set should error on type mismatch")
	}

	// Boundary: last valid index
	if err := s.Set(2, int64(99)); err != nil {
		t.Errorf("Set at last index should succeed: %v", err)
	}
}

func TestSeries_Get_ErrorCases(t *testing.T) {
	s, _ := NewSeries("test", []int64{1, 2, 3})
	if _, err := s.Get(-1); err == nil {
		t.Error("Get should error on negative index")
	}
	if _, err := s.Get(10); err == nil {
		t.Error("Get should error on out of bounds index")
	}
}

func TestSeries_GetFloat64_GetString_ErrorCases(t *testing.T) {
	sf, _ := NewSeries("test", []float64{1.1, 2.2})
	if _, err := sf.GetFloat64(10); err == nil {
		t.Error("GetFloat64 should error on out of bounds")
	}
	ss, _ := NewSeries("test", []string{"a", "b"})
	if _, err := ss.GetString(10); err == nil {
		t.Error("GetString should error on out of bounds")
	}
}

func TestSeries_Copy_AllTypes(t *testing.T) {
	s1, _ := NewSeries("test", []int64{1, 2, 3})
	c1 := s1.Copy()
	if c1.Name != s1.Name || c1.Length != s1.Length {
		t.Error("Copy should create identical series")
	}
	s1.Set(0, int64(99))
	v, _ := c1.Get(0)
	if v == int64(99) {
		t.Error("Copy should be independent")
	}

	s2, _ := NewSeries("test", []float64{1.1, 2.2})
	c2 := s2.Copy()
	if c2.Length != 2 {
		t.Error("Copy float64 should preserve length")
	}

	s3, _ := NewSeries("test", []bool{true, false})
	c3 := s3.Copy()
	if c3.Length != 2 {
		t.Error("Copy bool should preserve length")
	}

	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s4, _ := NewSeries("test", []time.Time{tm})
	c4 := s4.Copy()
	if c4.Length != 1 {
		t.Error("Copy time should preserve length")
	}
}

func TestSeries_NewSeries_EmptySlice(t *testing.T) {
	s, err := NewSeries("test", []int64{})
	if err != nil {
		t.Errorf("NewSeries should accept empty slice: %v", err)
	}
	if s.Length != 0 {
		t.Error("NewSeries with empty slice should have length 0")
	}
}

func TestSeries_NewSeries_InvalidType(t *testing.T) {
	if _, err := NewSeries("test", []int{1, 2, 3}); err == nil {
		t.Error("NewSeries should error on invalid type")
	}
}

func TestSeries_Set_TypeMismatch_AllTypes(t *testing.T) {
	// String series — set wrong type
	ss, _ := NewSeries("s", []string{"a", "b"})
	if err := ss.Set(0, int64(1)); err == nil {
		t.Error("Set should error: wrong type for string series")
	}

	// Int64 series — set wrong type
	si, _ := NewSeries("i", []int64{1, 2})
	if err := si.Set(0, "wrong"); err == nil {
		t.Error("Set should error: wrong type for int64 series")
	}

	// Float64 series — set wrong type
	sf, _ := NewSeries("f", []float64{1.1, 2.2})
	if err := sf.Set(0, "wrong"); err == nil {
		t.Error("Set should error: wrong type for float64 series")
	}

	// Bool series — set wrong type
	sb, _ := NewSeries("b", []bool{true, false})
	if err := sb.Set(0, "wrong"); err == nil {
		t.Error("Set should error: wrong type for bool series")
	}

	// Time series — set wrong type
	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	st, _ := NewSeries("t", []time.Time{tm, tm})
	if err := st.Set(0, "wrong"); err == nil {
		t.Error("Set should error: wrong type for time series")
	}

	// Unknown type — should hit default branch
	su := &Series{Name: "u", Type: ColumnType(99), Data: []int64{1, 2}, Length: 2}
	if err := su.Set(0, int64(1)); err == nil {
		t.Error("Set should error: unknown column type")
	}
}
