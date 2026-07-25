package otters

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// --- test helpers ---

// dfFromSeries builds a DataFrame from (name, data) pairs preserving column order.
func dfFromSeries(t *testing.T, pairs ...any) *DataFrame {
	t.Helper()
	if len(pairs)%2 != 0 {
		t.Fatal("dfFromSeries: pairs must be name, data alternating")
	}
	var series []*Series
	for i := 0; i < len(pairs); i += 2 {
		name, ok := pairs[i].(string)
		if !ok {
			t.Fatalf("dfFromSeries: pair %d name is %T, want string", i, pairs[i])
		}
		s, err := NewSeries(name, pairs[i+1])
		if err != nil {
			t.Fatalf("dfFromSeries: NewSeries(%s): %v", name, err)
		}
		series = append(series, s)
	}
	df, err := NewDataFrameFromSeries(series...)
	if err != nil {
		t.Fatalf("dfFromSeries: %v", err)
	}
	return df
}

// checkColumn asserts a column's full contents match want (a typed slice).
func checkColumn(t *testing.T, df *DataFrame, col string, want any) {
	t.Helper()
	if df.Error() != nil {
		t.Fatalf("checkColumn(%s): DataFrame has error: %v", col, df.Error())
	}
	s, err := df.GetSeries(col)
	if err != nil {
		t.Fatalf("checkColumn(%s): %v", col, err)
	}
	if !reflect.DeepEqual(s.Data, want) {
		t.Errorf("column %s = %#v, want %#v", col, s.Data, want)
	}
}

// checkColumns asserts the exact column names and order.
func checkColumns(t *testing.T, df *DataFrame, want ...string) {
	t.Helper()
	if df.Error() != nil {
		t.Fatalf("checkColumns: DataFrame has error: %v", df.Error())
	}
	got := df.Columns()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("columns = %v, want %v", got, want)
	}
}

// ordersCustomers returns the canonical left/right test frames.
// left "orders": order_id, customer_id (30 unmatched, 0 null), amount
// right "customers": customer_id (40 unmatched, 0 null), name
func ordersCustomers(t *testing.T) (*DataFrame, *DataFrame) {
	t.Helper()
	left := dfFromSeries(t,
		"order_id", []int64{1, 2, 3, 4, 5},
		"customer_id", []int64{10, 20, 30, 10, 0},
		"amount", []float64{100, 200, 300, 400, 500},
	)
	right := dfFromSeries(t,
		"customer_id", []int64{10, 20, 40, 0},
		"name", []string{"Alice", "Bob", "Carol", "NullCo"},
	)
	return left, right
}

// --- JoinType ---

func TestJoinTypeString(t *testing.T) {
	cases := map[JoinType]string{
		InnerJoin:     "inner",
		LeftJoin:      "left",
		RightJoin:     "right",
		OuterJoin:     "outer",
		JoinType(999): "unknown",
	}
	for jt, want := range cases {
		if got := jt.String(); got != want {
			t.Errorf("JoinType(%d).String() = %q, want %q", int(jt), got, want)
		}
	}
}

// --- basic join semantics ---

func TestInnerJoinBasic(t *testing.T) {
	left, right := ordersCustomers(t)
	res := left.Join(right, []string{"customer_id"}, InnerJoin)
	if res.Error() != nil {
		t.Fatalf("inner join error: %v", res.Error())
	}

	// Left rows in order: 1(10→Alice), 2(20→Bob), 3(30 no match), 4(10→Alice), 5(0 null key skipped)
	checkColumns(t, res, "order_id", "customer_id", "amount", "name")
	checkColumn(t, res, "order_id", []int64{1, 2, 4})
	checkColumn(t, res, "customer_id", []int64{10, 20, 10})
	checkColumn(t, res, "amount", []float64{100, 200, 400})
	checkColumn(t, res, "name", []string{"Alice", "Bob", "Alice"})
}

func TestLeftJoinBasic(t *testing.T) {
	left, right := ordersCustomers(t)
	res := left.Join(right, []string{"customer_id"}, LeftJoin)
	if res.Error() != nil {
		t.Fatalf("left join error: %v", res.Error())
	}

	// All 5 left rows preserved in order; unmatched (30) and null-key (0) rows zero-filled on right.
	checkColumn(t, res, "order_id", []int64{1, 2, 3, 4, 5})
	checkColumn(t, res, "customer_id", []int64{10, 20, 30, 10, 0})
	checkColumn(t, res, "amount", []float64{100, 200, 300, 400, 500})
	checkColumn(t, res, "name", []string{"Alice", "Bob", "", "Alice", ""})
}

func TestRightJoinBasic(t *testing.T) {
	left, right := ordersCustomers(t)
	res := left.Join(right, []string{"customer_id"}, RightJoin)
	if res.Error() != nil {
		t.Fatalf("right join error: %v", res.Error())
	}

	// Matched pairs in left-row order, then unmatched right rows (40, null 0) in right order.
	checkColumn(t, res, "order_id", []int64{1, 2, 4, 0, 0})
	checkColumn(t, res, "customer_id", []int64{10, 20, 10, 40, 0})
	checkColumn(t, res, "amount", []float64{100, 200, 400, 0, 0})
	checkColumn(t, res, "name", []string{"Alice", "Bob", "Alice", "Carol", "NullCo"})
}

func TestOuterJoinBasic(t *testing.T) {
	left, right := ordersCustomers(t)
	res := left.Join(right, []string{"customer_id"}, OuterJoin)
	if res.Error() != nil {
		t.Fatalf("outer join error: %v", res.Error())
	}

	// All left rows in order (incl. unmatched 30 and null 0), then unmatched right rows.
	// Null keys never match: left row with key 0 and right row with key 0 stay separate.
	checkColumn(t, res, "order_id", []int64{1, 2, 3, 4, 5, 0, 0})
	checkColumn(t, res, "customer_id", []int64{10, 20, 30, 10, 0, 40, 0})
	checkColumn(t, res, "amount", []float64{100, 200, 300, 400, 500, 0, 0})
	checkColumn(t, res, "name", []string{"Alice", "Bob", "", "Alice", "", "Carol", "NullCo"})
}

// --- duplicate keys / row multiplication ---

func TestJoinDuplicateRightKeys(t *testing.T) {
	left := dfFromSeries(t,
		"id", []int64{1, 2},
		"l", []string{"L1", "L2"},
	)
	right := dfFromSeries(t,
		"id", []int64{1, 1, 3},
		"r", []string{"a", "b", "c"},
	)
	res := left.Join(right, []string{"id"}, InnerJoin)
	// Left row 1 matches two right rows, emitted in right-frame order.
	checkColumn(t, res, "id", []int64{1, 1})
	checkColumn(t, res, "l", []string{"L1", "L1"})
	checkColumn(t, res, "r", []string{"a", "b"})
}

func TestJoinDuplicateBothSides(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{7, 7}, "l", []string{"x", "y"})
	right := dfFromSeries(t, "id", []int64{7, 7}, "r", []string{"p", "q"})
	res := left.Join(right, []string{"id"}, InnerJoin)
	// Cartesian per key: 4 rows, left-major order.
	checkColumn(t, res, "l", []string{"x", "x", "y", "y"})
	checkColumn(t, res, "r", []string{"p", "q", "p", "q"})
}

// --- composite keys ---

func TestJoinCompositeKeys(t *testing.T) {
	left := dfFromSeries(t,
		"region", []string{"East", "East", "West", "West"},
		"year", []int64{2024, 2025, 2024, 2025},
		"sales", []float64{10, 20, 30, 40},
	)
	right := dfFromSeries(t,
		"region", []string{"East", "West", "East"},
		"year", []int64{2024, 2025, 2030},
		"target", []float64{11, 44, 99},
	)
	res := left.Join(right, []string{"region", "year"}, InnerJoin)
	checkColumns(t, res, "region", "year", "sales", "target")
	checkColumn(t, res, "region", []string{"East", "West"})
	checkColumn(t, res, "year", []int64{2024, 2025})
	checkColumn(t, res, "sales", []float64{10, 40})
	checkColumn(t, res, "target", []float64{11, 44})
}

func TestJoinCompositeKeysNoCrossTupleConfusion(t *testing.T) {
	// Keys ("ab","c") and ("a","bc") must not collide in the tuple encoding.
	left := dfFromSeries(t, "k1", []string{"ab"}, "k2", []string{"c"}, "v", []int64{1})
	right := dfFromSeries(t, "k1", []string{"a"}, "k2", []string{"bc"}, "w", []int64{2})
	res := left.Join(right, []string{"k1", "k2"}, InnerJoin)
	if res.Error() != nil {
		t.Fatalf("join error: %v", res.Error())
	}
	if res.Len() != 0 {
		t.Errorf("expected 0 rows, got %d — tuple keys collided", res.Len())
	}
}

func TestJoinCompositeNullComponent(t *testing.T) {
	// A key tuple with any null (zero-value) component never matches.
	left := dfFromSeries(t, "a", []string{"x", ""}, "b", []int64{1, 2}, "v", []int64{10, 20})
	right := dfFromSeries(t, "a", []string{"x", ""}, "b", []int64{1, 2}, "w", []int64{30, 40})
	res := left.Join(right, []string{"a", "b"}, InnerJoin)
	checkColumn(t, res, "v", []int64{10})
	checkColumn(t, res, "w", []int64{30})
}

// --- JoinOn with differently-named keys ---

func TestJoinOnDifferentKeyNames(t *testing.T) {
	left := dfFromSeries(t,
		"cust", []int64{10, 20, 30},
		"amount", []float64{1, 2, 3},
	)
	right := dfFromSeries(t,
		"id", []int64{10, 30},
		"name", []string{"Alice", "Carl"},
	)
	res := left.JoinOn(right, []string{"cust"}, []string{"id"}, InnerJoin)
	// Key column appears once, under the left name.
	checkColumns(t, res, "cust", "amount", "name")
	checkColumn(t, res, "cust", []int64{10, 30})
	checkColumn(t, res, "name", []string{"Alice", "Carl"})
	if res.HasColumn("id") {
		t.Error("right key column should not appear in output")
	}
}

func TestJoinOnRightOuterKeyValuesComeFromRight(t *testing.T) {
	left := dfFromSeries(t, "cust", []int64{10}, "amount", []float64{1})
	right := dfFromSeries(t, "id", []int64{10, 55}, "name", []string{"Alice", "Eve"})
	res := left.JoinOn(right, []string{"cust"}, []string{"id"}, RightJoin)
	// Unmatched right row contributes its key value into the left-named key column.
	checkColumn(t, res, "cust", []int64{10, 55})
	checkColumn(t, res, "amount", []float64{1, 0})
	checkColumn(t, res, "name", []string{"Alice", "Eve"})
}

// --- column name collisions ---

func TestJoinNonKeyCollisionSuffixes(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{1, 2}, "amount", []float64{10, 20})
	right := dfFromSeries(t, "id", []int64{1, 2}, "amount", []float64{100, 200})
	res := left.Join(right, []string{"id"}, InnerJoin)
	checkColumns(t, res, "id", "amount_left", "amount_right")
	checkColumn(t, res, "id", []int64{1, 2})
	checkColumn(t, res, "amount_left", []float64{10, 20})
	checkColumn(t, res, "amount_right", []float64{100, 200})
}

func TestJoinRightNonKeyCollidesWithLeftKeyName(t *testing.T) {
	// Right has a non-key column named like the left key: key keeps its name,
	// only the right column is suffixed.
	left := dfFromSeries(t, "id", []int64{1}, "v", []int64{9})
	right := dfFromSeries(t, "rid", []int64{1}, "id", []int64{777})
	res := left.JoinOn(right, []string{"id"}, []string{"rid"}, InnerJoin)
	checkColumns(t, res, "id", "v", "id_right")
	checkColumn(t, res, "id", []int64{1})
	checkColumn(t, res, "id_right", []int64{777})
}

func TestJoinSuffixCollisionErrors(t *testing.T) {
	// Suffixing "amount" would collide with a pre-existing "amount_left".
	left := dfFromSeries(t, "id", []int64{1}, "amount", []float64{1}, "amount_left", []float64{2})
	right := dfFromSeries(t, "id", []int64{1}, "amount", []float64{3})
	res := left.Join(right, []string{"id"}, InnerJoin)
	if res.Error() == nil {
		t.Fatal("expected error on suffix collision, got none")
	}
}

func TestJoinSelfJoin(t *testing.T) {
	df := dfFromSeries(t, "id", []int64{1, 2}, "v", []int64{10, 20})
	res := df.Join(df, []string{"id"}, InnerJoin)
	checkColumns(t, res, "id", "v_left", "v_right")
	checkColumn(t, res, "v_left", []int64{10, 20})
	checkColumn(t, res, "v_right", []int64{10, 20})
}

// --- key type handling ---

func TestJoinMixedNumericKeysCoerceToFloat64(t *testing.T) {
	left := dfFromSeries(t, "k", []int64{1, 2, 3}, "l", []string{"a", "b", "c"})
	right := dfFromSeries(t, "k", []float64{1.0, 2.5, 3.0}, "r", []string{"x", "y", "z"})
	res := left.Join(right, []string{"k"}, InnerJoin)
	if res.Error() != nil {
		t.Fatalf("mixed numeric join error: %v", res.Error())
	}
	// int64 1 and 3 match float64 1.0 and 3.0; 2 vs 2.5 does not.
	// Output key column is promoted to Float64Type.
	ct, err := res.GetColumnType("k")
	if err != nil {
		t.Fatal(err)
	}
	if ct != Float64Type {
		t.Errorf("mixed numeric key column type = %v, want Float64Type", ct)
	}
	checkColumn(t, res, "k", []float64{1, 3})
	checkColumn(t, res, "l", []string{"a", "c"})
	checkColumn(t, res, "r", []string{"x", "z"})
}

func TestJoinMixedNumericOuterFillFromRight(t *testing.T) {
	left := dfFromSeries(t, "k", []int64{1}, "l", []string{"a"})
	right := dfFromSeries(t, "k", []float64{2.5}, "r", []string{"y"})
	res := left.Join(right, []string{"k"}, OuterJoin)
	checkColumn(t, res, "k", []float64{1, 2.5})
	checkColumn(t, res, "l", []string{"a", ""})
	checkColumn(t, res, "r", []string{"", "y"})
}

func TestJoinIncompatibleKeyTypesError(t *testing.T) {
	left := dfFromSeries(t, "k", []int64{1}, "l", []string{"a"})
	right := dfFromSeries(t, "k", []string{"1"}, "r", []string{"x"})
	res := left.Join(right, []string{"k"}, InnerJoin)
	if res.Error() == nil {
		t.Fatal("expected key type mismatch error, got none")
	}
}

func TestJoinStringKeys(t *testing.T) {
	left := dfFromSeries(t, "name", []string{"a", "b", ""}, "v", []int64{1, 2, 3})
	right := dfFromSeries(t, "name", []string{"b", "", "a"}, "w", []int64{20, 30, 10})
	res := left.Join(right, []string{"name"}, InnerJoin)
	// "" is the string null sentinel: never matches.
	checkColumn(t, res, "name", []string{"a", "b"})
	checkColumn(t, res, "v", []int64{1, 2})
	checkColumn(t, res, "w", []int64{10, 20})
}

func TestJoinTimeKeys(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	var zero time.Time
	left := dfFromSeries(t, "ts", []time.Time{t1, t2, zero}, "v", []int64{1, 2, 3})
	right := dfFromSeries(t, "ts", []time.Time{t2, zero}, "w", []int64{20, 30})
	res := left.Join(right, []string{"ts"}, InnerJoin)
	// Zero time is the null sentinel: never matches.
	checkColumn(t, res, "v", []int64{2})
	checkColumn(t, res, "w", []int64{20})
}

func TestJoinTimeKeysDifferentLocationsSameInstant(t *testing.T) {
	utc := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	est := utc.In(time.FixedZone("EST", -5*3600))
	left := dfFromSeries(t, "ts", []time.Time{utc}, "v", []int64{1})
	right := dfFromSeries(t, "ts", []time.Time{est}, "w", []int64{2})
	res := left.Join(right, []string{"ts"}, InnerJoin)
	// Same instant in different zones must match (time.Time.Equal semantics).
	if res.Len() != 1 {
		t.Errorf("same-instant times in different zones should join, got %d rows", res.Len())
	}
}

func TestJoinFloat64Keys(t *testing.T) {
	left := dfFromSeries(t, "k", []float64{1.5, 2.5, 0}, "v", []int64{1, 2, 3})
	right := dfFromSeries(t, "k", []float64{2.5, 0}, "w", []int64{20, 30})
	res := left.Join(right, []string{"k"}, InnerJoin)
	// 0.0 is the float null sentinel: never matches.
	checkColumn(t, res, "v", []int64{2})
	checkColumn(t, res, "w", []int64{20})
}

func TestJoinBoolKeys(t *testing.T) {
	// Consistent with the zero-value-is-null rule, false keys never match.
	left := dfFromSeries(t, "k", []bool{true, false}, "v", []int64{1, 2})
	right := dfFromSeries(t, "k", []bool{true, false}, "w", []int64{10, 20})
	res := left.Join(right, []string{"k"}, InnerJoin)
	checkColumn(t, res, "v", []int64{1})
	checkColumn(t, res, "w", []int64{10})
}

// --- empty frames ---

func TestJoinEmptyRight(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{1, 2}, "v", []int64{10, 20})
	right := dfFromSeries(t, "id", []int64{}, "w", []string{})

	inner := left.Join(right, []string{"id"}, InnerJoin)
	if inner.Error() != nil {
		t.Fatalf("inner join with empty right: %v", inner.Error())
	}
	if inner.Len() != 0 {
		t.Errorf("inner join with empty right should have 0 rows, got %d", inner.Len())
	}
	checkColumns(t, inner, "id", "v", "w")

	lj := left.Join(right, []string{"id"}, LeftJoin)
	checkColumn(t, lj, "id", []int64{1, 2})
	checkColumn(t, lj, "v", []int64{10, 20})
	checkColumn(t, lj, "w", []string{"", ""})
}

func TestJoinEmptyLeft(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{}, "v", []int64{})
	right := dfFromSeries(t, "id", []int64{1}, "w", []string{"x"})

	rj := left.Join(right, []string{"id"}, RightJoin)
	checkColumn(t, rj, "id", []int64{1})
	checkColumn(t, rj, "v", []int64{0})
	checkColumn(t, rj, "w", []string{"x"})

	inner := left.Join(right, []string{"id"}, InnerJoin)
	if inner.Len() != 0 {
		t.Errorf("inner join with empty left should have 0 rows, got %d", inner.Len())
	}
}

func TestJoinBothEmpty(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{}, "v", []int64{})
	right := dfFromSeries(t, "id", []int64{}, "w", []int64{})
	res := left.Join(right, []string{"id"}, OuterJoin)
	if res.Error() != nil {
		t.Fatalf("outer join of empty frames: %v", res.Error())
	}
	if res.Len() != 0 {
		t.Errorf("expected 0 rows, got %d", res.Len())
	}
	checkColumns(t, res, "id", "v", "w")
}

// --- error handling ---

func TestJoinValidationErrors(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{1}, "v", []int64{2})
	right := dfFromSeries(t, "id", []int64{1}, "w", []int64{3})

	cases := []struct {
		name string
		run  func() *DataFrame
	}{
		{"nil other", func() *DataFrame { return left.Join(nil, []string{"id"}, InnerJoin) }},
		{"no keys", func() *DataFrame { return left.Join(right, []string{}, InnerJoin) }},
		{"nil keys", func() *DataFrame { return left.Join(right, nil, InnerJoin) }},
		{"missing left column", func() *DataFrame { return left.Join(right, []string{"nope"}, InnerJoin) }},
		{"missing right column", func() *DataFrame { return left.JoinOn(right, []string{"id"}, []string{"nope"}, InnerJoin) }},
		{"key count mismatch", func() *DataFrame { return left.JoinOn(right, []string{"id"}, []string{"id", "w"}, InnerJoin) }},
		{"duplicate left key", func() *DataFrame { return left.JoinOn(right, []string{"id", "id"}, []string{"id", "w"}, InnerJoin) }},
		{"duplicate right key", func() *DataFrame {
			l := dfFromSeries(t, "a", []int64{1}, "b", []int64{2})
			return l.JoinOn(right, []string{"a", "b"}, []string{"id", "id"}, InnerJoin)
		}},
		{"invalid join type", func() *DataFrame { return left.Join(right, []string{"id"}, JoinType(42)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.run()
			if res.Error() == nil {
				t.Fatalf("%s: expected error, got none", tc.name)
			}
		})
	}
}

func TestJoinMissingColumnErrorIsColumnNotFound(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{1})
	right := dfFromSeries(t, "id", []int64{1})
	res := left.Join(right, []string{"ghost"}, InnerJoin)
	if !errors.Is(res.Error(), ErrColumnNotFound) {
		t.Errorf("missing key column error should match ErrColumnNotFound, got %v", res.Error())
	}
}

func TestJoinErrorPropagation(t *testing.T) {
	good := dfFromSeries(t, "id", []int64{1})
	bad := good.Filter("ghost", "==", 1) // carries an error

	res := bad.Join(good, []string{"id"}, InnerJoin)
	if res.Error() == nil {
		t.Fatal("join on errored left frame should carry the error")
	}

	res2 := good.Join(bad, []string{"id"}, InnerJoin)
	if res2.Error() == nil {
		t.Fatal("join with errored right frame should carry the error")
	}
}

// --- immutability & determinism ---

func TestJoinDoesNotMutateInputs(t *testing.T) {
	left, right := ordersCustomers(t)
	leftBefore := left.String()
	rightBefore := right.String()

	_ = left.Join(right, []string{"customer_id"}, OuterJoin)

	if left.String() != leftBefore {
		t.Error("Join mutated the left frame")
	}
	if right.String() != rightBefore {
		t.Error("Join mutated the right frame")
	}
	if left.Error() != nil || right.Error() != nil {
		t.Error("Join set an error on an input frame")
	}
}

func TestJoinResultIsIndependentCopy(t *testing.T) {
	left := dfFromSeries(t, "id", []int64{1}, "v", []int64{10})
	right := dfFromSeries(t, "id", []int64{1}, "w", []int64{20})
	res := left.Join(right, []string{"id"}, InnerJoin)

	if err := res.Set(0, "v", int64(999)); err != nil {
		t.Fatal(err)
	}
	orig, _ := left.Get(0, "v")
	if orig != int64(10) {
		t.Error("mutating join result changed the input frame")
	}
}

func TestJoinDeterministic(t *testing.T) {
	left, right := ordersCustomers(t)
	first := left.Join(right, []string{"customer_id"}, OuterJoin).String()
	for i := 0; i < 10; i++ {
		again := left.Join(right, []string{"customer_id"}, OuterJoin).String()
		if again != first {
			t.Fatalf("join output not deterministic on run %d:\n%s\nvs\n%s", i, first, again)
		}
	}
}

// --- chaining ---

func TestJoinChainsWithOtherOps(t *testing.T) {
	left, right := ordersCustomers(t)
	res := left.
		Join(right, []string{"customer_id"}, InnerJoin).
		Filter("amount", ">", 150.0).
		Sort("order_id", true)
	if res.Error() != nil {
		t.Fatalf("chained join error: %v", res.Error())
	}
	checkColumn(t, res, "order_id", []int64{2, 4})
	checkColumn(t, res, "name", []string{"Bob", "Alice"})
}

func TestJoinLargeConsistency(t *testing.T) {
	// A larger randomized-shape join cross-checked against a brute-force nested loop.
	n, m := 500, 300
	lk := make([]int64, n)
	lv := make([]int64, n)
	for i := range lk {
		lk[i] = int64((i*7)%97) + 1 // avoid 0: null sentinel
		lv[i] = int64(i)
	}
	rk := make([]int64, m)
	rv := make([]int64, m)
	for j := range rk {
		rk[j] = int64((j*13)%89) + 1
		rv[j] = int64(j)
	}
	left := dfFromSeries(t, "k", lk, "lrow", lv)
	right := dfFromSeries(t, "k", rk, "rrow", rv)

	res := left.Join(right, []string{"k"}, InnerJoin)
	if res.Error() != nil {
		t.Fatalf("join error: %v", res.Error())
	}

	// Brute force expected pairs in left-major, right-minor order.
	var wantL, wantR []int64
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			if lk[i] == rk[j] {
				wantL = append(wantL, lv[i])
				wantR = append(wantR, rv[j])
			}
		}
	}
	checkColumn(t, res, "lrow", wantL)
	checkColumn(t, res, "rrow", wantR)
}

// --- full type-space coverage ---

func TestJoinCompositeKeysAllSpaces(t *testing.T) {
	ts := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	// Composite key spanning every comparison space, including a mixed
	// int64/float64 component (left int, right float).
	left := dfFromSeries(t,
		"s", []string{"x", "y"},
		"i", []int64{1, 2},
		"f", []int64{7, 8}, // int on left, float on right → float space
		"b", []bool{true, true},
		"ts", []time.Time{ts, ts},
		"v", []int64{100, 200},
	)
	right := dfFromSeries(t,
		"s", []string{"x", "y"},
		"i", []int64{1, 99},
		"f", []float64{7, 8},
		"b", []bool{true, true},
		"ts", []time.Time{ts, ts},
		"w", []int64{111, 222},
	)
	res := left.Join(right, []string{"s", "i", "f", "b", "ts"}, InnerJoin)
	if res.Error() != nil {
		t.Fatalf("composite all-spaces join error: %v", res.Error())
	}
	// Only row 0 matches (right row 1 has i=99).
	checkColumn(t, res, "v", []int64{100})
	checkColumn(t, res, "w", []int64{111})
	// Mixed numeric key component is promoted to float64 in the output.
	ct, _ := res.GetColumnType("f")
	if ct != Float64Type {
		t.Errorf("mixed numeric composite key output type = %v, want Float64Type", ct)
	}
}

func TestJoinOuterCompositeKeys(t *testing.T) {
	left := dfFromSeries(t,
		"a", []string{"x", "y"},
		"n", []int64{1, 2},
		"v", []float64{1.5, 2.5},
	)
	right := dfFromSeries(t,
		"a", []string{"x", "z"},
		"n", []int64{1, 3},
		"w", []float64{10, 30},
	)
	res := left.Join(right, []string{"a", "n"}, OuterJoin)
	// Left rows in order, then unmatched right row; key values for the
	// right-only row come from the right frame.
	checkColumn(t, res, "a", []string{"x", "y", "z"})
	checkColumn(t, res, "n", []int64{1, 2, 3})
	checkColumn(t, res, "v", []float64{1.5, 2.5, 0})
	checkColumn(t, res, "w", []float64{10, 0, 30})
}

func TestJoinOuterFillsAllColumnTypes(t *testing.T) {
	ts := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	left := dfFromSeries(t,
		"id", []int64{1, 2},
		"ls", []string{"a", "b"},
		"lf", []float64{1.1, 2.2},
		"lb", []bool{true, true},
		"lt", []time.Time{ts, ts},
	)
	right := dfFromSeries(t,
		"id", []int64{2, 3},
		"rs", []string{"B", "C"},
		"rf", []float64{20, 30},
		"rb", []bool{true, true},
		"rt", []time.Time{ts, ts},
	)
	res := left.Join(right, []string{"id"}, OuterJoin)
	if res.Error() != nil {
		t.Fatalf("outer join error: %v", res.Error())
	}
	var zt time.Time
	checkColumn(t, res, "id", []int64{1, 2, 3})
	checkColumn(t, res, "ls", []string{"a", "b", ""})
	checkColumn(t, res, "lf", []float64{1.1, 2.2, 0})
	checkColumn(t, res, "lb", []bool{true, true, false})
	checkColumn(t, res, "lt", []time.Time{ts, ts, zt})
	checkColumn(t, res, "rs", []string{"", "B", "C"})
	checkColumn(t, res, "rf", []float64{0, 20, 30})
	checkColumn(t, res, "rb", []bool{false, true, true})
	checkColumn(t, res, "rt", []time.Time{zt, ts, ts})
}

func TestJoinOuterKeyTypesStringBoolTime(t *testing.T) {
	ts1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	// String key, right-only row feeds the key column.
	l1 := dfFromSeries(t, "k", []string{"a"}, "v", []int64{1})
	r1 := dfFromSeries(t, "k", []string{"b"}, "w", []int64{2})
	res1 := l1.Join(r1, []string{"k"}, OuterJoin)
	checkColumn(t, res1, "k", []string{"a", "b"})

	// Time key.
	l2 := dfFromSeries(t, "k", []time.Time{ts1}, "v", []int64{1})
	r2 := dfFromSeries(t, "k", []time.Time{ts2}, "w", []int64{2})
	res2 := l2.Join(r2, []string{"k"}, OuterJoin)
	checkColumn(t, res2, "k", []time.Time{ts1, ts2})

	// Bool key: true matches true; both sides' false rows are null keys.
	l3 := dfFromSeries(t, "k", []bool{true, false}, "v", []int64{1, 2})
	r3 := dfFromSeries(t, "k", []bool{true, false}, "w", []int64{10, 20})
	res3 := l3.Join(r3, []string{"k"}, OuterJoin)
	checkColumn(t, res3, "k", []bool{true, false, false})
	checkColumn(t, res3, "v", []int64{1, 2, 0})
	checkColumn(t, res3, "w", []int64{10, 0, 20})

	// Float key on both sides stays Float64Type.
	l4 := dfFromSeries(t, "k", []float64{1.5}, "v", []int64{1})
	r4 := dfFromSeries(t, "k", []float64{2.5}, "w", []int64{2})
	res4 := l4.Join(r4, []string{"k"}, OuterJoin)
	checkColumn(t, res4, "k", []float64{1.5, 2.5})
	ct, _ := res4.GetColumnType("k")
	if ct != Float64Type {
		t.Errorf("float key type = %v, want Float64Type", ct)
	}
}
