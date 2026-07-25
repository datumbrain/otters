package otters

import (
	"fmt"
	"strconv"
	"time"
)

// JoinType specifies the kind of relational join to perform.
type JoinType int

const (
	// InnerJoin keeps only rows whose key exists in both frames.
	InnerJoin JoinType = iota
	// LeftJoin keeps every left row, zero-filling right columns when unmatched.
	LeftJoin
	// RightJoin keeps every right row, zero-filling left columns when unmatched.
	RightJoin
	// OuterJoin keeps every row from both frames.
	OuterJoin
)

// String returns the string representation of a JoinType.
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "inner"
	case LeftJoin:
		return "left"
	case RightJoin:
		return "right"
	case OuterJoin:
		return "outer"
	default:
		return "unknown"
	}
}

// Join combines two DataFrames on one or more shared key columns.
//
// Semantics:
//   - Key columns must have the same type in both frames, except that an
//     int64/float64 pairing is allowed: matching then happens in float64
//     space and the output key column is promoted to Float64Type.
//   - A key equal to its column type's zero value ("", 0, 0.0, false, zero
//     time) is treated as missing and never matches — such rows are dropped
//     by an inner join and zero-filled on the outer side otherwise. For
//     composite keys, one missing component makes the whole key missing.
//   - Key columns appear once in the output. Non-key columns present in both
//     frames are suffixed "_left" / "_right".
//   - Row order is deterministic: left-frame row order first (a left row
//     matching several right rows expands in right-frame order), then, for
//     right/outer joins, unmatched right rows in right-frame order.
//   - Unmatched sides are filled with the column type's zero value.
//
// Join never mutates its inputs; errors are carried on the returned frame.
func (df *DataFrame) Join(other *DataFrame, on []string, how JoinType) *DataFrame {
	return df.JoinOn(other, on, on, how)
}

// JoinOn joins two DataFrames whose key columns are named differently.
// leftKeys and rightKeys are matched positionally and must have equal length.
// The output contains each key column once, under its left-frame name; for
// right/outer joins, unmatched right rows contribute their key values into
// that column. See Join for the full semantics.
func (df *DataFrame) JoinOn(other *DataFrame, leftKeys, rightKeys []string, how JoinType) *DataFrame {
	const op = "Join"

	if df.err != nil {
		return df
	}
	if other == nil {
		return df.setError(newOpError(op, "other DataFrame is nil"))
	}
	if other.err != nil {
		return df.setError(wrapError(op, other.err))
	}
	if how < InnerJoin || how > OuterJoin {
		return df.setError(newOpError(op, fmt.Sprintf("invalid join type %d", int(how))))
	}
	if len(leftKeys) == 0 {
		return df.setError(newOpError(op, "at least one key column must be specified"))
	}
	if len(leftKeys) != len(rightKeys) {
		return df.setError(newOpError(op,
			fmt.Sprintf("leftKeys and rightKeys must have the same length (%d vs %d)", len(leftKeys), len(rightKeys))))
	}
	if col, ok := firstDuplicate(leftKeys); ok {
		return df.setError(newColumnError(op, col, "key column specified more than once"))
	}
	if col, ok := firstDuplicate(rightKeys); ok {
		return df.setError(newColumnError(op, col, "key column specified more than once"))
	}
	if err := df.validateColumnsExist(leftKeys); err != nil {
		return df.setError(err)
	}
	if err := other.validateColumnsExist(rightKeys); err != nil {
		return df.setError(err)
	}

	spaces, err := joinKeySpaces(df, other, leftKeys, rightKeys)
	if err != nil {
		return df.setError(err)
	}

	leftOut, rightOut, err := joinOutputNames(df, other, leftKeys, rightKeys)
	if err != nil {
		return df.setError(err)
	}

	leftIdx, rightIdx := joinRowPairs(df, other, leftKeys, rightKeys, spaces, how)

	return buildJoinResult(df, other, leftKeys, rightKeys, spaces, leftOut, rightOut, leftIdx, rightIdx)
}

// joinSpace is the unified comparison space for one key column pair.
type joinSpace int

const (
	spaceInt joinSpace = iota
	spaceFloat
	spaceString
	spaceBool
	spaceTime
)

// joinKeySpaces validates key type compatibility and returns the comparison
// space for each key pair. Identical types map to their own space; an
// int64/float64 mix is compared (and emitted) in float64 space; any other
// combination is an error.
func joinKeySpaces(left, right *DataFrame, leftKeys, rightKeys []string) ([]joinSpace, error) {
	spaces := make([]joinSpace, len(leftKeys))
	for k := range leftKeys {
		lt := left.columns[leftKeys[k]].Type
		rt := right.columns[rightKeys[k]].Type

		switch {
		case lt == rt:
			switch lt {
			case Int64Type:
				spaces[k] = spaceInt
			case Float64Type:
				spaces[k] = spaceFloat
			case StringType:
				spaces[k] = spaceString
			case BoolType:
				spaces[k] = spaceBool
			case TimeType:
				spaces[k] = spaceTime
			default:
				return nil, newColumnError("Join", leftKeys[k], "unsupported key column type")
			}
		case (lt == Int64Type && rt == Float64Type) || (lt == Float64Type && rt == Int64Type):
			spaces[k] = spaceFloat
		default:
			return nil, newColumnError("Join", leftKeys[k],
				fmt.Sprintf("key type mismatch: left %q is %s, right %q is %s",
					leftKeys[k], lt, rightKeys[k], rt))
		}
	}
	return spaces, nil
}

// joinOutputNames computes the output column names. leftOut is indexed like
// left.order; rightOut like right.order, with "" marking right key columns
// (which are dropped — the key appears once under its left name). Key columns
// always keep their name; colliding non-key columns get "_left" / "_right".
func joinOutputNames(left, right *DataFrame, leftKeys, rightKeys []string) (leftOut, rightOut []string, err error) {
	leftKeySet := make(map[string]bool, len(leftKeys))
	for _, k := range leftKeys {
		leftKeySet[k] = true
	}
	rightKeySet := make(map[string]bool, len(rightKeys))
	for _, k := range rightKeys {
		rightKeySet[k] = true
	}

	leftNameSet := make(map[string]bool, len(left.order))
	for _, c := range left.order {
		leftNameSet[c] = true
	}
	rightNonKeySet := make(map[string]bool, len(right.order))
	for _, c := range right.order {
		if !rightKeySet[c] {
			rightNonKeySet[c] = true
		}
	}

	leftOut = make([]string, len(left.order))
	for i, c := range left.order {
		switch {
		case leftKeySet[c]:
			leftOut[i] = c
		case rightNonKeySet[c]:
			leftOut[i] = c + "_left"
		default:
			leftOut[i] = c
		}
	}

	rightOut = make([]string, len(right.order))
	for i, c := range right.order {
		switch {
		case rightKeySet[c]:
			rightOut[i] = "" // dropped: key emitted once under its left name
		case leftNameSet[c]:
			rightOut[i] = c + "_right"
		default:
			rightOut[i] = c
		}
	}

	seen := make(map[string]bool, len(leftOut)+len(rightOut))
	for _, name := range leftOut {
		if seen[name] {
			return nil, nil, newColumnError("Join", name, "output column name collision")
		}
		seen[name] = true
	}
	for _, name := range rightOut {
		if name == "" {
			continue
		}
		if seen[name] {
			return nil, nil, newColumnError("Join", name, "output column name collision")
		}
		seen[name] = true
	}
	return leftOut, rightOut, nil
}

// joinRowPairs computes the output row mapping as parallel slices of left and
// right source row indices; -1 marks the zero-filled side of an unmatched row.
func joinRowPairs(left, right *DataFrame, leftKeys, rightKeys []string, spaces []joinSpace, how JoinType) ([]int, []int) {
	if len(leftKeys) == 1 {
		ls := left.columns[leftKeys[0]]
		rs := right.columns[rightKeys[0]]
		switch spaces[0] {
		case spaceInt:
			lk := ls.Data.([]int64)
			rk := rs.Data.([]int64)
			return hashJoinPairs(lk, nullMaskInt64(lk), rk, nullMaskInt64(rk), how)
		case spaceFloat:
			lk := asFloat64Keys(ls)
			rk := asFloat64Keys(rs)
			return hashJoinPairs(lk, nullMaskFloat64(lk), rk, nullMaskFloat64(rk), how)
		case spaceString:
			lk := ls.Data.([]string)
			rk := rs.Data.([]string)
			return hashJoinPairs(lk, nullMaskString(lk), rk, nullMaskString(rk), how)
		case spaceBool:
			lk := ls.Data.([]bool)
			rk := rs.Data.([]bool)
			return hashJoinPairs(lk, nullMaskBool(lk), rk, nullMaskBool(rk), how)
		case spaceTime:
			lt := ls.Data.([]time.Time)
			rt := rs.Data.([]time.Time)
			return hashJoinPairs(timeKeys(lt), nullMaskTime(lt), timeKeys(rt), nullMaskTime(rt), how)
		}
	}

	// Composite keys: encode each row's key tuple into a string.
	lk, lnull := encodeCompositeKeys(left, leftKeys, spaces)
	rk, rnull := encodeCompositeKeys(right, rightKeys, spaces)
	return hashJoinPairs(lk, lnull, rk, rnull, how)
}

// hashJoinPairs is the join core: it indexes the right side's non-null keys,
// probes with each left row in order, and appends unmatched right rows for
// right/outer joins. Null keys never match.
func hashJoinPairs[K comparable](lk []K, lnull []bool, rk []K, rnull []bool, how JoinType) ([]int, []int) {
	// Chained hash index: head maps a key to its first right row, next links
	// rows sharing that key. Building in reverse makes each chain run in
	// right-frame order, and the flat next slice avoids allocating a small
	// row slice per distinct key.
	head := make(map[K]int, len(rk))
	next := make([]int, len(rk))
	for j := len(rk) - 1; j >= 0; j-- {
		if rnull[j] {
			continue
		}
		if h, ok := head[rk[j]]; ok {
			next[j] = h
		} else {
			next[j] = -1
		}
		head[rk[j]] = j
	}

	var matched []bool
	if how == RightJoin || how == OuterJoin {
		matched = make([]bool, len(rk))
	}

	leftIdx := make([]int, 0, len(lk))
	rightIdx := make([]int, 0, len(lk))

	for i, key := range lk {
		j := -1
		if !lnull[i] {
			if h, ok := head[key]; ok {
				j = h
			}
		}
		if j < 0 {
			if how == LeftJoin || how == OuterJoin {
				leftIdx = append(leftIdx, i)
				rightIdx = append(rightIdx, -1)
			}
			continue
		}
		for ; j >= 0; j = next[j] {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, j)
			if matched != nil {
				matched[j] = true
			}
		}
	}

	if matched != nil {
		for j := range rk {
			if !matched[j] {
				leftIdx = append(leftIdx, -1)
				rightIdx = append(rightIdx, j)
			}
		}
	}

	return leftIdx, rightIdx
}

// Null-key masks: the zero value of the key column type marks a missing key.

func nullMaskInt64(data []int64) []bool {
	mask := make([]bool, len(data))
	for i, v := range data {
		mask[i] = v == 0
	}
	return mask
}

func nullMaskFloat64(data []float64) []bool {
	mask := make([]bool, len(data))
	for i, v := range data {
		mask[i] = v == 0
	}
	return mask
}

func nullMaskString(data []string) []bool {
	mask := make([]bool, len(data))
	for i, v := range data {
		mask[i] = v == ""
	}
	return mask
}

func nullMaskBool(data []bool) []bool {
	mask := make([]bool, len(data))
	for i, v := range data {
		mask[i] = !v
	}
	return mask
}

func nullMaskTime(data []time.Time) []bool {
	mask := make([]bool, len(data))
	for i, v := range data {
		mask[i] = v.IsZero()
	}
	return mask
}

// timeKeys maps times to comparable int64 keys. UnixNano compares instants,
// so the same moment in different locations hashes identically (matching
// time.Time.Equal semantics used elsewhere in the library).
func timeKeys(data []time.Time) []int64 {
	keys := make([]int64, len(data))
	for i, v := range data {
		keys[i] = v.UnixNano()
	}
	return keys
}

// asFloat64Keys returns the series data as a float64 slice, converting from
// int64 when the key pair mixes numeric types. Float64 series are returned
// as-is (read-only use).
func asFloat64Keys(s *Series) []float64 {
	if s.Type == Float64Type {
		return s.Data.([]float64)
	}
	data := s.Data.([]int64)
	out := make([]float64, len(data))
	for i, v := range data {
		out[i] = float64(v)
	}
	return out
}

// encodeCompositeKeys builds one string key per row from multiple key
// columns. Components are rendered in the pair's comparison space, separated
// by a zero byte; string components are length-prefixed so tuple boundaries
// stay unambiguous (("ab","c") never equals ("a","bc")). A row is null if any
// component is null.
func encodeCompositeKeys(df *DataFrame, keys []string, spaces []joinSpace) ([]string, []bool) {
	n := df.length
	out := make([]string, n)
	null := make([]bool, n)

	series := make([]*Series, len(keys))
	for k, name := range keys {
		series[k] = df.columns[name]
	}

	buf := make([]byte, 0, 64)
	for i := 0; i < n; i++ {
		buf = buf[:0]
		rowNull := false
		for k, s := range series {
			if k > 0 {
				buf = append(buf, 0)
			}
			var componentNull bool
			buf, componentNull = appendKeyComponent(buf, s, i, spaces[k])
			rowNull = rowNull || componentNull
		}
		null[i] = rowNull
		if !rowNull {
			out[i] = string(buf)
		}
	}
	return out, null
}

// appendKeyComponent renders one key cell into buf in the given comparison
// space and reports whether the cell is the null sentinel.
func appendKeyComponent(buf []byte, s *Series, row int, space joinSpace) ([]byte, bool) {
	switch space {
	case spaceInt:
		v := s.Data.([]int64)[row]
		return strconv.AppendInt(buf, v, 10), v == 0
	case spaceFloat:
		var v float64
		if s.Type == Int64Type {
			v = float64(s.Data.([]int64)[row])
		} else {
			v = s.Data.([]float64)[row]
		}
		return strconv.AppendFloat(buf, v, 'g', -1, 64), v == 0
	case spaceString:
		v := s.Data.([]string)[row]
		buf = strconv.AppendInt(buf, int64(len(v)), 10)
		buf = append(buf, ':')
		return append(buf, v...), v == ""
	case spaceBool:
		v := s.Data.([]bool)[row]
		if v {
			return append(buf, 't'), false
		}
		return append(buf, 'f'), true
	case spaceTime:
		v := s.Data.([]time.Time)[row]
		return strconv.AppendInt(buf, v.UnixNano(), 10), v.IsZero()
	default:
		return buf, true
	}
}

// buildJoinResult materializes the output frame from the row-pair mapping.
func buildJoinResult(left, right *DataFrame, leftKeys, rightKeys []string, spaces []joinSpace,
	leftOut, rightOut []string, leftIdx, rightIdx []int) *DataFrame {

	leftKeyPos := make(map[string]int, len(leftKeys))
	for k, name := range leftKeys {
		leftKeyPos[name] = k
	}

	newDf := NewDataFrame()
	newDf.length = len(leftIdx)

	for i, col := range left.order {
		var data any
		if k, isKey := leftKeyPos[col]; isKey {
			data = buildKeyColumnData(left.columns[col], right.columns[rightKeys[k]], spaces[k], leftIdx, rightIdx)
		} else {
			data = gatherSeriesWithFill(left.columns[col], leftIdx)
		}
		series, err := newSeriesOwned(leftOut[i], data)
		if err != nil {
			return left.setError(wrapColumnError("Join", col, err))
		}
		newDf.addSeriesUnsafe(series)
	}

	for i, col := range right.order {
		if rightOut[i] == "" {
			continue // right key column: values already emitted under the left key name
		}
		series, err := newSeriesOwned(rightOut[i], gatherSeriesWithFill(right.columns[col], rightIdx))
		if err != nil {
			return left.setError(wrapColumnError("Join", col, err))
		}
		newDf.addSeriesUnsafe(series)
	}

	return newDf
}

// buildKeyColumnData assembles a key column: values come from the left row
// when present, otherwise from the right row (right/outer unmatched rows).
// A mixed int64/float64 key pair is emitted as float64.
func buildKeyColumnData(ls, rs *Series, space joinSpace, leftIdx, rightIdx []int) any {
	if space == spaceFloat && (ls.Type != Float64Type || rs.Type != Float64Type) {
		return gatherKeyColumn(asFloat64Keys(ls), asFloat64Keys(rs), leftIdx, rightIdx, 0)
	}

	switch ls.Type {
	case StringType:
		return gatherKeyColumn(ls.Data.([]string), rs.Data.([]string), leftIdx, rightIdx, "")
	case Int64Type:
		return gatherKeyColumn(ls.Data.([]int64), rs.Data.([]int64), leftIdx, rightIdx, 0)
	case Float64Type:
		return gatherKeyColumn(ls.Data.([]float64), rs.Data.([]float64), leftIdx, rightIdx, 0)
	case BoolType:
		return gatherKeyColumn(ls.Data.([]bool), rs.Data.([]bool), leftIdx, rightIdx, false)
	case TimeType:
		return gatherKeyColumn(ls.Data.([]time.Time), rs.Data.([]time.Time), leftIdx, rightIdx, time.Time{})
	default:
		return nil
	}
}

// gatherKeyColumn picks each output row's key from the left source when its
// index is present, falling back to the right source, then to the zero value.
func gatherKeyColumn[T any](l, r []T, leftIdx, rightIdx []int, zero T) []T {
	out := make([]T, len(leftIdx))
	for i := range leftIdx {
		switch {
		case leftIdx[i] >= 0:
			out[i] = l[leftIdx[i]]
		case rightIdx[i] >= 0:
			out[i] = r[rightIdx[i]]
		default:
			out[i] = zero
		}
	}
	return out
}

// gatherSeriesWithFill extracts rows at idx from a series; -1 produces the
// column type's zero value.
func gatherSeriesWithFill(s *Series, idx []int) any {
	switch s.Type {
	case StringType:
		return gatherWithFill(s.Data.([]string), idx, "")
	case Int64Type:
		return gatherWithFill(s.Data.([]int64), idx, 0)
	case Float64Type:
		return gatherWithFill(s.Data.([]float64), idx, 0)
	case BoolType:
		return gatherWithFill(s.Data.([]bool), idx, false)
	case TimeType:
		return gatherWithFill(s.Data.([]time.Time), idx, time.Time{})
	default:
		return nil
	}
}

func gatherWithFill[T any](data []T, idx []int, zero T) []T {
	out := make([]T, len(idx))
	for i, j := range idx {
		if j >= 0 {
			out[i] = data[j]
		} else {
			out[i] = zero
		}
	}
	return out
}

// firstDuplicate returns the first name appearing more than once.
func firstDuplicate(names []string) (string, bool) {
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if seen[n] {
			return n, true
		}
		seen[n] = true
	}
	return "", false
}
