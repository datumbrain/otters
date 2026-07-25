package otters

import (
	"strconv"
	"testing"
)

func benchFrames(b *testing.B, nLeft, nRight int) (*DataFrame, *DataFrame) {
	b.Helper()
	lk := make([]int64, nLeft)
	lv := make([]float64, nLeft)
	ls := make([]string, nLeft)
	for i := range lk {
		lk[i] = int64(i%nRight) + 1
		lv[i] = float64(i)
		ls[i] = "left-" + strconv.Itoa(i%1000)
	}
	rk := make([]int64, nRight)
	rn := make([]string, nRight)
	for j := range rk {
		rk[j] = int64(j) + 1
		rn[j] = "name-" + strconv.Itoa(j)
	}

	mustSeries := func(name string, data any) *Series {
		s, err := NewSeries(name, data)
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	left, err := NewDataFrameFromSeries(
		mustSeries("id", lk), mustSeries("value", lv), mustSeries("tag", ls))
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewDataFrameFromSeries(
		mustSeries("id", rk), mustSeries("name", rn))
	if err != nil {
		b.Fatal(err)
	}
	return left, right
}

func BenchmarkJoinInnerInt64_100k_x_10k(b *testing.B) {
	left, right := benchFrames(b, 100_000, 10_000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := left.Join(right, []string{"id"}, InnerJoin)
		if res.Error() != nil {
			b.Fatal(res.Error())
		}
	}
}

func BenchmarkJoinLeftInt64_100k_x_10k(b *testing.B) {
	left, right := benchFrames(b, 100_000, 10_000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := left.Join(right, []string{"id"}, LeftJoin)
		if res.Error() != nil {
			b.Fatal(res.Error())
		}
	}
}

func BenchmarkJoinInnerString_100k_x_10k(b *testing.B) {
	nLeft, nRight := 100_000, 10_000
	lk := make([]string, nLeft)
	lv := make([]int64, nLeft)
	for i := range lk {
		lk[i] = "key-" + strconv.Itoa(i%nRight)
		lv[i] = int64(i)
	}
	rk := make([]string, nRight)
	rv := make([]int64, nRight)
	for j := range rk {
		rk[j] = "key-" + strconv.Itoa(j)
		rv[j] = int64(j)
	}
	mustSeries := func(name string, data any) *Series {
		s, err := NewSeries(name, data)
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	left, _ := NewDataFrameFromSeries(mustSeries("k", lk), mustSeries("v", lv))
	right, _ := NewDataFrameFromSeries(mustSeries("k", rk), mustSeries("w", rv))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := left.Join(right, []string{"k"}, InnerJoin)
		if res.Error() != nil {
			b.Fatal(res.Error())
		}
	}
}

func BenchmarkJoinInnerComposite_100k_x_10k(b *testing.B) {
	nLeft, nRight := 100_000, 10_000
	lk1 := make([]string, nLeft)
	lk2 := make([]int64, nLeft)
	for i := range lk1 {
		lk1[i] = "grp-" + strconv.Itoa(i%100)
		lk2[i] = int64(i % 100)
	}
	// Right keys are unique; the first 100 line up with the left key domain,
	// so every left row matches exactly one right row (100k output rows).
	rk1 := make([]string, nRight)
	rk2 := make([]int64, nRight)
	for j := range rk1 {
		rk1[j] = "grp-" + strconv.Itoa(j%100)
		rk2[j] = int64(j)
	}
	mustSeries := func(name string, data any) *Series {
		s, err := NewSeries(name, data)
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	left, _ := NewDataFrameFromSeries(mustSeries("g", lk1), mustSeries("n", lk2))
	right, _ := NewDataFrameFromSeries(mustSeries("g", rk1), mustSeries("n", rk2))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := left.Join(right, []string{"g", "n"}, InnerJoin)
		if res.Error() != nil {
			b.Fatal(res.Error())
		}
	}
}
