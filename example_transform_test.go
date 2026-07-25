package otters

import (
	"fmt"
)

// Example_transform demonstrates deriving columns and handling missing values.
func Example_transform() {
	name, _ := NewSeries("name", []string{"ann", "", "cy"})
	salary, _ := NewSeries("salary", []float64{1000, 2000, 0})
	df, _ := NewDataFrameFromSeries(name, salary)

	res := df.
		FillNA("name", "unknown").
		DropNA("salary").
		Apply("bonus", func(row map[string]any) any {
			return row["salary"].(float64) * 0.1
		})
	if res.Error() != nil {
		fmt.Println("transform failed:", res.Error())
		return
	}
	fmt.Print(res)

	// Output:
	// name	salary	bonus
	// ann	1000	100
	// unknown	2000	200
}
