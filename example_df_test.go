package otters

import (
	"fmt"
	"log"
)

// Example_basicDataFrame demonstrates basic DataFrame operations
func Example_basicDataFrame() {
	// Create sample data
	data := map[string]any{
		"name":   []string{"Alice", "Bob", "Carol"},
		"age":    []int64{25, 30, 35},
		"salary": []float64{50000, 60000, 70000},
	}

	df, err := NewDataFrameFromMap(data)
	if err != nil {
		log.Fatal(err)
	}

	// Basic info
	rows, cols := df.Shape()
	fmt.Printf("Shape: (%d, %d)\n", rows, cols)
	fmt.Printf("Columns: %d\n", len(df.Columns()))

	// Display first row data
	name, _ := df.Get(0, "name")
	age, _ := df.Get(0, "age")
	fmt.Printf("First person: %s, age %d\n", name, age)

	// Output:
	// Shape: (3, 3)
	// Columns: 3
	// First person: Alice, age 25
}
