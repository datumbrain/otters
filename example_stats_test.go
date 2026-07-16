package otters

import (
	"fmt"
	"log"
)

// Example_statistics demonstrates statistical analysis
func Example_statistics() {
	// Sales data
	data := map[string]any{
		"region":  []string{"North", "South", "East", "West", "North", "South"},
		"sales":   []float64{120000, 110000, 95000, 130000, 125000, 115000},
		"quarter": []int64{1, 1, 1, 1, 2, 2},
	}

	df, err := NewDataFrameFromMap(data)
	if err != nil {
		log.Fatal(err)
	}

	// Basic statistics
	fmt.Println("=== Sales Statistics ===")
	total, _ := df.Sum("sales")
	average, _ := df.Mean("sales")
	minSales, _ := df.Min("sales")
	maxSales, _ := df.Max("sales")
	stdDev, _ := df.Std("sales")

	fmt.Printf("Total Sales: $%.0f\n", total)
	fmt.Printf("Average: $%.0f\n", average)
	fmt.Printf("Range: $%.0f - $%.0f\n", minSales, maxSales)
	fmt.Printf("Std Dev: $%.0f\n", stdDev)

	// Detailed summary
	fmt.Println("\n=== Detailed Summary ===")
	summary, err := df.Describe()
	if err == nil && summary != nil {
		fmt.Print(summary)
	}

	// Regional analysis
	fmt.Println("\n=== Regional Analysis ===")
	regional, err := df.GroupBy("region").Sum()
	if err == nil && regional != nil {
		fmt.Print(regional)
	}

	// Output:
	// === Sales Statistics ===
	// Total Sales: $695000
	// Average: $115833
	// Range: $95000 - $130000
	// Std Dev: $12416
	//
	// === Detailed Summary ===
	// statistic	quarter	sales
	// count	6	6
	// mean	1.333333	115833.333333
	// std	0.516398	12416.387021
	// min	1.000000	95000.000000
	// 25%	1.000000	111250.000000
	// 50%	1.000000	117500.000000
	// 75%	1.750000	123750.000000
	// max	2.000000	130000.000000
	//
	// === Regional Analysis ===
	// region	quarter	sales
	// East	1	95000
	// North	3	245000
	// South	3	225000
	// West	1	130000
}
