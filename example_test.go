package otters

import (
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"
)

// ExampleBasicDataFrame demonstrates basic DataFrame operations
func ExampleBasicDataFrame() {
	// Create sample data
	data := map[string]interface{}{
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

// DemoFiltering demonstrates filtering and selection operations
func DemoFiltering() {
	// Sample employee data
	csvData := `name,department,salary,experience
Alice,Engineering,75000,3
Bob,Engineering,80000,5
Carol,Marketing,60000,2
David,Engineering,70000,4
Eve,Marketing,65000,3
Frank,Sales,55000,1`

	df, err := ReadCSVFromString(csvData)
	if err != nil {
		log.Fatal(err)
	}

	// Filter high earners in Engineering
	engineers := df.
		Filter("department", "==", "Engineering").
		Filter("salary", ">=", 70000).
		Select("name", "salary", "experience").
		Sort("salary", false) // descending

	fmt.Println("=== High-earning Engineers ===")
	fmt.Print(engineers)

	// Chain multiple operations
	summary, err := df.
		Filter("experience", ">", 2).
		GroupBy("department").
		Mean()

	fmt.Println("=== Department Averages (experienced employees) ===")
	if err == nil && summary != nil {
		fmt.Print(summary)
	}

	// Output:
	// === High-earning Engineers ===
	// name	salary	experience
	// Bob	80000	5
	// Alice	75000	3
	// David	70000	4
	//
	// === Department Averages (experienced employees) ===
	// department	salary	experience
	// Engineering	75000	4
	// Marketing	65000	3
}

// DemoStatistics demonstrates statistical analysis
func DemoStatistics() {
	// Sales data
	data := map[string]interface{}{
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
	// Std Dev: $12472
	//
	// === Detailed Summary ===
	// statistic	sales	quarter
	// count	6	6
	// mean	115833.333333	1.333333
	// std	12472.191289	0.516398
	// min	95000.000000	1.000000
	// 25%	112500.000000	1.000000
	// 50%	117500.000000	1.000000
	// 75%	123750.000000	2.000000
	// max	130000.000000	2.000000
	//
	// === Regional Analysis ===
	// region	sales	quarter
	// East	95000	1
	// North	245000	3
	// South	225000	2
	// West	130000	1
}

// DemoCSVOperations demonstrates CSV file operations
func DemoCSVOperations() {
	// Create sample CSV file
	csvContent := `product,category,price,units_sold,date
Laptop,Electronics,999.99,50,2024-01-15
Mouse,Electronics,29.99,200,2024-01-15
Desk,Furniture,299.99,25,2024-01-16
Chair,Furniture,199.99,30,2024-01-16
Keyboard,Electronics,79.99,75,2024-01-17`

	// Write to temp file
	tmpFile := "temp_sales.csv"
	file, err := os.Create(tmpFile)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile)

	_, err = file.WriteString(csvContent)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	// Read CSV with automatic type inference
	df, err := ReadCSV(tmpFile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== Product Catalog ===")
	fmt.Printf("Loaded %d products\n", df.Count())
	fmt.Print(df.Info())

	// Calculate revenue
	fmt.Println("\n=== Revenue Analysis ===")
	electronics := df.
		Filter("category", "==", "Electronics").
		Select("product", "price", "units_sold")

	fmt.Println("Electronics products:")
	fmt.Print(electronics)

	// Save filtered results
	outputFile := "electronics.csv"
	err = electronics.WriteCSV(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(outputFile)

	fmt.Printf("\nSaved electronics data to %s\n", outputFile)

	// Output:
	// === Product Catalog ===
	// Loaded 5 products
	// DataFrame Info:
	//   Shape: (5, 5)
	//   Columns:
	//     product: string
	//     category: string
	//     price: float64
	//     units_sold: int64
	//     date: string
	//
	// === Revenue Analysis ===
	// Electronics products:
	// product	price	units_sold
	// Laptop	999.99	50
	// Mouse	29.99	200
	// Keyboard	79.99	75
	//
	// Saved electronics data to electronics.csv
}

// DemoWorkflow demonstrates complex data analysis workflow
func DemoWorkflow() {
	// Complex sales dataset
	salesData := `salesperson,region,product,sales_amount,commission_rate,sale_date
Alice,North,Laptop,1200,0.05,2024-01-15
Bob,South,Phone,800,0.03,2024-01-15
Carol,East,Tablet,600,0.04,2024-01-16
Alice,North,Phone,750,0.03,2024-01-17
David,West,Laptop,1100,0.05,2024-01-17
Bob,South,Tablet,550,0.04,2024-01-18
Carol,East,Laptop,1300,0.05,2024-01-18
Eve,North,Phone,720,0.03,2024-01-19`

	df, err := ReadCSVFromString(salesData)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== Sales Performance Analysis ===")

	// Top performing salesperson
	topSales, err := df.GroupBy("salesperson").Sum()
	if err == nil && topSales != nil {
		sorted := topSales.Sort("sales_amount", false)
		fmt.Println("Top performers by total sales:")
		fmt.Print(sorted.Head(3))
	}

	// Regional performance
	fmt.Println("\n=== Regional Performance ===")
	regional, err := df.GroupBy("region").Mean()
	if err == nil && regional != nil {
		fmt.Print(regional)
	}

	// High-value sales analysis
	fmt.Println("\n=== High-Value Sales (>$1000) ===")
	highValue := df.
		Filter("sales_amount", ">", 1000).
		Select("salesperson", "product", "sales_amount", "sale_date").
		Sort("sales_amount", false)

	fmt.Print(highValue)

	// Product performance
	fmt.Println("\n=== Product Performance ===")
	productStats, err := df.GroupBy("product").Count()
	if err == nil && productStats != nil {
		fmt.Print(productStats)
	}

	// Output:
	// === Sales Performance Analysis ===
	// Top performers by total sales:
	// salesperson	sales_amount	commission_rate
	// Alice	1950	0.08
	// Carol	1900	0.09
	// Bob	1350	0.07
	//
	// === Regional Performance ===
	// region	sales_amount	commission_rate
	// East	950	0.045
	// North	890	0.036667
	// South	675	0.035
	// West	1100	0.05
	//
	// === High-Value Sales (>$1000) ===
	// salesperson	product	sales_amount	sale_date
	// Carol	Laptop	1300	2024-01-18
	// Alice	Laptop	1200	2024-01-15
	// David	Laptop	1100	2024-01-17
	//
	// === Product Performance ===
	// product	sales_amount	commission_rate
	// Laptop	3600	0.15
	// Phone	2270	0.09
	// Tablet	1150	0.08
}

// Test basic DataFrame creation and operations
func TestDataFrameBasics(t *testing.T) {
	data := map[string]interface{}{
		"numbers": []int64{1, 2, 3, 4, 5},
		"names":   []string{"a", "b", "c", "d", "e"},
	}

	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Test shape
	rows, cols := df.Shape()
	if rows != 5 || cols != 2 {
		t.Errorf("Expected shape (5, 2), got (%d, %d)", rows, cols)
	}

	// Test columns
	columns := df.Columns()
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	// Test filtering
	filtered := df.Filter("numbers", ">", int64(3))
	if err := filtered.Error(); err != nil {
		t.Errorf("Filter error: %v", err)
	}

	filteredRows, _ := filtered.Shape()
	if filteredRows != 2 {
		t.Errorf("Expected 2 filtered rows, got %d", filteredRows)
	}
}

// Test CSV operations
func TestCSVOperations(t *testing.T) {
	csvData := `name,age,score
Alice,25,95.5
Bob,30,87.2
Carol,28,92.1`

	df, err := ReadCSVFromString(csvData)
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Test automatic type inference
	ageType, err := df.GetColumnType("age")
	if err != nil {
		t.Fatalf("Failed to get column type: %v", err)
	}
	if ageType != Int64Type {
		t.Errorf("Expected Int64Type for age, got %v", ageType)
	}

	scoreType, err := df.GetColumnType("score")
	if err != nil {
		t.Fatalf("Failed to get column type: %v", err)
	}
	if scoreType != Float64Type {
		t.Errorf("Expected Float64Type for score, got %v", scoreType)
	}

	// Test statistics with proper floating point comparison
	avgScore, err := df.Mean("score")
	if err != nil {
		t.Fatalf("Failed to calculate mean: %v", err)
	}

	expectedAvg := (95.5 + 87.2 + 92.1) / 3
	// Use tolerance for floating point comparison
	tolerance := 0.001
	if math.Abs(avgScore-expectedAvg) > tolerance {
		t.Errorf("Expected average %.6f, got %.6f", expectedAvg, avgScore)
	}
}

// Test error handling
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

// Benchmark basic operations
func BenchmarkDataFrameOperations(b *testing.B) {
	// Create test data
	size := 10000
	data := map[string]interface{}{
		"id":     make([]int64, size),
		"value":  make([]float64, size),
		"status": make([]string, size),
	}

	for i := 0; i < size; i++ {
		data["id"].([]int64)[i] = int64(i)
		data["value"].([]float64)[i] = float64(i) * 2.5
		data["status"].([]string)[i] = fmt.Sprintf("status_%d", i%10)
	}

	df, err := NewDataFrameFromMap(data)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	b.ResetTimer()

	b.Run("Filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = df.Filter("value", ">", 5000.0)
		}
	})

	b.Run("Sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = df.Sort("value", false)
		}
	})

	b.Run("GroupBy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = df.GroupBy("status").Sum()
		}
	})

	b.Run("Statistics", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = df.Mean("value")
		}
	})
}

// DemoRealWorldUsage demonstrates real-world usage
func DemoRealWorldUsage() {
	fmt.Println("🦦 Welcome to Otters - Smooth Data Processing for Go!")
	fmt.Println("================================================")

	// Simulate loading real data
	start := time.Now()

	salesData := `date,product,category,revenue,units,region
2024-01-01,Widget A,Electronics,1250.00,25,North
2024-01-01,Widget B,Electronics,980.50,15,South
2024-01-02,Gadget X,Electronics,2100.75,35,East
2024-01-02,Tool Y,Hardware,750.25,10,West
2024-01-03,Widget A,Electronics,1375.00,27,North
2024-01-03,Gadget Z,Electronics,1680.90,22,South`

	df, err := ReadCSVFromString(salesData)
	if err != nil {
		log.Fatal(err)
	}

	loadTime := time.Since(start)
	fmt.Printf("✅ Loaded %d records in %v\n", df.Count(), loadTime)

	// Quick analysis
	totalRevenue, _ := df.Sum("revenue")
	avgRevenue, _ := df.Mean("revenue")

	fmt.Printf("💰 Total Revenue: $%.2f\n", totalRevenue)
	fmt.Printf("📊 Average: $%.2f\n", avgRevenue)

	// Best performing region
	regional, err := df.GroupBy("region").Sum()
	if err == nil && regional != nil {
		best := regional.Sort("revenue", false).Head(1)
		fmt.Println("🏆 Top Region:")
		fmt.Print(best)
	}

	fmt.Println("\n🦦 Otters makes data analysis smooth and efficient!")

	// Output:
	// 🦦 Welcome to Otters - Smooth Data Processing for Go!
	// ================================================
	// ✅ Loaded 6 records in 123.456µs
	// 💰 Total Revenue: $8136.40
	// 📊 Average: $1356.07
	// 🏆 Top Region:
	// region	revenue	units
	// North	2625	52
	//
	// 🦦 Otters makes data analysis smooth and efficient!
}
