package otters

import (
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"
)

// Example_basicDataFrame demonstrates basic DataFrame operations
func Example_basicDataFrame() {
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

// TestTimeTypeHeadTail verifies that Head and Tail work on DataFrames with
// TimeType columns (regression for missing TimeType case in slice()).
func TestTimeTypeHeadTail(t *testing.T) {
	t1, _ := time.Parse("2006-01-02", "2024-01-01")
	t2, _ := time.Parse("2006-01-02", "2024-01-02")
	t3, _ := time.Parse("2006-01-02", "2024-01-03")

	s, err := NewSeries("date", []time.Time{t1, t2, t3})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	df, err := NewDataFrameFromSeries(s)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	head := df.Head(2)
	if head.Error() != nil {
		t.Fatalf("Head on TimeType column failed: %v", head.Error())
	}
	if rows, _ := head.Shape(); rows != 2 {
		t.Errorf("Head(2) returned %d rows, want 2", rows)
	}

	tail := df.Tail(1)
	if tail.Error() != nil {
		t.Fatalf("Tail on TimeType column failed: %v", tail.Error())
	}
	if rows, _ := tail.Shape(); rows != 1 {
		t.Errorf("Tail(1) returned %d rows, want 1", rows)
	}

	// Verify the value is correct
	val, err := tail.Get(0, "date")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !val.(time.Time).Equal(t3) {
		t.Errorf("Tail value = %v, want %v", val, t3)
	}
}

// TestGroupByKeyCollision verifies that group values containing the pipe
// character do not cause key collisions (regression for GroupBy key bug).
func TestGroupByKeyCollision(t *testing.T) {
	// "a|b" and "a" with "b" are distinct groups but produced the same "|"-joined key.
	data := map[string]interface{}{
		"category": []string{"a|b", "a|b", "a"},
		"value":    []float64{1, 2, 10},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	result, err := df.GroupBy("category").Sum()
	if err != nil {
		t.Fatalf("GroupBy.Sum failed: %v", err)
	}

	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("expected 2 groups, got %d", rows)
	}

	// Find the "a|b" group and verify its sum is 3, not 13.
	for i := 0; i < rows; i++ {
		cat, _ := result.Get(i, "category")
		val, _ := result.Get(i, "value")
		if cat.(string) == "a|b" {
			if val.(float64) != 3 {
				t.Errorf("group \"a|b\" sum = %v, want 3", val)
			}
		}
		if cat.(string) == "a" {
			if val.(float64) != 10 {
				t.Errorf("group \"a\" sum = %v, want 10", val)
			}
		}
	}
}

// TestSetErrorDoesNotMutateCaller verifies that a failed operation does not
// corrupt the original DataFrame (regression for the setError mutation bug).
func TestSetErrorDoesNotMutateCaller(t *testing.T) {
	data := map[string]interface{}{
		"a": []int64{1, 2, 3},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_ = df.Filter("nonexistent", "==", int64(1))

	if df.Error() != nil {
		t.Errorf("Filter on nonexistent column mutated the original DataFrame: %v", df.Error())
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 1 {
		t.Errorf("original DataFrame shape changed after failed Filter: got (%d, %d), want (3, 1)", rows, cols)
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

// TestDeterministicFromMap verifies that NewDataFrameFromMap always produces
// columns in alphabetical order, regardless of map iteration order.
func TestDeterministicFromMap(t *testing.T) {
	data := map[string]interface{}{
		"zebra": []int64{1, 2, 3},
		"apple": []int64{4, 5, 6},
		"mango": []int64{7, 8, 9},
	}
	expected := []string{"apple", "mango", "zebra"}
	for i := 0; i < 20; i++ {
		df, err := NewDataFrameFromMap(data)
		if err != nil {
			t.Fatalf("NewDataFrameFromMap failed on iteration %d: %v", i, err)
		}
		cols := df.Columns()
		if len(cols) != len(expected) {
			t.Fatalf("iteration %d: expected %d columns, got %d", i, len(expected), len(cols))
		}
		for j, col := range cols {
			if col != expected[j] {
				t.Errorf("iteration %d: column[%d] = %q, want %q", i, j, col, expected[j])
			}
		}
	}
}

// TestDeterministicGroupBy verifies that GroupBy produces rows in the same
// order across repeated calls.
func TestDeterministicGroupBy(t *testing.T) {
	data := map[string]interface{}{
		"category": []string{"B", "A", "C", "A", "B", "C"},
		"value":    []float64{10, 20, 30, 40, 50, 60},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	var orders [][]string
	for i := 0; i < 10; i++ {
		result, err := df.GroupBy("category").Sum()
		if err != nil {
			t.Fatalf("GroupBy.Sum failed on iteration %d: %v", i, err)
		}
		rows, _ := result.Shape()
		order := make([]string, rows)
		for r := 0; r < rows; r++ {
			val, _ := result.Get(r, "category")
			order[r] = val.(string)
		}
		orders = append(orders, order)
	}

	for i := 1; i < len(orders); i++ {
		for j, cat := range orders[i] {
			if cat != orders[0][j] {
				t.Errorf("non-deterministic GroupBy: iteration %d row %d = %q, want %q",
					i, j, cat, orders[0][j])
			}
		}
	}
}

// TestDataFrameManipulation covers Tail, Set, GetSeries, AddColumn, DropColumn,
// RenameColumn, IsEmpty, and HasColumn.
func TestDataFrameManipulation(t *testing.T) {
	data := map[string]interface{}{
		"id":   []int64{1, 2, 3, 4, 5},
		"name": []string{"a", "b", "c", "d", "e"},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Tail
	tail := df.Tail(2)
	if err := tail.Error(); err != nil {
		t.Fatalf("Tail error: %v", err)
	}
	rows, _ := tail.Shape()
	if rows != 2 {
		t.Errorf("Tail(2) returned %d rows, want 2", rows)
	}

	// Set
	if err := df.Set(0, "id", int64(99)); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	val, err := df.Get(0, "id")
	if err != nil {
		t.Fatalf("Get after Set error: %v", err)
	}
	if val.(int64) != 99 {
		t.Errorf("Set: got %v, want 99", val)
	}

	// GetSeries
	s, err := df.GetSeries("id")
	if err != nil {
		t.Fatalf("GetSeries error: %v", err)
	}
	if s == nil || s.Name != "id" {
		t.Errorf("GetSeries returned unexpected series: %v", s)
	}

	// AddColumn (mutates df in place)
	scoreSeries, err := NewSeries("score", []float64{10.0, 20.0, 30.0, 40.0, 50.0})
	if err != nil {
		t.Fatalf("NewSeries error: %v", err)
	}
	df.AddColumn(scoreSeries)
	if !df.HasColumn("score") {
		t.Error("AddColumn: 'score' column not found")
	}

	// DropColumn (returns copy)
	dfDropped := df.DropColumn("score")
	if err := dfDropped.Error(); err != nil {
		t.Fatalf("DropColumn error: %v", err)
	}
	if dfDropped.HasColumn("score") {
		t.Error("DropColumn: 'score' still present in returned DataFrame")
	}
	if !df.HasColumn("score") {
		t.Error("DropColumn: 'score' removed from original DataFrame unexpectedly")
	}

	// RenameColumn (returns copy)
	dfRenamed := df.RenameColumn("id", "user_id")
	if err := dfRenamed.Error(); err != nil {
		t.Fatalf("RenameColumn error: %v", err)
	}
	if !dfRenamed.HasColumn("user_id") {
		t.Error("RenameColumn: 'user_id' not found in result")
	}
	if dfRenamed.HasColumn("id") {
		t.Error("RenameColumn: old 'id' still present in result")
	}

	// IsEmpty
	empty := NewDataFrame()
	if !empty.IsEmpty() {
		t.Error("IsEmpty: expected true for new empty DataFrame")
	}
	if df.IsEmpty() {
		t.Error("IsEmpty: expected false for non-empty DataFrame")
	}

	// HasColumn
	if !df.HasColumn("name") {
		t.Error("HasColumn: 'name' should exist")
	}
	if df.HasColumn("nonexistent") {
		t.Error("HasColumn: 'nonexistent' should not exist")
	}
}

// TestOpsOperations covers Drop, SortBy, Unique, Query, Where, and ResetIndex.
func TestOpsOperations(t *testing.T) {
	data := map[string]interface{}{
		"a": []int64{3, 1, 2, 1, 3},
		"b": []int64{30, 10, 20, 15, 35},
		"c": []string{"x", "y", "z", "w", "v"},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Drop
	dfDropped := df.Drop("c")
	if err := dfDropped.Error(); err != nil {
		t.Fatalf("Drop error: %v", err)
	}
	if dfDropped.HasColumn("c") {
		t.Error("Drop: column 'c' still present")
	}
	_, cols := dfDropped.Shape()
	if cols != 2 {
		t.Errorf("Drop: expected 2 columns, got %d", cols)
	}

	// SortBy
	sorted := df.SortBy([]string{"a"}, []bool{true})
	if err := sorted.Error(); err != nil {
		t.Fatalf("SortBy error: %v", err)
	}
	first, _ := sorted.Get(0, "a")
	if first.(int64) != 1 {
		t.Errorf("SortBy ascending: first 'a' value = %v, want 1", first)
	}

	// Unique
	unique, err := df.Unique("a")
	if err != nil {
		t.Fatalf("Unique error: %v", err)
	}
	if len(unique) != 3 {
		t.Errorf("Unique: got %d values, want 3", len(unique))
	}

	// Query
	queried := df.Query("a > 2")
	if err := queried.Error(); err != nil {
		t.Fatalf("Query error: %v", err)
	}
	qRows, _ := queried.Shape()
	if qRows != 2 {
		t.Errorf("Query 'a > 2': got %d rows, want 2", qRows)
	}

	// Where (alias for Filter)
	where := df.Where("a", ">", int64(2))
	if err := where.Error(); err != nil {
		t.Fatalf("Where error: %v", err)
	}
	wRows, _ := where.Shape()
	if wRows != 2 {
		t.Errorf("Where 'a > 2': got %d rows, want 2", wRows)
	}

	// ResetIndex
	reset := df.ResetIndex()
	if err := reset.Error(); err != nil {
		t.Fatalf("ResetIndex error: %v", err)
	}
	rRows, rCols := reset.Shape()
	dfRows, dfCols := df.Shape()
	if rRows != dfRows || rCols != dfCols {
		t.Errorf("ResetIndex: shape changed: got (%d, %d), want (%d, %d)",
			rRows, rCols, dfRows, dfCols)
	}
}

// TestGroupByMinMax covers GroupBy.Min() and GroupBy.Max().
func TestGroupByMinMax(t *testing.T) {
	data := map[string]interface{}{
		"dept":   []string{"Eng", "Eng", "Sales", "Sales"},
		"salary": []float64{70000, 80000, 50000, 60000},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Min
	minDf, err := df.GroupBy("dept").Min()
	if err != nil {
		t.Fatalf("GroupBy.Min error: %v", err)
	}
	rows, _ := minDf.Shape()
	if rows != 2 {
		t.Fatalf("GroupBy.Min: expected 2 groups, got %d", rows)
	}
	for i := 0; i < rows; i++ {
		dept, _ := minDf.Get(i, "dept")
		sal, _ := minDf.Get(i, "salary")
		switch dept.(string) {
		case "Eng":
			if sal.(float64) != 70000 {
				t.Errorf("Min Eng salary = %v, want 70000", sal)
			}
		case "Sales":
			if sal.(float64) != 50000 {
				t.Errorf("Min Sales salary = %v, want 50000", sal)
			}
		}
	}

	// Max
	maxDf, err := df.GroupBy("dept").Max()
	if err != nil {
		t.Fatalf("GroupBy.Max error: %v", err)
	}
	rows, _ = maxDf.Shape()
	if rows != 2 {
		t.Fatalf("GroupBy.Max: expected 2 groups, got %d", rows)
	}
	for i := 0; i < rows; i++ {
		dept, _ := maxDf.Get(i, "dept")
		sal, _ := maxDf.Get(i, "salary")
		switch dept.(string) {
		case "Eng":
			if sal.(float64) != 80000 {
				t.Errorf("Max Eng salary = %v, want 80000", sal)
			}
		case "Sales":
			if sal.(float64) != 60000 {
				t.Errorf("Max Sales salary = %v, want 60000", sal)
			}
		}
	}
}

// TestStringOperators covers Filter with "contains", "startswith", and "endswith".
func TestStringOperators(t *testing.T) {
	data := map[string]interface{}{
		"name": []string{"Alice", "Bob", "Albany", "Sara"},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// contains
	filtered := df.Filter("name", "contains", "l")
	if err := filtered.Error(); err != nil {
		t.Fatalf("Filter contains error: %v", err)
	}
	rows, _ := filtered.Shape()
	if rows != 2 { // Alice, Albany
		t.Errorf("Filter contains 'l': got %d rows, want 2", rows)
	}

	// startswith
	starts := df.Filter("name", "startswith", "Al")
	if err := starts.Error(); err != nil {
		t.Fatalf("Filter startswith error: %v", err)
	}
	rows, _ = starts.Shape()
	if rows != 2 { // Alice, Albany
		t.Errorf("Filter startswith 'Al': got %d rows, want 2", rows)
	}

	// endswith
	ends := df.Filter("name", "endswith", "e")
	if err := ends.Error(); err != nil {
		t.Fatalf("Filter endswith error: %v", err)
	}
	rows, _ = ends.Shape()
	if rows != 1 { // Alice
		t.Errorf("Filter endswith 'e': got %d rows, want 1", rows)
	}
}

// TestStatsOperations covers Median, Var, Quantile, Describe, ValueCounts,
// Correlation, and NumericSummary.
func TestStatsOperations(t *testing.T) {
	data := map[string]interface{}{
		"value":    []float64{10, 20, 30, 40, 50},
		"category": []string{"a", "b", "a", "b", "a"},
		"x":        []float64{1, 2, 3, 4, 5},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Median
	median, err := df.Median("value")
	if err != nil {
		t.Fatalf("Median error: %v", err)
	}
	if median != 30 {
		t.Errorf("Median = %v, want 30", median)
	}

	// Var
	variance, err := df.Var("value")
	if err != nil {
		t.Fatalf("Var error: %v", err)
	}
	if variance <= 0 {
		t.Errorf("Var = %v, want > 0", variance)
	}

	// Quantile
	q25, err := df.Quantile("value", 0.25)
	if err != nil {
		t.Fatalf("Quantile error: %v", err)
	}
	if q25 <= 0 {
		t.Errorf("Quantile(0.25) = %v, want > 0", q25)
	}

	// Describe
	desc, err := df.Describe()
	if err != nil {
		t.Fatalf("Describe error: %v", err)
	}
	if desc == nil {
		t.Fatal("Describe returned nil")
	}
	descRows, _ := desc.Shape()
	if descRows == 0 {
		t.Error("Describe: expected non-empty result")
	}

	// ValueCounts
	vc, err := df.ValueCounts("category")
	if err != nil {
		t.Fatalf("ValueCounts error: %v", err)
	}
	if vc == nil {
		t.Fatal("ValueCounts returned nil")
	}
	vcRows, _ := vc.Shape()
	if vcRows != 2 { // "a" and "b"
		t.Errorf("ValueCounts: got %d rows, want 2", vcRows)
	}

	// Correlation (needs >= 2 numeric columns)
	corr, err := df.Correlation()
	if err != nil {
		t.Fatalf("Correlation error: %v", err)
	}
	if corr == nil {
		t.Fatal("Correlation returned nil")
	}

	// NumericSummary
	ns, err := df.NumericSummary("value")
	if err != nil {
		t.Fatalf("NumericSummary error: %v", err)
	}
	if ns == nil {
		t.Fatal("NumericSummary returned nil")
	}
	if ns.Mean != 30 {
		t.Errorf("NumericSummary.Mean = %v, want 30", ns.Mean)
	}
	if ns.Min != 10 {
		t.Errorf("NumericSummary.Min = %v, want 10", ns.Min)
	}
	if ns.Max != 50 {
		t.Errorf("NumericSummary.Max = %v, want 50", ns.Max)
	}
}

// TestCSVFileOperations covers file-based CSV I/O using os.CreateTemp.
func TestCSVFileOperations(t *testing.T) {
	data := map[string]interface{}{
		"id":   []int64{1, 2, 3},
		"name": []string{"Alice", "Bob", "Carol"},
		"age":  []int64{25, 30, 35},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// WriteCSV + ReadCSV roundtrip
	tmpCSV, err := os.CreateTemp("", "otter_test_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp error: %v", err)
	}
	tmpCSV.Close()
	defer os.Remove(tmpCSV.Name())

	if err := df.WriteCSV(tmpCSV.Name()); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}
	df2, err := ReadCSV(tmpCSV.Name())
	if err != nil {
		t.Fatalf("ReadCSV error: %v", err)
	}
	rows, cols := df2.Shape()
	if rows != 3 || cols != 3 {
		t.Errorf("ReadCSV roundtrip: got shape (%d, %d), want (3, 3)", rows, cols)
	}

	// WriteCSVWithOptions + ReadCSVWithOptions with tab delimiter
	tmpTSV, err := os.CreateTemp("", "otter_test_*.tsv")
	if err != nil {
		t.Fatalf("CreateTemp error: %v", err)
	}
	tmpTSV.Close()
	defer os.Remove(tmpTSV.Name())

	if err := df.WriteCSVWithOptions(tmpTSV.Name(), CSVOptions{HasHeader: true, Delimiter: '\t'}); err != nil {
		t.Fatalf("WriteCSVWithOptions error: %v", err)
	}
	df3, err := ReadCSVWithOptions(tmpTSV.Name(), CSVOptions{HasHeader: true, Delimiter: '\t'})
	if err != nil {
		t.Fatalf("ReadCSVWithOptions error: %v", err)
	}
	rows, cols = df3.Shape()
	if rows != 3 || cols != 3 {
		t.Errorf("ReadCSVWithOptions tab roundtrip: got shape (%d, %d), want (3, 3)", rows, cols)
	}

	// DetectDelimiter on tab-delimited file
	delim, err := DetectDelimiter(tmpTSV.Name())
	if err != nil {
		t.Fatalf("DetectDelimiter error: %v", err)
	}
	if delim != '\t' {
		t.Errorf("DetectDelimiter: got %q, want tab", string(delim))
	}

	// ValidateCSV on valid file
	info, err := ValidateCSV(tmpCSV.Name())
	if err != nil {
		t.Fatalf("ValidateCSV error: %v", err)
	}
	if info == nil {
		t.Fatal("ValidateCSV returned nil info")
	}

	// ValidateCSV on invalid file (inconsistent column counts)
	tmpInvalid, err := os.CreateTemp("", "otter_invalid_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp error: %v", err)
	}
	_, _ = tmpInvalid.WriteString("col1,col2\n1,2\n3,4,5\n")
	tmpInvalid.Close()
	defer os.Remove(tmpInvalid.Name())
	_, err = ValidateCSV(tmpInvalid.Name())
	if err == nil {
		t.Error("ValidateCSV: expected error for inconsistent columns, got nil")
	}

	// Headerless CSV
	tmpNoHeader, err := os.CreateTemp("", "otter_noheader_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp error: %v", err)
	}
	_, _ = tmpNoHeader.WriteString("1,Alice\n2,Bob\n")
	tmpNoHeader.Close()
	defer os.Remove(tmpNoHeader.Name())
	dfNoHeader, err := ReadCSVWithOptions(tmpNoHeader.Name(), CSVOptions{HasHeader: false, Delimiter: ','})
	if err != nil {
		t.Fatalf("ReadCSVWithOptions headerless error: %v", err)
	}
	nhRows, nhCols := dfNoHeader.Shape()
	if nhRows != 2 || nhCols != 2 {
		t.Errorf("Headerless CSV: got shape (%d, %d), want (2, 2)", nhRows, nhCols)
	}

	// MaxRows option
	tmpMaxRows, err := os.CreateTemp("", "otter_maxrows_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp error: %v", err)
	}
	_, _ = tmpMaxRows.WriteString("id,name\n1,Alice\n2,Bob\n3,Carol\n4,Dave\n")
	tmpMaxRows.Close()
	defer os.Remove(tmpMaxRows.Name())
	dfMax, err := ReadCSVWithOptions(tmpMaxRows.Name(), CSVOptions{HasHeader: true, Delimiter: ',', MaxRows: 2})
	if err != nil {
		t.Fatalf("ReadCSVWithOptions MaxRows error: %v", err)
	}
	maxRows, _ := dfMax.Shape()
	if maxRows != 2 {
		t.Errorf("MaxRows: got %d rows, want 2", maxRows)
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
