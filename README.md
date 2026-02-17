# 🦦 Otters

_Smooth, intelligent data processing for Go._

Otters is a high-performance DataFrame library for Go, inspired by Pandas but designed for Go's strengths: type safety, performance, and simplicity.

[![Go Version](https://img.shields.io/badge/go-1.19+-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/datumbrain/otters)](https://goreportcard.com/report/github.com/datumbrain/otters)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## ✨ Features

- 🎯 **Type-safe** - Native Go types (int64, float64, string, bool, time)
- ⚡ **High performance** - Optimized for Go's strengths
- 🛡️ **Memory safe** - No shared slices, proper error handling
- 🐍 **Pandas-like API** - Familiar for data scientists
- 🌊 **Fluent interface** - Chain operations naturally
- 📁 **CSV support** - Read/write with automatic type inference
- 🔍 **Rich operations** - Filter, sort, select, group, join
- 📊 **Built-in statistics** - Sum, mean, std, describe, and more

## 🚀 Quick Start

### Installation

```bash
go get github.com/datumbrain/otters
```

### Performance Benchmarks

```raw
goos: darwin
goarch: arm64
pkg: github.com/datumbrain/otters
cpu: Apple M2 Pro
BenchmarkDataFrameOperations/Filter-10         	   14218	     86220 ns/op	  482570 B/op	      20 allocs/op
BenchmarkDataFrameOperations/Sort-10           	    3201	    353629 ns/op	  570477 B/op	   20039 allocs/op
BenchmarkDataFrameOperations/GroupBy-10        	    1492	    788746 ns/op	  654403 B/op	   30154 allocs/op
BenchmarkDataFrameOperations/Statistics-10     	   12099	     97350 ns/op	   79992 B/op	    9999 allocs/op
PASS
ok  	github.com/datumbrain/otters	7.016s
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    "github.com/datumbrain/otters"
)

func main() {
    // Read CSV with automatic type inference
    df, err := otters.ReadCSV("sales.csv")
    if err != nil {
        log.Fatal(err)
    }

    // Chain operations like Pandas
    result := df.
        Filter("amount", ">", 1000).
        Select("region", "amount", "product").
        Sort("amount", false) // descending

    if err := result.Error(); err != nil {
        log.Fatal(err)
    }

    // Get insights
    totalSales, _ := result.Sum("amount")
    avgDeal, _ := result.Mean("amount")
    fmt.Printf("Total sales: $%.2f\n", totalSales)
    fmt.Printf("Average deal: $%.2f\n", avgDeal)
    fmt.Printf("Top deals: %d\n", result.Count())

    // Save results
    err = result.WriteCSV("top_sales.csv")
    if err != nil {
        log.Fatal(err)
    }
}
```

## 📊 Examples

### Data Exploration

```go
// Load and explore data
df, _ := otters.ReadCSV("employees.csv")

// Basic info
fmt.Println("Shape:", df.Shape())        // (1000, 5)
fmt.Println("Columns:", df.Columns())   // [name, age, department, salary, hired_date]

// Quick look
fmt.Println(df.Head(5))   // First 5 rows
fmt.Println(df.Tail(3))   // Last 3 rows
fmt.Println(df.Describe()) // Summary statistics
```

### Filtering and Selection

```go
// Multiple filters
high_earners := df.
    Filter("salary", ">", 75000).
    Filter("department", "==", "Engineering").
    Filter("age", "<=", 35)

// Select specific columns
summary := high_earners.Select("name", "salary", "age")

// Complex conditions
experienced := df.Filter("age", ">=", 30).Filter("salary", ">", 60000)
```

### Sorting and Ranking

```go
// Sort by single column
top_paid := df.Sort("salary", false) // descending

// Multi-column sort
ranked := df.SortBy(
    []string{"department", "salary"},
    []bool{true, false}, // department ascending, salary descending
)
```

### Aggregations and Statistics

```go
// Basic statistics
avgSalary, _ := df.Mean("salary")
totalPayroll, _ := df.Sum("salary")
minSalary, _ := df.Min("salary")
maxSalary, _ := df.Max("salary")
stdDev, _ := df.Std("salary")

fmt.Printf("Average salary: $%.2f\n", avgSalary)
fmt.Printf("Total payroll: $%.2f\n", totalPayroll)
fmt.Printf("Salary range: $%.2f - $%.2f\n", minSalary, maxSalary)
fmt.Printf("Std deviation: $%.2f\n", stdDev)

// Summary statistics for all numeric columns
summary, _ := df.Describe()
fmt.Println(summary)
```

### Data Transformation

```go
// Create new columns
df_with_bonus := df.Copy()
// Add 10% bonus calculation (implementation coming soon)

// Rename columns
clean_df := df.RenameColumn("hired_date", "start_date")

// Drop columns
essential := df.Drop("internal_id", "notes")
```

## 🏗️ API Reference

### DataFrame Creation

```go
// From CSV
df, err := otters.ReadCSV("data.csv")
df, err := otters.ReadCSVWithOptions("data.csv", otters.CSVOptions{
    HasHeader: true,
    Delimiter: ',',
    SkipRows:  1,
})

// From data
df, err := otters.NewDataFrameFromMap(map[string]interface{}{
    "name":   []string{"Alice", "Bob", "Carol"},
    "age":    []int64{25, 30, 35},
    "salary": []float64{50000, 60000, 70000},
})
```

### Data Operations

```go
// Filtering
df.Filter("column", "==", value)    // Equal
df.Filter("column", "!=", value)    // Not equal
df.Filter("column", ">", value)     // Greater than
df.Filter("column", ">=", value)    // Greater than or equal
df.Filter("column", "<", value)     // Less than
df.Filter("column", "<=", value)    // Less than or equal

// Selection
df.Select("col1", "col2", "col3")   // Select columns
df.Drop("col1", "col2")             // Drop columns

// Sorting
df.Sort("column", true)             // Single column, ascending
df.Sort("column", false)            // Single column, descending
df.SortBy([]string{"col1", "col2"}, []bool{true, false})
```

### Statistics

```go
// Basic stats
df.Count()                    // Number of rows
sum, _ := df.Sum("column")    // Sum of numeric column
mean, _ := df.Mean("column")  // Average of numeric column
min, _ := df.Min("column")    // Minimum value
max, _ := df.Max("column")    // Maximum value
std, _ := df.Std("column")    // Standard deviation

// Summary
summary, _ := df.Describe()   // Summary statistics for all numeric columns
```

### I/O Operations

```go
// CSV
df, err := otters.ReadCSV("input.csv")
err = df.WriteCSV("output.csv")

// With options
df, err := otters.ReadCSVWithOptions("data.csv", otters.CSVOptions{
    HasHeader: true,
    Delimiter: '\t',
    SkipRows:  2,
    MaxRows:   1000,
})
```

## 🎯 Design Philosophy

### Pandas-Inspired, Go-Optimized

Otters brings the familiar Pandas API to Go while embracing Go's strengths:

- **Type Safety**: No more runtime type errors
- **Performance**: Optimized for Go's memory model
- **Simplicity**: Clean, readable code
- **Error Handling**: Proper Go error handling patterns

### Memory Safety

Unlike many DataFrame libraries, Otters ensures:

- No shared underlying slices
- Proper deep copying when needed
- No data races in concurrent usage
- Explicit error handling, no panics

### Performance First

- Type-specific operations for maximum speed
- Minimal allocations and copying
- Efficient sorting and filtering algorithms
- Memory-conscious design for large datasets

## 🔄 Pandas Migration

Coming from Pandas? Here's how Otters compares:

| Pandas                | Otters                      | Notes                    |
| --------------------- | --------------------------- | ------------------------ |
| `pd.read_csv()`       | `otters.ReadCSV()`          | Automatic type inference |
| `df.head()`           | `df.Head(5)`                | Must specify count       |
| `df[df.age > 25]`     | `df.Filter("age", ">", 25)` | Explicit syntax          |
| `df[['name', 'age']]` | `df.Select("name", "age")`  | Method-based selection   |
| `df.sort_values()`    | `df.Sort("column", true)`   | Simple sort syntax       |
| `df.describe()`       | `df.Describe()`             | Similar functionality    |

## 🚧 Roadmap

### ✅ MVP (Current)

- [x] Core DataFrame with type safety
- [x] CSV I/O with type inference
- [x] Basic operations (filter, select, sort)
- [x] Essential statistics
- [x] Fluent API with error handling

### 🔄 Coming Soon

- [ ] GroupBy operations
- [ ] Join operations (inner, left, right, outer)
- [ ] More file formats (JSON, Parquet)
- [ ] Advanced statistics
- [ ] Data visualization helpers
- [ ] Streaming operations for large files

### 🎯 Future

- [ ] SQL-like query interface
- [ ] Integration with popular Go ML libraries
- [ ] Advanced time series operations
- [ ] Distributed processing capabilities

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
git clone https://github.com/datumbrain/otters.git
cd otters
go mod tidy
go test ./...
```

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [Pandas](https://pandas.pydata.org/) for the API design
- Built for the Go community with ❤️

> Like an otter in water - smooth, efficient, and playful with data. 🦦

[![Made with ❤️ by Datum Brain](https://img.shields.io/badge/made%20with%20❤️%20by-Datum%20Brain-blue)](https://github.com/datumbrain)
