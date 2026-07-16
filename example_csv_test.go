package otters

import (
	"fmt"
	"log"
	"os"
)

// Example_csvOperations demonstrates CSV file operations
func Example_csvOperations() {
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
	//     date: time
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
