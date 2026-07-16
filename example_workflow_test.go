package otters

import (
	"fmt"
	"log"
)

// Example_workflow demonstrates complex data analysis workflow
func Example_workflow() {
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
	// North	890	0.03666666666666667
	// South	675	0.035
	// West	1100	0.05
	//
	// === High-Value Sales (>$1000) ===
	// salesperson	product	sales_amount	sale_date
	// Carol	Laptop	1300	2024-01-18 00:00:00 +0000 UTC
	// Alice	Laptop	1200	2024-01-15 00:00:00 +0000 UTC
	// David	Laptop	1100	2024-01-17 00:00:00 +0000 UTC
	//
	// === Product Performance ===
	// product	count
	// Laptop	3
	// Phone	3
	// Tablet	2
}

// Example_realWorldUsage demonstrates real-world usage
func Example_realWorldUsage() {
	fmt.Println("🦦 Welcome to Otters - Smooth Data Processing for Go!")
	fmt.Println("================================================")

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

	fmt.Printf("✅ Loaded %d records\n", df.Count())

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
	// ✅ Loaded 6 records
	// 💰 Total Revenue: $8137.40
	// 📊 Average: $1356.23
	// 🏆 Top Region:
	// region	revenue	units
	// South	2661.4	37
	//
	// 🦦 Otters makes data analysis smooth and efficient!
}
