package otters

import (
	"fmt"
	"log"
)

// Example_filtering demonstrates filtering and selection operations
func Example_filtering() {
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
	// === Department Averages (experienced employees) ===
	// department	salary	experience
	// Engineering	75000	4
	// Marketing	65000	3
}
