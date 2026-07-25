package otters

import (
	"fmt"
)

// Example_join demonstrates combining two DataFrames on a key column.
func Example_join() {
	orderID, _ := NewSeries("order_id", []int64{1, 2, 3})
	orderCust, _ := NewSeries("customer_id", []int64{10, 20, 30})
	amount, _ := NewSeries("amount", []float64{250, 125.5, 300})
	orders, _ := NewDataFrameFromSeries(orderID, orderCust, amount)

	custID, _ := NewSeries("customer_id", []int64{10, 20, 40})
	name, _ := NewSeries("name", []string{"Alice", "Bob", "Carol"})
	customers, _ := NewDataFrameFromSeries(custID, name)

	// Inner join: only orders with a matching customer survive.
	matched := orders.Join(customers, []string{"customer_id"}, InnerJoin)
	if matched.Error() != nil {
		fmt.Println("join failed:", matched.Error())
		return
	}
	fmt.Print(matched)

	// Left join keeps every order; unmatched rows zero-fill customer columns.
	all := orders.Join(customers, []string{"customer_id"}, LeftJoin)
	rows, cols := all.Shape()
	fmt.Printf("left join keeps all orders: %d rows, %d columns\n", rows, cols)

	// Output:
	// order_id	customer_id	amount	name
	// 1	10	250	Alice
	// 2	20	125.5	Bob
	// left join keeps all orders: 3 rows, 4 columns
}

// ExampleDataFrame_JoinOn joins frames whose key columns are named differently.
func ExampleDataFrame_JoinOn() {
	empID, _ := NewSeries("emp", []string{"ann", "bob"})
	deptRef, _ := NewSeries("dept_code", []string{"ENG", "OPS"})
	employees, _ := NewDataFrameFromSeries(empID, deptRef)

	code, _ := NewSeries("code", []string{"ENG", "OPS"})
	deptName, _ := NewSeries("dept_name", []string{"Engineering", "Operations"})
	departments, _ := NewDataFrameFromSeries(code, deptName)

	res := employees.JoinOn(departments, []string{"dept_code"}, []string{"code"}, InnerJoin)
	fmt.Print(res)

	// Output:
	// emp	dept_code	dept_name
	// ann	ENG	Engineering
	// bob	OPS	Operations
}
