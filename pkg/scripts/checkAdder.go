package scripts

import (
	"fmt"
	"trell/go-arch/db"
)

type Operations struct {
	dbFactory db.DBFactory
}

func (o *Operations) getAddition(a int, b int) int {
	fmt.Printf("Doing operation : %d + %d\n", a, b)
	return a + b
}

func (o *Operations) getSubtraction(a int, b int) int {
	fmt.Printf("Doing operation : %d - %d\n", a, b)
	return a - b
}

func (o *Operations) getMultiplication(a int, b int) int {
	fmt.Printf("Doing operation : %d * %d\n", a, b)
	return a * b
}

func (o *Operations) ExportableFunction() {
	fmt.Println("you are at exporatble function")
}
func (o *Operations) Init() {
	fmt.Println("Welcome to init function")
	x := o.getAddition(10, 11)
	fmt.Println(x)
}

func NewOperation(factory db.DBFactory) *Operations {
	return &Operations{dbFactory: factory}
}
