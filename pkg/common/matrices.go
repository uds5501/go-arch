package common

import (

)

type Matrices struct {
	a map[string]int
}

var test Matrices

// Should run at the very beginning, before any other package
// or code.
func init() {
test.a=map[string]int{
	"sushant":1,
	}
}

func GetMatrices() Matrices {
	return test
}
