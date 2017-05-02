package language

import (
	"testing"
	"fmt"
)

type Thing interface {
	String() string
}

type strt struct {
	I int
}

func (z strt) String() string {
	return "Struct"
}

type Bar strt

func (b Bar) String() string {
	return "Bar"
}

func TestRetrieveAndCast(t *testing.T) {


	x := strt{4}

	// When an interface{} is a type, then types can be cast, as long as they 
	// implement the same interface
	f := func(p interface{}) {
		if v, ok := p.(Thing); ok {
			t := p
			fmt.Printf("%v, %#v\n", t, v)
		} else {
			fmt.Printf("%v\n", ok)
		}
	}

	f((Bar)(x))
}
