package main

import (
	"fmt"
	"github.com/sscaling/goplayground/vmtest/vm"
)

func main() {
	fmt.Println("Start")

	vm.New([]int{vm.HALT}, 0, 0).Run()

	fmt.Println("Stop")
}
