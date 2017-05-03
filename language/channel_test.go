package language

import (
	"testing"
	"fmt"
	"time"
)

func TestChannels(t *testing.T) {
	a := make(chan int)
	b := make(chan int)

	// Needs to be in a separate goroutine, otherwise when
	// we try to send to one of the channels the VM will
	// know that nothing is ready to receive, so will get a deadlock warning
	go func() {
		a <- 1
		b <- 2
		time.Sleep(5 * time.Second)
	}()

	for {
		select {
		case aval := <-a:
			go fmt.Printf("a ? %d\n", aval)
		case bval := <-b:
			go fmt.Printf("b ? %d\n", bval)
		default:
			/* nothing */
			// need to have a default statement or a signal to end (done channel)
			// the for loop, otherwise all goroutines will end and the VM
			// will detect deadlock. However, this will loop forever.
		}
	}
}

func TestFibChannel(t *testing.T) {
	vals := make(chan int)
	done := make(chan bool)

	last := 0
	curr := 1
	max := 10

	loop:
	for {
		fmt.Println(curr)

		go func() {
			if max <= 0 {
				done <- true
			}
			vals <- last + curr
		}()

		select {
		case v := <-vals:
			last = curr
			curr = v
		case <-done:
			break loop
		}

		max--
	}
}