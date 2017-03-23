package vm

import (
	"testing"
)

func TestAdd(t *testing.T) {
	code := []int{
		CONST_I32, 1,      // push 1 onto the stack
		CONST_I32, 2,      // push 2 onto the stack
		ADD_I32, PRINT,     // add values, result should be left on stack
	}

	New(code, 0, 0).Run()
}

func TestJmpT(t *testing.T) {
	code := []int{
		CONST_I32, 1,      // push 1 onto the stack
		JMPT, 9,  // if true, jump to branch that prints 3
		CONST_I32, 2, // otherwise print 2
		PRINT, JMP, 12,  // then skip to end
		CONST_I32, 3,
		PRINT,
		HALT,
	}

	New(code, 0, 0).Run()
}

func TestFibonacci(t *testing.T) {
	fib := 0  // address of the fibonacci procedure
	prog := []int{
		// int fib(n) {
		//     if(n == 0) return 0;
		LOAD, -3,       // 0 - load last function argument N
		CONST_I32, 0,   // 2 - put 0
		EQ_I32,         // 4 - check equality: N == 0
		JMPF, 10,       // 5 - if they are NOT equal, goto 10
		CONST_I32, 0,   // 7 - otherwise put 0
		RET,            // 9 - and return it
		//     if(n < 3) return 1;
		LOAD, -3,       // 10 - load last function argument N
		CONST_I32, 3,   // 12 - put 3
		LT_I32,         // 14 - check if 3 is less than N
		JMPF, 20,       // 15 - if 3 is NOT less than N, goto 20
		CONST_I32, 1,   // 17 - otherwise put 1
		RET,            // 19 - and return it
		//     else return fib(n-1) + fib(n-2);
		LOAD, -3,       // 20 - load last function argument N
		CONST_I32, 1,   // 22 - put 1
		SUB_I32,        // 24 - calculate: N-1, result is on the stack
		CALL, fib, 1,   // 25 - call fib function with 1 arg. from the stack
		LOAD, -3,       // 28 - load N again
		CONST_I32, 2,   // 30 - put 2
		SUB_I32,        // 32 - calculate: N-2, result is on the stack
		CALL, fib, 1,   // 33 - call fib function with 1 arg. from the stack
		ADD_I32,        // 36 - since 2 fibs pushed their ret values on the stack, just add them
		RET,            // 37 - return from procedure
		// entrypoint - main function
		CONST_I32, 6,   // 38 - put 6
		CALL, fib, 1,   // 40 - call function: fib(arg) where arg = 6;
		PRINT,          // 43 - print result
		HALT,           // 44 - stop program
	};

	New(prog, 38, 0).Run()
}