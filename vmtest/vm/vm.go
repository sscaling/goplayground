package vm

import (
	"fmt"
)

// Based on: http://bartoszsypytkowski.com/simple-virtual-machine/

// Needs
// * Byte code format (format defined)
// * Instruction Pointer - op code to invoke
// * Stack - local store for holding data
// * Stack Pointer - Pointer to the top of the stack
// * Heap - dynamic memory allocated at run-time
// * Frame pointer - The current stack frame being executed.
//     e.g. function, would have function address and arguments (normally function return address pushed first)

const STACK_SIZE int = 100

type vm struct {
	locals []int // local scoped data
	code   []int // array od byte codes to be executed
	stack  []int // virtual stack
	pc     int   // program counter (aka. IP - instruction pointer)
	sp     int   // stack pointer
	fp     int   // frame pointer (for local scope)
}

func New(code []int, pc int, datasize int) *vm {
	return &vm{
		locals: make([]int, datasize),
		code:   code,
		stack:  make([]int, STACK_SIZE),
		pc:     pc,
		sp:     -1,
		fp:     0,
	}
}

// #define PUSH(vm, v) vm->stack[++vm->sp] = v // push value on top of the stack
func (machine *vm) StackPush(value int) {
	machine.sp++
	machine.stack[machine.sp] = value
}

//#define POP(vm)     vm->stack[vm->sp--]     // pop value from top of the stack
func (machine *vm) StackPop() int {
	defer func() {
		// cleanup the stack
		machine.stack[machine.sp] = 0
		machine.sp--
	}()

	return machine.stack[machine.sp]
}

//#define NCODE(vm)   vm->code[vm->pc++]      // get next bytecode
func (machine *vm) Next() int {
	defer func() {
		machine.pc++
	}()

	if machine.pc < len(machine.code) {
		return machine.code[machine.pc]
	} else {
		fmt.Println("End of program")
		return HALT
	}
}

var EMPTY_STACK []int = []int{}

func (machine *vm) String() string {
	stack := EMPTY_STACK
	if machine.sp > 0 && machine.sp < len(machine.stack) {
		stack = machine.stack[:machine.sp+1]
	}

	return fmt.Sprintf(" SP:%d, Stack:%v, PC:%d, Prog:%v", machine.sp, stack, machine.pc-1, machine.code)
}

func (machine *vm) Run() {
	for {
		code := machine.Next()

		fmt.Println(machine)

		switch code {
		case CONST_I32:
			value := machine.Next()
			machine.StackPush(value)
		case ADD_I32:
			a := machine.StackPop()
			b := machine.StackPop()
			machine.StackPush(a + b)
		case SUB_I32:
			b := machine.StackPop()
			a := machine.StackPop()
			machine.StackPush(a - b)
		case MUL_I32:
			a := machine.StackPop()
			b := machine.StackPop()
			machine.StackPush(a * b)
		case LT_I32:
			b := machine.StackPop()
			a := machine.StackPop()
			if a < b {
				machine.StackPush(1)
			} else {
				machine.StackPush(0)
			}
		case EQ_I32:
			a := machine.StackPop()
			b := machine.StackPop()
			if a == b {
				machine.StackPush(1)
			} else {
				machine.StackPush(0)
			}
		case JMP:
			machine.pc = machine.Next()

		case JMPF: // jump if false
			addr := machine.Next()
			value := machine.StackPop()

			if value == 0 {
				machine.pc = addr
			}
		case JMPT: // jump if true
			addr := machine.Next()
			value := machine.StackPop()

			if value == 1 {
				machine.pc = addr
			}
		case GLOAD:
			addr := machine.StackPop()
			value := machine.locals[addr] // read from global memory
			machine.StackPush(value)
		case GSTORE:
			value := machine.StackPop()
			addr := machine.Next()
			machine.locals[addr] = value // store in global memory
		case STORE:
			value := machine.StackPop()
			offset := machine.Next()
			machine.locals[machine.fp+offset] = value
		case LOAD:
			offset := machine.Next()
			machine.StackPush(machine.stack[machine.fp+offset])
		case CALL:
			addr := machine.Next()
			argc := machine.Next()

			machine.StackPush(argc)       // number of args
			machine.StackPush(machine.fp) // function pointer
			machine.StackPush(machine.pc) // program counter
			machine.fp = machine.sp       // frame pointer points to bottom of stack for this frame
			machine.pc = addr             // program counter jumps to function
		case RET:
			rval := machine.StackPop() // should contain the return value

			machine.sp = machine.fp         // should return sp to the stack as it was at the start of the frame
			machine.pc = machine.StackPop() // previous program counter
			machine.fp = machine.StackPop() // restore previous fp

			argc := machine.StackPop()

			for argc > 0 {
				machine.StackPop() // pop arguments off stack
				argc--
			}

			machine.StackPush(rval)

		case POP:
			machine.StackPop()
		case PRINT:
			value := machine.StackPop() // pop value from top of the stack ...
			fmt.Printf("%d\n", value)   // ... and print it
		case HALT:
			fmt.Println("Halting")
			return
		default:
			fmt.Printf("ERROR: invalid opcode '%d'\n", code)
		}
	}
}
