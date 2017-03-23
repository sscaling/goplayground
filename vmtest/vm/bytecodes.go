package vm

// Bytecodes
const (
	ADD_I32   = 1  // int add
	SUB_I32   = 2  // int sub
	MUL_I32   = 3  // int mul
	LT_I32    = 4  // int less than
	EQ_I32    = 5  // int equal
	JMP       = 6  // branch
	JMPT      = 7  // branch if true
	JMPF      = 8  // branch if false
	CONST_I32 = 9  // push constant integer
	LOAD      = 10 // load from local
	GLOAD     = 11 // load from global
	STORE     = 12 // store in local
	GSTORE    = 13 // store in global memory
	PRINT     = 14 // print value on top of the stack
	POP       = 15 // throw away top of the stack
	HALT      = 16 // stop program
	CALL      = 17 // call procedure
	RET       = 18 // return from procedure
)
