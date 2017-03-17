package yacc

import (
	"fmt"
)

// This is required by the 'go generate' command
//go:generate goyacc -v y.output -o test.go test.y


// See: Test.go for structures / types / interfaces
// This type implements the yyLexer interface, and passed to yyParse
type TestLex struct {
	s string
	pos int
}

// NOTE: if -p prefix is given to the go:generate command
//       then yy will become the prefix. i.e. -p test => testSymType
func (l *TestLex) Lex(lval *yySymType) int {

	// position in the input
	if l.pos == len(l.s) {
		// ... return '0' if we have reached end of input
		return 0
	}

	// grab the byte at current position
	b := l.s[l.pos]
	fmt.Printf("l.s[%d] == %q\n", l.pos, b)
	// valid X bytes
	if b == 'x' || b == 'X' {
		// Set the byte value
		lval.b = int('x')
		// move the lexer to the next position in the input string
		l.pos++
		// return the associated token
		return X
	} else {
		return -1
	}
}

// Required as part of the yyLexer interface
func (l *TestLex) Error(s string) {
	fmt.Printf("error: %s\n", s)
}

func Start() {
	fmt.Println("yacc test run")

	// extra verbosity and debugging
	yyDebug = 1
	yyErrorVerbose = true


	i := yyParse(&TestLex{s:"x"})

	// non-zero exit code indicates failure
	fmt.Printf("Result ? %d", i)
}