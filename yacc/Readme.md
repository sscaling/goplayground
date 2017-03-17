# Go Generate YACC Test

References:
* [http://dinosaur.compilertools.net/yacc/](http://dinosaur.compilertools.net/yacc/)
* [https://godoc.org/golang.org/x/tools/cmd/goyacc](https://godoc.org/golang.org/x/tools/cmd/goyacc)
* [https://github.com/golang-samples/yacc/blob/master/simple/calc.y](https://github.com/golang-samples/yacc/blob/master/simple/calc.y)

**NOTE** [https://golang.org/cmd/yacc/](https://golang.org/cmd/yacc/) seems dead.

## Scope

Dumb go generate test using goyacc. Simple grammar that accepts one input character only 'x' or 'X'. everything else is invalid.


## Pre-requisites

    go get golang.org/x/tools/cmd/goyacc

## Generate

    go generate

## Run

    go ../main.go