/* Declarations */

%{
package yacc

import "fmt"

%}

%union {
    b int
}

%token X

%start input

%%  /* Grammar rules below */


input : X
      {
          fmt.Println($1);
          $$ = $1;
      }
      ;


%%  /* Program */

/*
func Run() {
    fmt.Println("run")
}
*/