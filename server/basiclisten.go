package main

import (
	"net"
	"fmt"
	"bytes"
)


func main() {
	fmt.Println("start")

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Error %s\n", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Printf("Error %s\n", err)
			return
		}
		go handleConnection(conn)
	}

	fmt.Println("end")
}
func handleConnection(conn net.Conn) {
	defer conn.Close()

	buff := bytes.NewBuffer([]byte{})

	b := make([]byte, 1024)

	i := 1024
	var err error
	for i == 1024 {
		if i, err = conn.Read(b); nil != err {
			fmt.Printf("Error: %s\n", err)
			return
		} else {
			fmt.Printf("Read %d bytes\n", i)
			for x := 0; x < i; x++ {
				buff.WriteByte(b[x])
			}
		}
	}

	fmt.Printf("Read '%s'\n", buff.String())

}