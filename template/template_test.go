package template

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"net"
	"testing"
)

func TestTemplate(t *testing.T) {
	ln, _ := net.Listen("tcp", ":8080")

	conn, err := ln.Accept()
	if err != nil {
		// handle error
		fmt.Printf("Error %s\n", err)
		return
	}
	handleConnection(conn)

	fmt.Println("end")
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	b := new(bytes.Buffer)
	renderTemplate(b)

	conn.Write(b.Bytes())
}

func renderTemplate(w io.Writer) {
	const tpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>{{.Title}}</title>
	</head>
	<body>
		<p>{{.Body}}</p>
	</body>
</html>`

	t, _ := template.New("webpage").Parse(tpl)

	data := struct {
		Title string
		Body  string
	}{
		Title: "a-title",
		Body:  "a-body",
	}

	t.Execute(w, data)
}
