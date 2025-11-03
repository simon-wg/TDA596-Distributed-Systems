package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
)

func validateFileName(path string) bool {
	validExtensions := []string{"html", "txt", "gif", "jpeg", "jpg", "css"}
	splits := strings.Split(path, ".")
	ext := splits[len(splits)-1]
	for _, v := range validExtensions {
		if ext == v {
			return true
		}
	}
	return false
}

func handler(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)
	req, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Invalid HTTP request")
		return
	}
	res := &http.Response{
		Proto: 	"HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{},
		Request: req,
	}
	// Validate file name
	if validateFileName(req.URL.Path) {
		setError(c, 400)
		writeResponse(c, res)
		return
	}
	method := req.Method
	switch method {
		case "GET": // Handle GET request
			getHandler(c, req)
			writeResponse(c, res)
			return
		case "POST": // Handle POST request
			postHandler(c, req)
			writeResponse(c, res)
			return
		default:
			setError(c, 400)
			res.Write(c)
			writeResponse(c, res)
			return
	}
}

func getHandler(c net.Conn, req *http.Request) {
	fmt.Println("GET request received")
}

func postHandler(c net.Conn, req *http.Request) {
	fmt.Println("POST request received")
}

func setError(c net.Conn, code int) {}

func writeResponse(c net.Conn, res *http.Response) {}

func main() {
	PORT := os.Args[1]
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		fmt.Println("Server failed to start")
		return
	}
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			continue
		}
		// Create goroutine and handle connection
		go handler(c)
	}
}
