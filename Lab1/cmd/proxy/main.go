package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
)

const maxConnections = 10

// Helper function to write error to HTTP response.
func writeError(c net.Conn, res *http.Response, code int) {
	res.StatusCode = code
	res.Status = http.StatusText(code)
	res.Write(c)
}

// Starts a connection with the client's requested host.
func proxyConnection(req *http.Request) (net.Conn, error) {
	// We do this because curl removes :80 when sending request
	hostname, port, err := net.SplitHostPort(req.Host)
	if err != nil {
		hostname = req.Host
		port = "80"
	}
	destination, err := net.Dial("tcp", hostname+":"+port)
	return destination, err
}

// Checks if incoming request is a get request, as they're the only supported HTTP method.
// Error 501 if not GET method.
func getIncomingRequest(c net.Conn, res *http.Response) (*http.Request, error) {
	incomingRequest, err := http.ReadRequest(bufio.NewReader(c))
	if err != nil {
		writeError(c, res, http.StatusBadRequest)
		return nil, err
	}
	if incomingRequest.Method != "GET" {
		writeError(c, res, http.StatusNotImplemented)
		return nil, err
	}
	return incomingRequest, nil
}

// Handler function for proxy server. Takes an incoming connection, establishes a connection to the end host.
// Forwards the incoming get request to end-host, receives a response, writes it to a new response to the client and sends that to the client.
func handler(c net.Conn) {
	defer c.Close()
	res := &http.Response{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
	}
	incomingRequest, err := getIncomingRequest(c, res)
	if err != nil {
		return
	}
	defer incomingRequest.Body.Close()
	destination, err := proxyConnection(incomingRequest)
	if err != nil {
		writeError(c, res, http.StatusBadGateway)
		return
	}
	defer destination.Close()
	err = incomingRequest.Write(destination)
	if err != nil {
		writeError(c, res, http.StatusBadGateway)
		return
	}
	incomingResponse, err := http.ReadResponse(bufio.NewReader(destination), incomingRequest)
	if err != nil {
		writeError(c, res, http.StatusBadGateway)
		return
	}
	defer incomingResponse.Body.Close()
	err = incomingResponse.Write(c)
	if err != nil {
		fmt.Println("Failed to send response to client:", err)
		return
	}
}

func main() {
	// Takes port it should listen to as argument.
	PORT := os.Args[1]
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		fmt.Println("Server failed to start")
		return
	}

	// Accepts all incoming connections.
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			continue
		}

		// Buffered channel used as a semaphore with n=maxConnections.
		semaphore := make(chan struct{}, maxConnections)

		semaphore <- struct{}{}
		// Create goroutine and handle connection, release semaphore when handler is done. .
		go func() {
			defer func() { <-semaphore }()
			handler(c)
		}()
	}
}
