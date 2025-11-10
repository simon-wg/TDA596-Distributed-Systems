package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
)

var root *os.Root

// Splits filepath on all '.'s and validates final element
func validateFileName(path string) bool {
	validExtensions := []string{"html", "txt", "gif", "jpeg", "jpg", "css"}
	splits := strings.Split(path, ".")
	ext := splits[len(splits)-1]
	if slices.Contains(validExtensions, ext) {
		return true
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
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Request:    req,
	}
	// Validate file name
	if !validateFileName(req.URL.Path) {
		fmt.Println("Invalid file extension")
		setError(res, 400)
		writeResponse(c, res)
		return
	}
	method := req.Method
	switch method {
	case "GET": // Handle GET request
		getHandler(c, req)
		return
	case "POST": // Handle POST request
		postHandler(c, req)
		return
	default:
		setError(res, 400)
		return
	}
}

func getHandler(c net.Conn, req *http.Request) error {
	file, err := getFileIfExists(req)
	res := &http.Response{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Request:    req,
	}
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File not found")
			setError(res, 404)
			writeResponse(c, res)
			return err
		}
		fmt.Println("Error opening file:", err)
		setError(res, 500)
		writeResponse(c, res)
		return err
	}
	defer file.Close()
	res.Body = file
	setContentType(res, file.Name())
	res.StatusCode = http.StatusOK
	res.Status = http.StatusText(res.StatusCode)
	return writeResponse(c, res)
}

func setContentType(res *http.Response, fileName string) {
	splits := strings.Split(fileName, ".")
	ext := splits[len(splits)-1]
	// text/html, text/plain, image/gif, image/jpeg, image/jpeg, or text/css
	switch ext {
	case "html":
		res.Header.Set("Content-Type", "text/html")
	case "txt":
		res.Header.Set("Content-Type", "text/plain")
	case "gif":
		res.Header.Set("Content-Type", "image/gif")
	case "jpg":
		res.Header.Set("Content-Type", "image/jpeg")
	case "jpeg":
		res.Header.Set("Content-Type", "image/jpeg")
	case "css":
		res.Header.Set("Content-Type", "text/css")
	}
}

func getFileIfExists(req *http.Request) (*os.File, error) {
	filePath := req.URL.Path[1:]
	file, err := root.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func postHandler(c net.Conn, req *http.Request) error {
	err := createFileIfNotExist(req)
	res := &http.Response{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Request:    req,
	}
	if err != nil {
		if os.IsExist(err) {
			fmt.Println("File already exists")
			setError(res, 409)
			writeResponse(c, res)
			return err
		}
		fmt.Println("Error creating file:", err)
		setError(res, 500)
		writeResponse(c, res)
		return err
	}
	res.StatusCode = http.StatusCreated
	res.Status = http.StatusText(res.StatusCode)
	return writeResponse(c, res)
}

func createFileIfNotExist(req *http.Request) error {
	filePath := req.URL.Path[1:]
	body := req.Body
	file, err := root.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.ReadFrom(body)
	fmt.Println("File created:", filePath)
	return err
}

func setError(res *http.Response, code int) {
	res.StatusCode = code
	res.Status = http.StatusText(code)
	res.Body = http.NoBody
}

func writeResponse(c net.Conn, res *http.Response) error {
	return res.Write(c)
}

func main() {
	// Get public root directory
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("Failed to get current working directory")
		return
	}

	// Create public directory if not already exists
	if _, err = os.Stat(path.Join(cwd, "public")); os.IsNotExist(err) {
		os.Mkdir(path.Join(cwd, "public"), 0755)
	}

	root, err = os.OpenRoot(path.Join(cwd, "public"))
	if err != nil {
		fmt.Println("Failed to open root directory")
		panic(err)
	}

	PORT := os.Args[1]
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", PORT))
	if err != nil {
		fmt.Println("Server failed to start")
		return
	}

	var semaphore = make(chan struct{}, 10)

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			continue
		}
		semaphore <- struct{}{}
		// Create goroutine and handle connection
		go func() {
			defer func() { <-semaphore }()
			handler(c)
		}()
	}
}
