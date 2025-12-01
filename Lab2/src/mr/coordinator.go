package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files          []string
	MapStatuses    MapStatuses
	ReduceStatuses ReduceStatuses
	FileAddresses  FileAddresses
	NReduce        int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	// If Mapping is not finished sends map task
	if !c.MapStatuses.Done() {
		for file := range c.Files {
			if c.sendMapTask(file, reply) {
				return nil
			}
		}
		if !c.MapStatuses.Done() {
			reply.TaskType = Wait
			return nil
		}
	}
	// If mapping is done, sends reduce task
	for reduceTask := 0; reduceTask < c.NReduce; reduceTask++ {
		if c.sendReduceTask(reduceTask, reply) {
			return nil
		}
	}

	if !c.ReduceStatuses.Done() {
		reply.TaskType = Wait
		return nil
	}
	reply.FileNumber = -1
	return nil
}

// Generates and sends map task to worker
func (c *Coordinator) sendMapTask(file int, reply *RequestTaskReply) bool {
	c.MapStatuses.Lock()
	defer c.MapStatuses.Unlock()
	if c.MapStatuses.v[file] == 0 {
		reply.File = make([][]byte, 1)
		data, err := os.ReadFile(c.Files[file])
		if err != nil {
			log.Fatal("cannot read file", err)
		}
		reply.File[0] = data
		reply.FileName = c.Files[file]
		reply.FileNumber = file
		reply.NReduce = c.NReduce
		reply.TaskType = Map
		c.MapStatuses.v[file] = 1
		fmt.Println("Starting map task for file:", c.Files[file])
		return true
	}
	return false
}

// Generates and sends reduce task to worker
func (c *Coordinator) sendReduceTask(reduceId int, reply *RequestTaskReply) bool {
	c.ReduceStatuses.Lock()
	defer c.ReduceStatuses.Unlock()
	if c.ReduceStatuses.v[reduceId] == 0 {
		reply.FileNumber = reduceId
		reply.NReduce = c.NReduce
		reply.TaskType = Reduce
		reply.FileAddresses = c.FileAddresses.GetAdresses(reduceId)
		c.ReduceStatuses.v[reduceId] = 1
		fmt.Println("Starting reduce task:", reduceId)
		return true
	}
	return false
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	if c.MapStatuses.Get(args.FileNumber) != 1 {
		return nil
	}
	file := args.FileNumber
	workerAdress := args.WorkerAdress
	for i := 0; i < c.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", file, i)
		c.FileAddresses.Set(intermediateFileName, workerAdress)
	}
	c.MapStatuses.Set(file, 2)
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	file := args.FileNumber
	c.ReduceStatuses.Set(file, 2)
	return nil
}

// Called by worker if connection is unavailable when requesting a file from another worker.
// Assigns the files at that worker as not started.
func (c *Coordinator) Missing(args *MissingArgs, reply *MissingReply) error {
	c.FileAddresses.Lock()
	defer c.FileAddresses.Unlock()
	c.ReduceStatuses.Set(args.ReduceNumber, 0)
	for file, address := range c.FileAddresses.v {
		if address == args.WorkerAdress {
			c.MapStatuses.Set(extractFileNumber(file), 0)
		}
	}
	
	return nil
}

func extractFileNumber(file string) int {
	var fileNumber int
	fmt.Sscanf(file, "mr-%d-", &fileNumber)
	return fileNumber
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp4", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.ReduceStatuses.Done()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapStatuses := make(map[int]int)
	for i := range files {
		mapStatuses[i] = 0
	}
	reduceStatuses := make(map[int]int)
	for i := range nReduce {
		reduceStatuses[i] = 0
	}
	c := Coordinator{
		Files:          files,
		MapStatuses:    MapStatuses{v: mapStatuses},
		ReduceStatuses: ReduceStatuses{v: reduceStatuses},
		FileAddresses:  FileAddresses{v: make(map[string]string)},
		NReduce:        nReduce,
	}
	c.server()
	go c.checkTimeouts(4)
	return &c
}

// Checks for timeouts in mapping and reducing tasks
func (c *Coordinator) checkTimeouts(t int) {
	for {
		mapDone := c.MapStatuses.Done()
		if mapDone {
			// If we are done mapping we don't need to worry about mapping timeouts
			initialReduceStatuses := make(map[int]int)
			c.ReduceStatuses.Lock()
			for k, v := range c.ReduceStatuses.v {
				initialReduceStatuses[k] = v
			}
			c.ReduceStatuses.Unlock()
			time.Sleep(time.Duration(t) * time.Second)
			for i := range c.ReduceStatuses.Len() {
				if c.ReduceStatuses.Get(i) == initialReduceStatuses[i] && initialReduceStatuses[i] == 1 {
					fmt.Printf("Reducing failed for task: %d\n", i)
					c.ReduceStatuses.Set(i, 0)
					break
				}
			}
		} else {
			initialMapStatuses := make(map[int]int)
			c.MapStatuses.Lock()
			for k, v := range c.MapStatuses.v {
				initialMapStatuses[k] = v
			}
			c.MapStatuses.Unlock()
			time.Sleep(time.Duration(t) * time.Second)
			for i := range c.MapStatuses.Len() {
				if c.MapStatuses.Get(i) == initialMapStatuses[i] && initialMapStatuses[i] == 1 {
					fmt.Printf("Mapping failed for file: %d\n", i)
					c.MapStatuses.Set(i, 0)
					break
				}
			}
		}
	}
}

// Types for concurrent reads of file/job statuses
type MapStatuses struct {
	v  map[int]int
	mu sync.Mutex
}

func (ms *MapStatuses) Get(key int) int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.v[key]
}

func (ms *MapStatuses) Set(key int, value int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.v[key] = value
}

func (ms *MapStatuses) Done() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for mapTask := range ms.v {
		if ms.v[mapTask] != 2 {
			return false
		}
	}
	return true
}

func (ms *MapStatuses) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.v)
}

func (ms *MapStatuses) Lock() { ms.mu.Lock() }

func (ms *MapStatuses) Unlock() { ms.mu.Unlock() }

type ReduceStatuses struct {
	v  map[int]int
	mu sync.Mutex
}

func (rs *ReduceStatuses) Get(key int) int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.v[key]
}

func (rs *ReduceStatuses) Set(key int, value int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.v[key] = value
}

func (rs *ReduceStatuses) Done() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for reduceTask := range rs.v {
		if rs.v[reduceTask] != 2 {
			return false
		}
	}
	return true
}

func (rs *ReduceStatuses) Len() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.v)
}

func (rs *ReduceStatuses) Lock() { rs.mu.Lock() }

func (rs *ReduceStatuses) Unlock() { rs.mu.Unlock() }

type FileAddresses struct {
	v  map[string]string
	mu sync.Mutex
}

// Get addressaworker where intermediary files for reducejob is located.
func (fa *FileAddresses) GetAdresses(reduceNumber int) []string {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	var addresses []string
	prefix := "mr-"
	suffix := fmt.Sprintf("-%d", reduceNumber)
	for file, address := range fa.v {
		if len(file) > len(prefix)+len(suffix) &&
			file[:len(prefix)] == prefix &&
			file[len(file)-len(suffix):] == suffix {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func (fa *FileAddresses) Set(file string, value string) {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	fa.v[file] = value
}

func (fa *FileAddresses) Len() int {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	return len(fa.v)
}

func (fa *FileAddresses) Lock() { fa.mu.Lock() }

func (fa *FileAddresses) Unlock() { fa.mu.Unlock() }