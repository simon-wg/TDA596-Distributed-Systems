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

type FileStatuses struct {
	v  map[int]int
	mu sync.Mutex
}

type ReduceStatuses struct {
	v  map[int]int
	mu sync.Mutex
}

func (fs *FileStatuses) Get(key int) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.v[key]
}

func (fs *FileStatuses) Set(key int, value int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.v[key] = value
}

func (fs *FileStatuses) Len() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.v)
}

func (fs *FileStatuses) Lock() { fs.mu.Lock() }

func (fs *FileStatuses) Unlock() { fs.mu.Unlock() }

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

func (rs *ReduceStatuses) Len() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.v)
}

func (rs *ReduceStatuses) Lock() { rs.mu.Lock() }

func (rs *ReduceStatuses) Unlock() { rs.mu.Unlock() }

type Coordinator struct {
	// Your definitions here.
	Files          []string
	FileStatuses   FileStatuses
	MappingDone    bool
	ReduceStatuses ReduceStatuses
	ReducingDone   bool
	NReduce        int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Println("Received RequestTask RPC")
	if !c.MappingDone {
		for file := range c.Files {
			c.FileStatuses.Lock()
			if c.FileStatuses.v[file] == 0 {
				reply.FileName = c.Files[file]
				reply.FileNumber = file
				reply.NReduce = c.NReduce
				reply.TaskType = Map
				c.FileStatuses.v[file] = 1
				c.FileStatuses.Unlock()
				go checkTimeoutMap(10, file, c)
				return nil
			}
			c.FileStatuses.Unlock()
		}
		// Check if mapping is done
		allDone := true
		for file := range c.FileStatuses.Len() {
			if c.FileStatuses.Get(file) != 2 {
				allDone = false
				break
			}
		}
		c.MappingDone = allDone
		if !c.MappingDone {
			reply.TaskType = Wait
			return nil
		}
	}
	for reduceTask := 0; reduceTask < c.NReduce; reduceTask++ {
		c.ReduceStatuses.Lock()
		if c.ReduceStatuses.v[reduceTask] == 0 {
			reply.FileNumber = reduceTask
			reply.NReduce = c.NReduce
			reply.TaskType = Reduce
			c.ReduceStatuses.v[reduceTask] = 1
			c.ReduceStatuses.Unlock()
			go checkTimeoutReduce(10, reduceTask, c)
			return nil
		}
		c.ReduceStatuses.Unlock()
	}
	allDone := true
	for reduceTask := range c.ReduceStatuses.Len() {
		if c.ReduceStatuses.Get(reduceTask) != 2 {
			allDone = false
			break
		}
	}
	c.ReducingDone = allDone
	if !c.ReducingDone {
		reply.TaskType = Wait
		return nil
	}
	// If all tasks are done
	reply.FileNumber = -1
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	file := args.FileNumber
	c.FileStatuses.Set(file, 2)
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneArgs) error {
	file := args.FileNumber
	c.ReduceStatuses.Set(file, 2)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for file := range c.FileStatuses.Len() {
		if c.FileStatuses.Get(file) != 2 {
			return false
		}
	} // Your code here.
	for reduceTask := range c.ReduceStatuses.Len() {
		if c.ReduceStatuses.Get(reduceTask) != 2 {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fileStatuses := make(map[int]int)
	for i := range fileStatuses {
		fileStatuses[i] = 0
	}
	reduceStatuses := make(map[int]int)
	for i := range nReduce {
		reduceStatuses[i] = 0
	}
	c := Coordinator{
		Files:          files,
		FileStatuses:   FileStatuses{v: fileStatuses},
		ReduceStatuses: ReduceStatuses{v: reduceStatuses},
		NReduce:        nReduce,
	}
	c.server()
	return &c
}

// Checks that reduce status has changed after t seconds. If not resets reducestatus to 0 for redistribution of task.
func checkTimeoutReduce(t int, file int, c *Coordinator) {
	//fmt.Printf("Checking reduce timeout for file: %d \n", file)
	stat := c.ReduceStatuses.Get(file)
	time.Sleep(time.Duration(t) * time.Second)
	c.ReduceStatuses.Lock()
	if stat == c.ReduceStatuses.v[file] {
		c.ReduceStatuses.v[file] = 0
		fmt.Printf("Reducing failed for file %d: \n", file)
	}
	//fmt.Printf("Finished reduce timeout check for file: %d \n", file)
	c.ReduceStatuses.Unlock()
}

// Checks that file status has changed after t seconds. If not resets filestatus to 0 for redistribution of task.
func checkTimeoutMap(t int, file int, c *Coordinator) {
	//fmt.Printf("Checking map timeout for file: %d \n", file)
	stat := c.FileStatuses.Get(file)
	time.Sleep(time.Duration(t) * time.Second)
	c.FileStatuses.Lock()
	if stat == c.FileStatuses.v[file] {
		c.FileStatuses.v[file] = 0
		fmt.Printf("Mapping failed for file %d: \n", file)
	}
	//fmt.Printf("Finished map timeout check for file: %d \n", file)
	c.FileStatuses.Unlock()
}
