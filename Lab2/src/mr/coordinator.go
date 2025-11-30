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

type MapStatuses struct {
	v    map[int]int
	done bool
	mu   sync.Mutex
}

type ReduceStatuses struct {
	v    map[int]int
	done bool
	mu   sync.Mutex
}

type FileAddresses struct {
	v  map[string]string
	mu sync.Mutex
}

func (fs *MapStatuses) Get(key int) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.v[key]
}

func (fs *MapStatuses) Set(key int, value int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.v[key] = value
}

func (fs *MapStatuses) Done() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.done
}

func (fs *MapStatuses) SetDone(value bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.done = value
}

func (fs *MapStatuses) Len() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.v)
}

func (fs *MapStatuses) Lock() { fs.mu.Lock() }

func (fs *MapStatuses) Unlock() { fs.mu.Unlock() }

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

func (fs *ReduceStatuses) Done() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.done
}

func (fs *ReduceStatuses) SetDone(value bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.done = value
}

func (rs *ReduceStatuses) Len() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.v)
}

func (rs *ReduceStatuses) Lock() { rs.mu.Lock() }

func (rs *ReduceStatuses) Unlock() { rs.mu.Unlock() }

func (fs *FileAddresses) GetAdresses(reduceNumber int) []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var addresses []string
	prefix := "mr-"
	suffix := fmt.Sprintf("-%d", reduceNumber)
	for file, address := range fs.v {
		if len(file) > len(prefix)+len(suffix) &&
			file[:len(prefix)] == prefix &&
			file[len(file)-len(suffix):] == suffix {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func (fs *FileAddresses) Set(file string, value string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.v[file] = value
}

func (fs *FileAddresses) Len() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.v)
}

func (fs *FileAddresses) Lock() { fs.mu.Lock() }

func (fs *FileAddresses) Unlock() { fs.mu.Unlock() }

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
	fmt.Println("Received RequestTask RPC")
	if !c.MapStatuses.Done() {
		for file := range c.Files {
			c.MapStatuses.Lock()
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
				c.MapStatuses.Unlock()
				//go checkTimeoutMap(10, file, c)
				fmt.Println("Starting map task for file:", c.Files[file])
				return nil
			}
			c.MapStatuses.Unlock()
		}
		// Check if mapping is done
		allDone := true
		for file := range c.MapStatuses.Len() {
			if c.MapStatuses.Get(file) != 2 {
				allDone = false
				break
			}
		}
		c.MapStatuses.SetDone(allDone)
		if !c.MapStatuses.Done() {
			c.MapStatuses.Lock()
			for mapTask := range c.MapStatuses.v {
				fmt.Println("Map task", mapTask, "status:", c.MapStatuses.v[mapTask])
			}
			c.MapStatuses.Unlock()
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
			reply.FileAddresses = c.FileAddresses.GetAdresses(reduceTask)
			c.ReduceStatuses.v[reduceTask] = 1
			c.ReduceStatuses.Unlock()
			//go checkTimeoutReduce(10, reduceTask, c)
			fmt.Println("Starting reduce task:", reduceTask)
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
	c.ReduceStatuses.SetDone(allDone)
	if !c.ReduceStatuses.Done() {
		reply.TaskType = Wait
		return nil
	}
	// If all tasks are done
	reply.FileNumber = -1
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	file := args.FileNumber
	workerAdress := args.WorkerAdress
	for i := 0; i < c.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", file, i)
		c.FileAddresses.Set(intermediateFileName, workerAdress)
	}
	c.MapStatuses.Set(file, 2)
	fmt.Printf("Map task %d done, intermediate files stored at worker %s\n", file, workerAdress)
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	file := args.FileNumber
	c.ReduceStatuses.Set(file, 2)
	fmt.Printf("Reduce task %d done\n", file)
	return nil
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
	for file := range c.MapStatuses.Len() {
		if c.MapStatuses.Get(file) != 2 {
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
		MapStatuses:    MapStatuses{v: fileStatuses},
		ReduceStatuses: ReduceStatuses{v: reduceStatuses},
		FileAddresses:  FileAddresses{v: make(map[string]string)},
		NReduce:        nReduce,
	}
	c.server()
	go c.checkTimeouts(10)
	return &c
}

// Checks that reduce status has changed after t seconds. If not, restart all tasks.
func checkTimeoutReduce(t int, file int, c *Coordinator) {
	//fmt.Printf("Checking reduce timeout for file: %d \n", file)
	stat := c.ReduceStatuses.Get(file)
	time.Sleep(time.Duration(t) * time.Second)
	if stat == c.ReduceStatuses.Get(file) {
		fmt.Printf("Reducing failed for file: %d\n", file)
	}
	c.restartAllTasks()
}

// Checks that file status has changed after t seconds. If not, restart all tasks.
func checkTimeoutMap(t int, file int, c *Coordinator) {
	//fmt.Printf("Checking map timeout for file: %d \n", file)
	stat := c.MapStatuses.Get(file)
	time.Sleep(time.Duration(t) * time.Second)
	if stat == c.MapStatuses.Get(file) {
		fmt.Printf("Mapping failed for file %d: \n", file)
	}
	c.restartAllTasks()
}

func (c *Coordinator) checkTimeouts(t int) {
	for {
		mapDone := c.MapStatuses.Done()
		if mapDone {
			initialReduceStatuses := make(map[int]int)
			c.ReduceStatuses.Lock()
			for k, v := range c.ReduceStatuses.v {
				initialReduceStatuses[k] = v
			}
			c.ReduceStatuses.Unlock()
			time.Sleep(time.Duration(t) * time.Second)
			c.ReduceStatuses.Lock()
			for k, v := range c.ReduceStatuses.v {
				if v == initialReduceStatuses[k] && v != 2 && v != 0 {
					fmt.Printf("Reducing failed for task: %d\n", k)
					c.restartAllTasks()
				}
			}
			c.ReduceStatuses.Unlock()
		} else {
			initialMapStatuses := make(map[int]int)
			c.MapStatuses.Lock()
			for k, v := range c.MapStatuses.v {
				initialMapStatuses[k] = v
			}
			c.MapStatuses.Unlock()
			time.Sleep(time.Duration(t) * time.Second)
			c.MapStatuses.Lock()
			for k, v := range c.MapStatuses.v {
				if v == initialMapStatuses[k] && v != 2 && v != 0 {
					fmt.Printf("Mapping failed for file: %d\n", k)
					c.restartAllTasks()
				}
			}
			c.MapStatuses.Unlock()
		}
	}
}

func (c *Coordinator) restartAllTasks() {
	// Restart map tasks
	c.MapStatuses.Lock()
	for file := range c.MapStatuses.v {
		c.MapStatuses.v[file] = 0
	}
	c.MapStatuses.Unlock()
	c.MapStatuses.SetDone(false)
	c.ReduceStatuses.Lock()
	for reduceTask := range c.ReduceStatuses.v {
		c.ReduceStatuses.v[reduceTask] = 0
	}
	c.ReduceStatuses.Unlock()
	c.FileAddresses.Lock()
	c.FileAddresses.v = make(map[string]string)
	c.FileAddresses.Unlock()
	c.ReduceStatuses.SetDone(false)
}
