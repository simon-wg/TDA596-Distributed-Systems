package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

var workerIp string = ""
var workerPort int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerServer struct {
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// start worker rpc server
	w := WorkerServer{}
	w.server()

	if workerIp == "" {
		fmt.Println("Requesting local IP")
		conn, err := net.Dial("tcp", "1.1.1.1:80")
		fmt.Println("Connection established")
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		workerIp, _, err = net.SplitHostPort(conn.LocalAddr().String())
		if err != nil {
			log.Fatal(err)
		}
		conn.Close()
	}

	// Your worker implementation here.

	for {
		// We check what task we have.
		reply := CallRequestTask()
		if reply.FileNumber == -1 {
			fmt.Println("Job's Done")
			break
		}
		if reply.TaskType == Wait {
			// Wait before requesting again
			time.Sleep(time.Second)
			continue
		}
		taskType := reply.TaskType
		switch taskType {
		case Map:
			err := os.WriteFile(reply.FileName, reply.File[0], 0644)
			if err != nil {
				log.Fatal("cannot write file from coordinator", err)
			}
			err = performMapTask(reply, mapf)
			if err != nil {
				log.Fatal("Map task failed:", err)
			}
			err = CallMapDone(reply.FileNumber)
			if err != nil {
				log.Fatal("MapDone RPC failed:", err)
			}
			fmt.Println("Completed map task for file:", reply.FileName)
		case Reduce:
			err := performReduceTask(reply, reducef)
			if err != nil {
				log.Println("Reduce task failed:", err)
				continue
			}
			err = CallReduceDone(reply.FileNumber)
			if err != nil {
				log.Fatal("ReduceDone RPC failed:", err)
			}
			fmt.Println("Completed reduce task:", reply.FileNumber)
		}
	}
}

func performMapTask(mapTask *RequestTaskReply, mapf func(string, string) []KeyValue) error {
	// Reads file given by reply
	content, err := os.ReadFile(mapTask.FileName)
	if err != nil {
		fmt.Println(err)
		return err
	}
	// Performs map function on task
	mapped := mapf(mapTask.FileName, string(content))
	// Writes result of mapf into NReduce files
	files := make([]*os.File, mapTask.NReduce)
	encoders := make([]*json.Encoder, mapTask.NReduce)
	// Creates NReduce tempfiles
	for i := 0; i < mapTask.NReduce; i++ {
		// Create mr-tmp folder if not exists
		err := os.MkdirAll("mr-tmp", os.ModePerm)
		if err != nil {
			log.Fatal("cannot create mr-tmp directory", err)
		}
		file, err := os.CreateTemp("mr-tmp", "mr-*")
		enc := json.NewEncoder(file)
		if err != nil {
			log.Fatal("cannot create file", err)
		}
		encoders[i] = enc
		files[i] = file
	}
	// Puts key, value pairs into designated Reduce file
	for _, kv := range mapped {
		hashedKey := ihash(kv.Key) % mapTask.NReduce
		enc := encoders[hashedKey]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode kv", err)
		}
	}
	// Renames temp files to mr-FileID-x
	for i, file := range files {
		file.Close()
		err := os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", mapTask.FileNumber, i))
		if err != nil {
			log.Fatal("cannot rename file", err)
		}
	}
	return nil
}

func performReduceTask(reduceTask *RequestTaskReply, reducef func(string, []string) string) error {
	reduceMap := map[string][]string{}
	reduceResult := map[string]string{}
	for _, address := range reduceTask.FileAddresses {
		err := GetFilesFromWorker(address, reduceTask.FileNumber)
		if err != nil {
			return err
		}
	}

	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("cannot read dir", err)
	}
	prefix := "mr-"
	suffix := fmt.Sprintf("-%d", reduceTask.FileNumber)
	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > len(prefix)+len(suffix) &&
			file.Name()[:len(prefix)] == prefix &&
			file.Name()[len(file.Name())-len(suffix):] == suffix {
			// Found a matching file
			f, err := os.Open(file.Name())
			if err != nil {
				log.Fatal("cannot open file", err)
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
			}
			f.Close()
		}
	}

	// Creates temp file
	for key, values := range reduceMap {
		reduceResult[key] = reducef(key, values)
	}
	outputFile, err := os.CreateTemp("mr-tmp", "mr-out-*")
	if err != nil {
		log.Fatal("cannot create output file", err)
	}
	// Writes reduceResult to mr-out-FileNumber
	for key, value := range reduceResult {
		_, err := fmt.Fprintf(outputFile, "%v %v\n", key, value)
		if err != nil {
			log.Fatal("cannot write to output file", err)
		}
	}
	outputFile.Close()
	err = os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%d", reduceTask.FileNumber))
	if err != nil {
		log.Fatal("cannot rename output file", err)
	}
	return nil
}

func CallRequestTask() *RequestTaskReply {
	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return &reply
}

func CallMapDone(fileNumber int) error {
	args := MapDoneArgs{
		FileNumber:   fileNumber,
		WorkerAdress: fmt.Sprintf("%s:%d", workerIp, workerPort),
	}
	reply := MapDoneReply{}

	ok := call("Coordinator.MapDone", &args, &reply)
	if !ok {
		return fmt.Errorf("call failed")
	}
	return nil
}

func CallReduceDone(fileNumber int) error {
	args := ReduceDoneArgs{
		FileNumber: fileNumber,
	}
	reply := ReduceDoneReply{}

	ok := call("Coordinator.ReduceDone", &args, &reply)
	if !ok {
		return fmt.Errorf("call failed")
	}
	return nil
}

func GetFilesFromWorker(address string, reduceNumber int) error {
	args := RequestReduceFilesArgs{
		ReduceNumber: reduceNumber,
	}
	reply := RequestReduceFilesReply{}
	ok := callWorker("WorkerServer.RequestReduceFiles", &args, &reply, address)
	if !ok {
		return fmt.Errorf("call to worker failed")
	}
	// Write files to local disk
	for fileName, data := range reply.Files {
		err := os.WriteFile(fileName, data, 0644)
		if err != nil {
			log.Fatal("cannot write file from worker", err)
		}
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func callWorker(rpcname string, args interface{}, reply interface{}, address string) bool {
	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *WorkerServer) RequestReduceFiles(args *RequestReduceFilesArgs, reply *RequestReduceFilesReply) error {
	reduceNumber := args.ReduceNumber
	reply.Files = make(map[string][]byte)
	prefix := "mr-"
	suffix := fmt.Sprintf("-%d", reduceNumber)
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("cannot read dir", err)
	}
	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > len(prefix)+len(suffix) &&
			file.Name()[:len(prefix)] == prefix &&
			file.Name()[len(file.Name())-len(suffix):] == suffix {
			// Found a matching file
			data, err := os.ReadFile(file.Name())
			if err != nil {
				log.Fatal("cannot read file", err)
			}
			reply.Files[file.Name()] = data
		}
	}
	return nil
}

// start a thread that listens for RPCs from other workers
func (w *WorkerServer) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp4", ":0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	workerPort = l.Addr().(*net.TCPAddr).Port
	go http.Serve(l, nil)
}
