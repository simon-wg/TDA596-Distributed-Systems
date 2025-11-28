package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestTaskType int

const (
	Map RequestTaskType = iota
	Reduce
	Wait
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	FileName   string
	FileNumber int
	NReduce    int
	TaskType   RequestTaskType
	File       [][]byte
}

type MapDoneArgs struct {
	FileNumber   int
	WorkerAdress string
}

type MapDoneReply struct{}

type ReduceDoneArgs struct {
	FileNumber int
}

type ReduceDoneReply struct{}
