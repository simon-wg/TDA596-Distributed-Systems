package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.

type RequestTaskType int

const (
	Map RequestTaskType = iota
	Reduce
	Wait
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	FileName      string
	FileNumber    int
	NReduce       int
	TaskType      RequestTaskType
	File          [][]byte
	FileAddresses []string
}

type MapDoneArgs struct {
	FileNumber   int
	WorkerAdress string
}

type MapDoneReply struct{}

type ReduceDoneArgs struct {
	FileNumber int
}

type ReduceDoneReply struct {
}

type MissingArgs struct {
	WorkerAdress string
	ReduceNumber int
}

type MissingReply struct {
}

type RequestReduceFilesArgs struct {
	ReduceNumber int
}

type RequestReduceFilesReply struct {
	Files map[string][]byte
}

