package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"sync"
	"time"
)

const (
	Sha1BitSize = 160
)

var (
	next int = -1
)

type Node struct {
	mu             *sync.RWMutex
	SuccessorLimit int
	Address        string
	Id             *big.Int
	Predecessor    string
	Successors     []string
	FingerTable    []string
}

func (n *Node) startRpcServer() {
	rpc.Register(n)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", n.Address)
	if err != nil {
		fmt.Printf("Error starting RPC server: %v\n", err)
		return
	}
	fmt.Printf("RPC server listening on %s\n", n.Address)
	go http.Serve(l, nil)
}

// Plz
func StartNode(address string, port int, successorLimit int, identifier string, stabilizationTime int, fixFingerTime int, checkPredTime int) *Node {
	n := &Node{
		mu:             &sync.RWMutex{},
		SuccessorLimit: successorLimit,
		Address:        fmt.Sprintf("%s:%d", address, port),
		FingerTable:    make([]string, Sha1BitSize),
		Successors:     make([]string, successorLimit),
	}
	n.create()
	if identifier != "" {
		n.Id = new(big.Int)
		n.Id.SetString(identifier, 16)
	}
	n.startRpcServer()
	n.startBackgroundRoutines(stabilizationTime, fixFingerTime, checkPredTime)
	return n
}

func (n *Node) create() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Predecessor = ""
	n.Successors[0] = n.Address
	for i := 1; i < len(n.Successors); i++ {
		n.Successors[i] = ""
	}
	n.Id = HashAddress(n.Address)
}

func (n *Node) Join(other string) {
	fmt.Printf("Joining node at %s\n", other)
	n.mu.Lock()
	defer n.mu.Unlock()
	successor := CallFindSuccessor(other, n.Id)
	if successor == "" {
		fmt.Println("Failed to find successor, cannot join the network")
		fmt.Println("Node is now its own successor")
		return
	}

	n.Successors[0] = successor
}

func (n *Node) ClosestPrecedingNode(id *big.Int) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for i := Sha1BitSize - 1; i >= 0; i-- {
		fingerId := HashAddress(n.FingerTable[i])
		if IsBetween(fingerId, n.Id, id) {
			return n.FingerTable[i]
		}
	}
	return n.Address
}

func (n *Node) fixFingers() {
	n.mu.Lock()
	next = (next + 1) % Sha1BitSize
	id := n.Id
	nextId := new(big.Int).Add(id, new(big.Int).Lsh(big.NewInt(1), uint(next)))
	args := &FindSuccessorArgs{
		Id: *nextId,
	}
	reply := &FindSuccessorReply{}
	n.mu.Unlock()
	err := n.FindSuccessor(args, reply)
	if err != nil {
		fmt.Printf("Error fixing finger %d: %v\n", next, err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.FingerTable[next] = reply.Successor
}

func (n *Node) stabilize() {
	var x string
	successor := n.ReadSuccessor()

	for {
		if successor != n.Address && !CallAlive(successor) {
			successor = n.PopSuccessor()
		} else {
			break
		}
	}
	n.UpdateSuccessors(CallGetSuccessors(successor), successor)
	// Get successor's predecessor
	if successor == n.Address {
		x = n.ReadPredecessor()
	} else {
		x = CallGetPredecessor(successor)
	}
	// Check if x is between n and n's successor
	successorId := HashAddress(successor)
	if x != "" {
		xId := HashAddress(x)
		nId := n.ReadID()
		if IsBetween(xId, nId, successorId) {
			n.mu.Lock()
			n.Successors[0] = x
			successor = x
			n.mu.Unlock()
		}
	}
	// Notify successor
	if successor == n.Address {
		args := &NotifyArgs{
			Address: n.Address,
		}
		reply := &NotifyReply{}
		n.Notify(args, reply)
	} else {
		CallNotify(n.Successors[0], n.Address)
	}
}

func (n *Node) checkPredecessor() {
	pred := n.ReadPredecessor()

	if pred != "" && pred != n.Address {
		if !CallAlive(pred) {
			n.mu.Lock()
			if n.Predecessor == pred {
				n.Predecessor = ""
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startBackgroundRoutines(stabilizationTime int, fixFingerTime int, checkPredTime int) {
	go func() {
		for {
			n.stabilize()
			time.Sleep(time.Duration(stabilizationTime) * time.Millisecond)
		}
	}()
	go func() {
		for {
			n.fixFingers()
			time.Sleep(time.Duration(fixFingerTime) * time.Millisecond)
		}
	}()
	go func() {
		for {
			n.checkPredecessor()
			time.Sleep(time.Duration(checkPredTime) * time.Millisecond)
		}
	}()
}

func HashAddress(address string) *big.Int {
	// SHA1 is considered insecure for cryptographic purposes, but is sufficient for generating node IDs in a Chord DHT.
	digest := sha1.Sum([]byte(address))
	id := new(big.Int).SetBytes(digest[:])
	return id
}

func IsBetween(id, start, end *big.Int) bool {
	if start.Cmp(end) < 0 {
		return id.Cmp(start) > 0 && id.Cmp(end) < 0
	} else {
		return id.Cmp(start) > 0 || id.Cmp(end) < 0
	}
}

func (n *Node) PopSuccessor() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < len(n.Successors)-1; i++ {
		n.Successors[i] = n.Successors[i+1]
	}
	n.Successors[len(n.Successors)-1] = ""
	return n.Successors[0]
}

func (n *Node) UpdateSuccessors(ns []string, s string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Successors[0] = s
	if slices.Compare(n.Successors[1:], ns[:len(ns)-1]) == 0 {
		return
	}
	for i := 0; i < len(ns)-1 && i+1 < len(n.Successors); i++ {
		n.Successors[i+1] = ns[i]
	}
}

func (n *Node) ReadPredecessor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Predecessor
}

func (n *Node) ReadSuccessor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.Successors) == 0 {
		return ""
	}
	return n.Successors[0]
}

func (n *Node) ReadSuccessors() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	successors := make([]string, len(n.Successors))
	copy(successors, n.Successors)
	return successors
}

func (n *Node) ReadID() *big.Int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Id
}

func (n *Node) PrintState() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Client info
	fmt.Println("=====")
	fmt.Printf("Node ID: %s\n", n.Id)
	fmt.Printf("Node Address: %s\n", n.Address)
	fmt.Println("=====")
	fmt.Printf("Predecessor: %s\n", n.Predecessor)
	fmt.Printf("Predecessor ID: %s\n", HashAddress(n.Predecessor))
	fmt.Println("=====")

	// Successors info
	for i, successor := range n.Successors {
		fmt.Printf("Successor %d: %s\n", i, successor)
		fmt.Printf("Successor %d ID: %s\n", i, HashAddress(successor))
		fmt.Println("-----")
	}

	// Fingertable info
	limit := min(len(n.FingerTable), 3)
	for i := range limit {
		finger := n.FingerTable[i]
		if finger == "" {
			continue
		}
		fmt.Printf("Finger %d: %s\n", i, finger)
		fmt.Printf("Finger %d ID: %s\n", i, HashAddress(finger))
		fmt.Println("-----")
	}
}
