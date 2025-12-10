package chord

import (
	"crypto/sha1"
	"fmt"
	"sync"
	"net"
	"net/rpc"
	"net/http"
	"time"
)

const (
	Sha1BitSize = 160
)

var (
	next int = 0
)

type Node struct {
	mu          *sync.RWMutex
	Address     string
	Id          string
	Predecessor string
	Successors  []string
	FingerTable []string
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
		mu:          &sync.RWMutex{},
		Address:     fmt.Sprintf("%s:%d", address, port),
		FingerTable: make([]string, Sha1BitSize),
		Successors:   make([]string, successorLimit),
	}
	n.create()
	if identifier != "" {
		n.Id = identifier
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
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("Joining node at %s\n", other)
	successor := CallFindSuccessor(other, n.Id)
	if successor == "" {
		fmt.Println("Failed to find successor, cannot join the network")
		fmt.Println("Node is now its own successor")
		return
	}

	n.Successors[0] = successor
}

func (n *Node) ClosestPrecedingNode(id string) string {
	for i := Sha1BitSize - 1; i >= 0; i-- {
		fingerId := HashAddress(n.FingerTable[i])
		if fingerId > n.Id && fingerId < id {
			return n.FingerTable[i]
		}
	}
	return n.Address
}

func (n *Node) fixFingers() {
	n.mu.Lock()
	defer n.mu.Unlock()
	next = (next + 1) % Sha1BitSize
	n.FingerTable[next] = CallFindSuccessor(n.Address, n.Id + fmt.Sprintf("%x", 1<<uint(next)))
}

func (n *Node) stabilize() {
	n.mu.Lock()
	defer n.mu.Unlock()
	predecessor := CallGetPredecessor(n.Successors[0])
	if predecessor != "" && predecessor > n.Id && predecessor < HashAddress(n.Successors[0]) {
		n.Successors[0] = predecessor
	}
	CallNotify(n.Successors[0], n.Address)
}

func (n *Node) checkPredecessor() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Predecessor != "" {
		if !CallAlive(n.Predecessor){
			n.Predecessor = ""
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

func HashAddress(address string) string {
	// SHA1 is considered insecure for cryptographic purposes, but is sufficient for generating node IDs in a Chord DHT.
	return fmt.Sprintf("%x", sha1.Sum([]byte(address)))
}