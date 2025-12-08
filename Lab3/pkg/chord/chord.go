package chord

import (
	"crypto/sha1"
	"fmt"
	"sync"
)

type Key string
type NodeAddress string

type Node struct {
	mu          *sync.RWMutex
	Address     NodeAddress
	Predecessor NodeAddress
	Successor   NodeAddress
	FingerTable []NodeAddress
}

func StartNode(address string, port int) *Node {
	n := &Node{
		mu:          &sync.RWMutex{},
		Address:     NodeAddress(fmt.Sprintf("%s:%d", address, port)),
		FingerTable: []NodeAddress{},
	}
	n.create()
	return n
}

func (n *Node) FindSuccessor(id int) {

}

func (n *Node) Join(other string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("Joining node at %s\n", other)
	n.Successor = CallFindSuccessor(other, hashAddress(n.Address))

}

func (n *Node) Notify(other Node) {

}
func (n *Node) closestPrecedingNode(id int) {

}

func (n *Node) checkPredecessor() {

}
func (n *Node) create() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Predecessor = ""
	n.Successor = n.Address
}

func (n *Node) fixFingers() {

}

func (n *Node) stabilize() {

}

func hashAddress(address NodeAddress) string {
	// SHA1 is considered insecure for cryptographic purposes, but is sufficient for generating node IDs in a Chord DHT.
	return fmt.Sprintf("%x", sha1.Sum([]byte(address)))
}
