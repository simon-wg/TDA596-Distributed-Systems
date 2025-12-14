package chord

import (
	"fmt"
	"math/big"
	"net/rpc"
)

func CallAlive(address string) bool {
	req := &struct{}{}
	resp := &struct{}{}
	err := CallCommand(address, "Alive", req, resp)
	if err != nil {
		fmt.Printf("Node at %s is not alive: %v\n", address, err)
		return false
	}
	return true
}

func CallFindSuccessor(address string, id *big.Int) string {
	req := &FindSuccessorArgs{Id: *id}
	resp := &FindSuccessorReply{}
	err := CallCommand(address, "FindSuccessor", req, resp)
	if err != nil {
		fmt.Printf("Error finding successor: %v\n", err)
		return ""
	}
	return resp.Successor
}

func CallNotify(address string, nodeAddress string) {
	req := &NotifyArgs{Address: nodeAddress}
	resp := &NotifyReply{}
	err := CallCommand(address, "Notify", req, resp)
	if err != nil {
		fmt.Printf("Error notifying node: %v\n", err)
	}
}

func CallGetPredecessor(address string) string {
	req := &GetPredecessorArgs{}
	resp := &GetPredecessorReply{}
	err := CallCommand(address, "GetPredecessor", req, resp)
	if err != nil {
		fmt.Printf("Error getting predecessor: %v\n", err)
		return ""
	}
	return resp.Predecessor
}

func CallGetSuccessors(address string) []string {
	req := &GetSuccessorsArgs{}
	resp := &GetSuccessorsReply{}
	err := CallCommand(address, "GetSuccessors", req, resp)
	if err != nil {
		fmt.Printf("Error getting successors: %v\n", err)
		return []string{}
	}
	return resp.Successors
}

func CallCommand(address string, method string, req interface{}, resp interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return fmt.Errorf("error dialing %s: %v", address, err)
	}
	defer client.Close()
	err = client.Call("Node."+method, req, resp)
	if err != nil {
		return fmt.Errorf("error calling %s on %s: %v", method, address, err)
	}
	return nil
}

func (n *Node) Alive(args *struct{}, reply *struct{}) error {
	return nil
}

func (n *Node) FindSuccessor(args *FindSuccessorArgs, reply *FindSuccessorReply) error {
	id := args.Id
	n.mu.RLock()
	for _, successor := range n.Successors {
		if successor == "" {
			continue
		}
		successorIdInclusive := new(big.Int).Add(HashAddress(successor), big.NewInt(1))
		if IsBetween(&id, n.Id, successorIdInclusive) {
			reply.Successor = successor
			n.mu.RUnlock()
			return nil
		}
	}
	n.mu.RUnlock()
	closestPrecedingNode := n.ClosestPrecedingNode(&id)
	// TODO: Double check that this is correct for larger rings
	n.mu.RLock()
	if closestPrecedingNode == n.Address {
		reply.Successor = n.Successors[0]
		n.mu.RUnlock()
		return nil
	}
	n.mu.RUnlock()
	successor := CallFindSuccessor(closestPrecedingNode, &id)
	reply.Successor = successor
	return nil
}

func (n *Node) Notify(args *NotifyArgs, reply *NotifyReply) error {
	address := args.Address
	id := HashAddress(address)
	n.mu.Lock()
	defer n.mu.Unlock()
	predId := HashAddress(n.Predecessor)
	if n.Predecessor == "" || IsBetween(id, predId, n.Id) {
		n.Predecessor = address
	}
	return nil
}

func (n *Node) GetPredecessor(args *GetPredecessorArgs, reply *GetPredecessorReply) error {
	reply.Predecessor = n.ReadPredecessor()
	return nil
}

func (n *Node) GetSuccessors(args *GetSuccessorsArgs, reply *GetSuccessorsReply) error {
	reply.Successors = n.ReadSuccessors()
	return nil
}

type FindSuccessorArgs struct {
	Id big.Int
}
type FindSuccessorReply struct {
	Successor string
}

type NotifyArgs struct {
	Address string
}
type NotifyReply struct{}

type GetPredecessorArgs struct{}
type GetPredecessorReply struct {
	Predecessor string
}

type GetSuccessorsArgs struct{}
type GetSuccessorsReply struct {
	Successors []string
}
