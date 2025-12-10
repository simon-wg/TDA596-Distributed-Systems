package chord

import (
	"fmt"
	"net/rpc"
	"time"
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

func CallFindSuccessor(address string, id string) string {
	req := &FindSuccessorArgs{Id: id}
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

func CallCommand(address string, method string, req interface{}, resp interface{}) error {
	c := make(chan error, 1)
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return fmt.Errorf("error dialing %s: %v", address, err)
	}
	defer client.Close()
	go func() { c <- client.Call("Node."+method, req, resp) }()
	select {
		case err := <-c:
			if err != nil {
				return fmt.Errorf("error calling %s on %s: %v", method, address, err)
			}
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout calling %s on %s", method, address)
	}
}

func (n *Node) Alive(args *struct{}, reply *struct{}) error {
	return nil
}

func (n *Node) FindSuccessor(args *FindSuccessorArgs, reply *FindSuccessorReply) error {
	id := args.Id
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, successor := range n.Successors {
		if successor == "" {
			continue
		}
		successorId := HashAddress(successor)
		if id > n.Id && id <= successorId {
			reply.Successor = successor
			return nil
		}
	}
	closestPrecedingNode := n.ClosestPrecedingNode(id)
	// TODO: Double check that this is correct for larger rings
	if closestPrecedingNode == n.Address {
		reply.Successor = n.Successors[0]
		return nil
	}
	successor := CallFindSuccessor(closestPrecedingNode, id)
	reply.Successor = successor
	return nil
}

func (n *Node) Notify(args *NotifyArgs, reply *NotifyReply) error {
	address := args.Address
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Predecessor == "" || address > n.Predecessor && address < n.Id {
		n.Predecessor = address
	}
	return nil
}

func (n *Node) GetPredecessor(args *GetPredecessorArgs, reply *GetPredecessorReply) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	reply.Predecessor = n.Predecessor
	return nil
}

type FindSuccessorArgs struct{
	Id string
}
type FindSuccessorReply struct{
	Successor string
}

type NotifyArgs struct{
	Address string
}
type NotifyReply struct{}

type GetPredecessorArgs struct{}
type GetPredecessorReply struct{
	Predecessor string
}