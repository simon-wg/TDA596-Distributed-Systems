package chord

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/rpc"
)

// CallAlive checks if a node at the given address is alive.
func CallAlive(address string) bool {
	if address == "" {
		return false
	}
	req := &struct{}{}
	resp := &struct{}{}
	err := CallCommand(address, "Alive", req, resp)
	if err != nil {
		slog.Warn("node is not alive", "address", address, "error", err)
		return false
	}
	return true
}

// CallFindSuccessor calls the FindSuccessor RPC on the node at the given address.
func CallFindSuccessor(address string, id *big.Int) string {
	req := &FindSuccessorArgs{Id: *id}
	resp := &FindSuccessorReply{}
	err := CallCommand(address, "FindSuccessor", req, resp)
	if err != nil {
		slog.Error("Error finding successor", "error", err)
		return ""
	}
	return resp.Successor
}

//
// Call functions for other RPC methods
//

func CallNotify(address string, nodeAddress string) {
	req := &NotifyArgs{Address: nodeAddress}
	err := CallCommand(address, "Notify", req, &struct{}{})
	if err != nil {
		slog.Error("notifying node", "error", err)
	}
}

func CallGetPredecessor(address string) string {
	resp := &GetPredecessorReply{}
	err := CallCommand(address, "GetPredecessor", struct{}{}, resp)
	if err != nil {
		slog.Error("getting predecessor", "error", err)
		return ""
	}
	return resp.Predecessor
}

func CallGetSuccessors(address string) []string {
	resp := &GetSuccessorsReply{}
	err := CallCommand(address, "GetSuccessors", struct{}{}, resp)
	if err != nil {
		slog.Error("Error getting successors", "error", err)
		return []string{}
	}
	return resp.Successors
}

func CallPut(address string, key string, value string) bool {
	req := &PutArgs{Key: key, Value: value}
	resp := &PutReply{}
	err := CallCommand(address, "Put", req, resp)
	if err != nil {
		slog.Error("Error putting data", "error", err)
		return false
	}
	return resp.Success
}

func CallGet(address string, key string) (string, bool) {
	req := &GetArgs{Key: key}
	resp := &GetReply{}
	err := CallCommand(address, "Get", req, resp)
	if err != nil {
		slog.Error("Error getting data", "error", err)
		return "", false
	}
	return resp.Value, resp.Found
}

func CallGetAll(address string) map[string]string {
	res := &GetAllReply{}
	err := CallCommand(address, "GetAll", struct{}{}, res)
	if err != nil {
		slog.Error("Error getting all data", "error", err)
		return nil
	}
	return res.Data
}

func CallTransferData(address string, data map[string]string) bool {
	req := &TransferDataArgs{Data: data}
	resp := &TransferDataReply{}
	err := CallCommand(address, "TransferData", req, resp)
	if err != nil {
		slog.Error("Error transferring data", "error", err)
		return false
	}
	return resp.Success
}

// CallCommand is a helper function to call an RPC method on a node at the given address.
func CallCommand(address string, method string, req interface{}, resp interface{}) error {
	conn, err := tls.Dial("tcp", address, &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return fmt.Errorf("error dialing %s: %v", address, err)
	}
	defer conn.Close()

	// Manually perform the HTTP CONNECT handshake
	// This is basically what rpc.DialHTTP does, but we need a manual connection with tls so we have to do it ourselves
	// DefaultRPCPath is "/_goRPC_"
	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")

	// Read the response from the server
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		return fmt.Errorf("error reading RPC handshake response: %v", err)
	}
	if response.Status != "200 Connected to Go RPC" && response.StatusCode != 200 {
		return fmt.Errorf("unexpected RPC handshake response: %s", response.Status)
	}

	// Creates new RPC client
	client := rpc.NewClient(conn)
	defer client.Close()
	// From this point on we can use the client as if it was a normal RPC client

	err = client.Call("Node."+method, req, resp)
	if err != nil {
		return fmt.Errorf("error calling %s on %s: %v", method, address, err)
	}
	return nil
}

//
// RPC method implementations
//

// Alive checks if the node is alive.
func (n *Node) Alive(args *struct{}, reply *struct{}) error {
	return nil
}

// FindSuccessor finds the successor of the given ID.
func (n *Node) FindSuccessor(args *FindSuccessorArgs, reply *FindSuccessorReply) error {
	id := args.Id
	n.mu.RLock()

	// Check if the ID is between this node and its first successor
	for _, successor := range n.successors {
		if successor == "" {
			continue
		}
		successorIdInclusive := new(big.Int).Add(Hash(successor), big.NewInt(1))
		if IsBetween(&id, n.id, successorIdInclusive) {
			reply.Successor = successor
			n.mu.RUnlock()
			return nil
		}
	}
	n.mu.RUnlock()
	closestPrecedingNode := n.ClosestPrecedingNode(&id)
	n.mu.RLock()

	// If the closest preceding node is this node, return the first successor
	if closestPrecedingNode == n.address {
		reply.Successor = n.successors[0]
		n.mu.RUnlock()
		return nil
	}

	// Release the read lock before making the RPC call
	n.mu.RUnlock()
	successor := CallFindSuccessor(closestPrecedingNode, &id)
	reply.Successor = successor
	return nil
}

// Notify notifies the node about a potential predecessor.
func (n *Node) Notify(args *NotifyArgs, reply *struct{}) error {
	address := args.Address
	id := Hash(address)
	n.mu.Lock()
	defer n.mu.Unlock()
	predId := Hash(n.predecessor)
	if n.predecessor == "" || IsBetween(id, predId, n.id) {
		n.predecessor = address
	}
	return nil
}

// GetPredecessor returns the predecessor of the node.
func (n *Node) GetPredecessor(args struct{}, reply *GetPredecessorReply) error {
	reply.Predecessor = n.ReadPredecessor()
	return nil
}

// GetSuccessors returns the successors of the node.
func (n *Node) GetSuccessors(args struct{}, reply *GetSuccessorsReply) error {
	reply.Successors = n.ReadSuccessors()
	return nil
}

// Put stores a key-value pair in the node's data store.
func (n *Node) Put(args *PutArgs, reply *PutReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.data[args.Key] = args.Value
	reply.Success = true
	return nil
}

// Get retrieves a value by key from the node's data store.
func (n *Node) Get(args *GetArgs, reply *GetReply) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	val, ok := n.data[args.Key]
	reply.Value = val
	reply.Found = ok
	return nil
}

// GetAll retrieves all key-value pairs from the node's data store.
func (n *Node) GetAll(args struct{}, reply *GetAllReply) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	reply.Data = make(map[string]string)
	for k, v := range n.data {
		reply.Data[k] = v
	}
	return nil
}

// TransferData transfers key-value pairs to the node's data store.
func (n *Node) TransferData(args *TransferDataArgs, reply *TransferDataReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, v := range args.Data {
		n.data[k] = v
	}
	reply.Success = true
	return nil
}

//
// // RPC argument and reply types
//

type GetAllReply struct {
	Data map[string]string
}

type TransferDataArgs struct {
	Data map[string]string
}

type TransferDataReply struct {
	Success bool
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

type GetPredecessorReply struct {
	Predecessor string
}

type GetSuccessorsReply struct {
	Successors []string
}

type PutArgs struct {
	Key   string
	Value string
}
type PutReply struct {
	Success bool
}

type GetArgs struct {
	Key string
}
type GetReply struct {
	Value string
	Found bool
}
