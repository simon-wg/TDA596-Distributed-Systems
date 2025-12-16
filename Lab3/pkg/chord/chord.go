package chord

import (
	"crypto/sha1"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
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
	mu                *sync.RWMutex
	successorLimit    int
	stabilizationTime int
	fixFingerTime     int
	checkPredTime     int
	address           string
	port              int
	id                *big.Int
	predecessor       string
	successors        []string
	fingerTable       []string
	data              map[string]string
	backup            map[string]string
}

// Lookup looks up a file in the chord and returns its content if found.
func (n *Node) Lookup(fileName string, password *string) (string, bool, string, string, string) {
	slog.Debug("Looking up file", "node", n.address, "file", fileName)
	fileHash := Hash(fileName)
	ownerAddress := CallFindSuccessor(n.address, fileHash)

	// If no owner found, return not found
	if ownerAddress == "" {
		slog.Error("Could not find successor for file", "node", n.address, "file", fileName, "hash", fileHash.Text(16))
		return "", false, "", "", ""
	}

	// Try to get the file from the owner node
	content, found := CallGet(ownerAddress, fileName)
	if !found {
		slog.Debug("File not found on owner node", "node", n.address, "file", fileName, "owner", ownerAddress)
		return "", false, ownerAddress, Hash(ownerAddress).Text(16), ""
	}

	// If password is provided, decrypt the content
	if password != nil {
		key := hashPassword(password)
		decryptedContent, err := decryptFileContent([]byte(content), *key)
		if err != nil {
			slog.Error("Error decrypting file content", "node", n.address, "file", fileName, "owner", ownerAddress, "error", err)
			return "", false, ownerAddress, Hash(ownerAddress).Text(16), ""
		}
		content = string(decryptedContent)
	}

	slog.Debug("File found on node", "node", n.address, "file", fileName, "owner", ownerAddress, "ownerID", Hash(ownerAddress).Text(16))
	return content, true, ownerAddress, Hash(ownerAddress).Text(16), ownerAddress
}

// StoreFile stores a file on a node in the chord, optionally encrypting it with a password.
func (n *Node) StoreFile(filePath string, password *string) bool {

	// Read file content
	fileContentBytes, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("Error reading file", "node", n.address, "path", filePath, "error", err)
		return false
	}

	// Encrypt file content if password is provided
	fileName := filepath.Base(filePath)
	if password != nil {
		key := hashPassword(password)
		encryptedContent, err := encryptFileContent(fileContentBytes, *key)
		slog.Info("Encrypting file before storage", "node", n.address, "file", fileName)
		if err != nil {
			slog.Error("Error encrypting file content", "node", n.address, "file", fileName, "error", err)
			return false
		}
		fileContentBytes = encryptedContent
	}
	fileContent := string(fileContentBytes)

	// Find the owner node for the file
	slog.Info("Storing file", "node", n.address, "file", fileName)
	fileHash := Hash(fileName)
	ownerAddress := CallFindSuccessor(n.address, fileHash)

	// If no owner found, return failure
	if ownerAddress == "" {
		slog.Error("Could not find successor for file", "node", n.address, "file", fileName, "hash", fileHash.Text(16))
		return false
	}

	// Store the file on the owner node
	success := CallPut(ownerAddress, fileName, fileContent)
	if success {
		slog.Info("Successfully stored file", "node", n.address, "file", fileName, "owner", ownerAddress, "ownerID", Hash(ownerAddress).Text(16))
	} else {
		slog.Error("Failed to store file on node", "node", n.address, "file", fileName, "owner", ownerAddress)
	}
	return success
}

// startRpcServer starts the RPC server for the node with TLS encryption.
func (n *Node) startRpcServer() {
	rpc.Register(n)
	rpc.HandleHTTP()

	// Generate the in-memory self-signed certificate
	cert, err := generateSelfSignedCert()
	if err != nil {
		slog.Error("Error generating self-signed certificate", "error", err)
		return
	}

	// Configure TLS with the cert
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// Listen using tls.Listen instead of net.Listen
	l, err := tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", n.port), tlsConfig)
	if err != nil {
		slog.Error("Error starting RPC server", "error", err)
		return
	}

	slog.Info("RPC server listening (Secure)", "address", n.address)

	// http.Serve will automatically handle HTTPS because the listener 'l' is a TLS listener
	go http.Serve(l, nil)
}

// Initializes a new Node with the given parameters.
func InitNode(address net.IP, port int, successorLimit int, identifier string, stabilizationTime int, fixFingerTime int, checkPredTime int) *Node {
	n := &Node{
		mu:                &sync.RWMutex{},
		successorLimit:    successorLimit,
		stabilizationTime: stabilizationTime,
		fixFingerTime:     fixFingerTime,
		checkPredTime:     checkPredTime,
		address:           fmt.Sprintf("%s:%d", address.String(), port),
		port:              port,
		id:                Hash(fmt.Sprintf("%s:%d", address.String(), port)),
		fingerTable:       make([]string, Sha1BitSize),
		successors:        make([]string, successorLimit),
	}
	if identifier != "" {
		n.id = new(big.Int)
		n.id.SetString(identifier, 16)
	}
	n.startRpcServer()
	return n
}

// Creates a new chord with this node as the only member.
func (n *Node) Create() {
	n.mu.Lock()
	n.predecessor = ""
	n.successors[0] = n.address
	for i := 1; i < len(n.successors); i++ {
		n.successors[i] = ""
	}
	n.data = make(map[string]string)
	n.mu.Unlock()
	n.startBackgroundRoutines()
}

// Joins an existing chord network via the given node address.
func (n *Node) Join(other string) {
	n.mu.Lock()
	n.predecessor = ""
	n.data = make(map[string]string)
	nId := n.id
	n.mu.Unlock()
	successor := CallFindSuccessor(other, nId)
	if successor == "" {
		slog.Warn("Failed to find successor, cannot join the network")
		slog.Info("Node is now its own successor")
		return
	}
	n.mu.Lock()
	n.successors[0] = successor
	n.mu.Unlock()
	n.startBackgroundRoutines()
}

// Returns the closest preceding node for the given ID from the finger table.
func (n *Node) ClosestPrecedingNode(id *big.Int) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for i := Sha1BitSize - 1; i >= 0; i-- {
		if n.fingerTable[i] == "" {
			continue
		}
		fingerId := Hash(n.fingerTable[i])
		if IsBetween(fingerId, n.id, id) {
			return n.fingerTable[i]
		}
	}
	return n.address
}

// Fixes the next finger in the finger table.
func (n *Node) fixFingers() {
	n.mu.Lock()
	next = (next + 1) % Sha1BitSize
	id := n.id
	nextId := new(big.Int).Add(id, new(big.Int).Lsh(big.NewInt(1), uint(next)))
	args := &FindSuccessorArgs{
		Id: *nextId,
	}
	reply := &FindSuccessorReply{}
	n.mu.Unlock()
	err := n.FindSuccessor(args, reply)
	if err != nil {
		slog.Error("Error fixing finger", "finger", next, "error", err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fingerTable[next] = reply.Successor
}

// Stabilizes the node by checking and updating its successor and notifying it.
func (n *Node) stabilize() {
	var x string
	successor := n.ReadSuccessor()

	for {
		if successor != n.address && !CallAlive(successor) {
			successor = n.PopSuccessor()
		} else {
			break
		}
	}
	n.UpdateSuccessors(CallGetSuccessors(successor), successor)
	// Get successor's predecessor
	if successor == n.address {
		x = n.ReadPredecessor()
	} else {
		x = CallGetPredecessor(successor)
	}
	// Check if x is between n and n's successor
	successorId := Hash(successor)
	if x != "" {
		xId := Hash(x)
		nId := n.ReadID()
		if IsBetween(xId, nId, successorId) {
			n.mu.Lock()
			n.successors[0] = x
			successor = x
			n.mu.Unlock()
		}
	}

	// Notify successor
	if successor == n.address {
		args := &NotifyArgs{
			Address: n.address,
		}
		n.Notify(args, &struct{}{})
	} else {
		CallNotify(n.successors[0], n.address)
	}
}

// Checks if the predecessor is alive and updates data replication if necessary.
func (n *Node) checkPredecessor() {
	pred := n.ReadPredecessor()

	if pred != "" && pred != n.address {
		if !CallAlive(pred) {
			n.mu.Lock()
			for key := range n.backup {
				n.data[key] = n.backup[key]
			}
			if n.predecessor == pred {
				n.predecessor = ""
			}
			n.mu.Unlock()
		} else {
			n.Replicate()
		}
	}
}

// Starts the background routines for stabilization, fixing fingers, and checking predecessor.
func (n *Node) startBackgroundRoutines() {
	n.mu.RLock()
	stabilizationTime := n.stabilizationTime
	fixFingerTime := n.fixFingerTime
	checkPredTime := n.checkPredTime
	n.mu.RUnlock()
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

// Hashes the given address using SHA1 and returns a big.Int representation.
func Hash(address string) *big.Int {
	// SHA1 is considered insecure for cryptographic purposes, but is sufficient for generating node IDs in a Chord DHT.
	digest := sha1.Sum([]byte(address))
	id := new(big.Int).SetBytes(digest[:])
	return id
}

// Checks if id is between start and end in the identifier circle.
func IsBetween(id, start, end *big.Int) bool {
	if start.Cmp(end) < 0 {
		return id.Cmp(start) > 0 && id.Cmp(end) < 0
	} else {
		return id.Cmp(start) > 0 || id.Cmp(end) < 0
	}
}

// Replicates data to the predecessor node.
func (n *Node) Replicate() {
	pred := n.ReadPredecessor()
	if pred == "" {
		return
	}
	data := CallGetAll(pred)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.predecessor != pred {
		return
	}
	n.backup = data
	n.pruneData()
}

// Prunes data if it no longer belongs to this node and transfers it to the predecessor.
func (n *Node) pruneData() {
	pred := n.predecessor
	if pred == "" {
		return
	}
	predId := Hash(pred)
	nIdInclusive := new(big.Int).Add(n.id, big.NewInt(1))

	toTransfer := make(map[string]string)
	for key, value := range n.data {
		keyHash := Hash(key)
		if !IsBetween(keyHash, predId, nIdInclusive) {
			toTransfer[key] = value
		}
	}

	if len(toTransfer) > 0 {
		n.mu.Unlock()
		success := CallTransferData(pred, toTransfer)
		n.mu.Lock()
		if success {
			for key := range toTransfer {
				delete(n.data, key)
			}
		} else {
			slog.Warn("Failed to transfer data to predecessor during prune", "pred", pred)
		}
	}
}

// Pops the first successor from the successor list and shifts the others.
func (n *Node) PopSuccessor() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < len(n.successors)-1; i++ {
		n.successors[i] = n.successors[i+1]
	}
	n.successors[len(n.successors)-1] = ""
	return n.successors[0]
}

// Updates the successor list with the given successors and new successor.
func (n *Node) UpdateSuccessors(ns []string, s string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.successors[0] = s
	if len(ns) != len(n.successors) {
		return
	}
	if slices.Compare(n.successors[1:], ns[:len(ns)-1]) == 0 {
		return
	}
	for i := 0; i < len(ns)-1 && i+1 < len(n.successors); i++ {
		n.successors[i+1] = ns[i]
	}
}

// Reads the predecessor address.
func (n *Node) ReadPredecessor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.predecessor
}

// Reads the first successor address.
func (n *Node) ReadSuccessor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.successors) == 0 {
		return ""
	}
	return n.successors[0]
}

// Reads the list of successor addresses.
func (n *Node) ReadSuccessors() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	successors := make([]string, len(n.successors))
	copy(successors, n.successors)
	return successors
}

// Reads the node ID.
func (n *Node) ReadID() *big.Int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) PrintState() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Client info
	fmt.Println("=====")
	fmt.Printf("Node ID: %s\n", n.id)
	fmt.Printf("Node Address: %s\n", n.address)
	fmt.Println("=====")
	fmt.Printf("Predecessor: %s\n", n.predecessor)
	fmt.Printf("Predecessor ID: %s\n", Hash(n.predecessor))
	fmt.Println("=====")

	// Successors info
	for i, successor := range n.successors {
		fmt.Printf("Successor %d: %s\n", i, successor)
		fmt.Printf("Successor %d ID: %s\n", i, Hash(successor))
		fmt.Println("-----")
	}

	// Fingertable info
	for i, _ := range n.fingerTable {
		finger := n.fingerTable[i]
		if finger == "" {
			continue
		}
		fmt.Printf("Finger %d: %s\n", i, finger)
		fmt.Printf("Finger %d ID: %s\n", i, Hash(finger))
		fmt.Println("-----")
	}

	// Stored data info
	fmt.Println("Stored Files:")
	for key := range n.data {
		fmt.Println(key)
	}
	fmt.Println("=====")

	// Mirrored data info
	fmt.Println("Mirrored Files:")
	for key := range n.backup {
		fmt.Println(key)
	}
	fmt.Println("=====")
}
