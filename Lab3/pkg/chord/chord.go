package chord

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
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
	mu             *sync.RWMutex
	SuccessorLimit int
	Address        string
	Id             *big.Int
	Predecessor    string
	Successors     []string
	FingerTable    []string
	Data           map[string]string
	AuthHash       *[32]byte
}

func (n *Node) Lookup(fileName string, password *string) (string, bool, string, string, string) {
	slog.Debug("Looking up file", "node", n.Address, "file", fileName)
	fileHash := Hash(fileName)
	ownerAddress := CallFindSuccessor(n.Address, fileHash)

	if ownerAddress == "" {
		slog.Error("Could not find successor for file", "node", n.Address, "file", fileName, "hash", fileHash.Text(16))
		return "", false, "", "", ""
	}

	content, found := CallGet(ownerAddress, fileName)
	if !found {
		slog.Debug("File not found on owner node", "node", n.Address, "file", fileName, "owner", ownerAddress)
		return "", false, ownerAddress, Hash(ownerAddress).Text(16), ""
	}
	if password == nil && n.AuthHash != nil {
		decryptedContent, err := decryptFileContent([]byte(content), *n.AuthHash)
		if err != nil {
			slog.Error("Error decrypting file content", "node", n.Address, "file", fileName, "owner", ownerAddress, "error", err)
			return "", false, ownerAddress, Hash(ownerAddress).Text(16), ""
		}
		content = string(decryptedContent)
	}
	if password != nil {
		key := hashPassword(password)
		decryptedContent, err := decryptFileContent([]byte(content), *key)
		if err != nil {
			slog.Error("Error decrypting file content", "node", n.Address, "file", fileName, "owner", ownerAddress, "error", err)
			return "", false, ownerAddress, Hash(ownerAddress).Text(16), ""
		}
		content = string(decryptedContent)
	}

	slog.Debug("File found on node", "node", n.Address, "file", fileName, "owner", ownerAddress, "ownerID", Hash(ownerAddress).Text(16))
	return content, true, ownerAddress, Hash(ownerAddress).Text(16), ownerAddress
}

func (n *Node) StoreFile(filePath string) bool {
	fileContentBytes, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("Error reading file", "node", n.Address, "path", filePath, "error", err)
		return false
	}
	fileName := filepath.Base(filePath)
	if n.AuthHash != nil {
		// We interpreted the assignment as all nodes which know the password can read the files.
		encryptedContent, err := encryptFileContent(fileContentBytes, *n.AuthHash)
		slog.Info("Encrypting file before storage", "node", n.Address, "file", fileName)
		if err != nil {
			slog.Error("Error encrypting file content", "node", n.Address, "file", fileName, "error", err)
			return false
		}
		fileContentBytes = encryptedContent
	}
	fileContent := string(fileContentBytes)

	slog.Info("Storing file", "node", n.Address, "file", fileName)
	fileHash := Hash(fileName)
	ownerAddress := CallFindSuccessor(n.Address, fileHash)

	if ownerAddress == "" {
		slog.Error("Could not find successor for file", "node", n.Address, "file", fileName, "hash", fileHash.Text(16))
		return false
	}

	success := CallPut(ownerAddress, fileName, fileContent)
	if success {
		slog.Info("Successfully stored file", "node", n.Address, "file", fileName, "owner", ownerAddress, "ownerID", Hash(ownerAddress).Text(16))
	} else {
		slog.Error("Failed to store file on node", "node", n.Address, "file", fileName, "owner", ownerAddress)
	}
	return success
}

func (n *Node) startRpcServer() {
	rpc.Register(n)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", n.Address)
	if err != nil {
		slog.Error("Error starting RPC server", "error", err)
		return
	}
	slog.Info("RPC server listening", "address", n.Address)
	go http.Serve(l, nil)
}

func StartNode(address string, port int, successorLimit int, identifier string, stabilizationTime int, fixFingerTime int, checkPredTime int, password *string) *Node {
	n := &Node{
		mu:             &sync.RWMutex{},
		SuccessorLimit: successorLimit,
		Address:        fmt.Sprintf("%s:%d", address, port),
		FingerTable:    make([]string, Sha1BitSize),
		Successors:     make([]string, successorLimit),
		AuthHash:       hashPassword(password),
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
	n.Data = make(map[string]string)
	n.Id = Hash(n.Address)
}

func (n *Node) Join(other string) {
	slog.Info("Joining node", "address", other)
	n.mu.Lock()
	defer n.mu.Unlock()
	successor := CallFindSuccessor(other, n.Id)
	if successor == "" {
		slog.Warn("Failed to find successor, cannot join the network")
		slog.Info("Node is now its own successor")
		return
	}

	n.Successors[0] = successor
}

func (n *Node) ClosestPrecedingNode(id *big.Int) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for i := Sha1BitSize - 1; i >= 0; i-- {
		fingerId := Hash(n.FingerTable[i])
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
		slog.Error("Error fixing finger", "finger", next, "error", err)
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
	successorId := Hash(successor)
	if x != "" {
		xId := Hash(x)
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

func Hash(address string) *big.Int {
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
	fmt.Printf("Predecessor ID: %s\n", Hash(n.Predecessor))
	fmt.Println("=====")

	// Successors info
	for i, successor := range n.Successors {
		fmt.Printf("Successor %d: %s\n", i, successor)
		fmt.Printf("Successor %d ID: %s\n", i, Hash(successor))
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
		fmt.Printf("Finger %d ID: %s\n", i, Hash(finger))
		fmt.Println("-----")
	}

	// Stored data info
	fmt.Println("Stored Files:")
	for key := range n.Data {
		fmt.Println(key)
	}
	fmt.Println("=====")
}

func hashPassword(password *string) *[32]byte {
	if password == nil {
		return nil
	}
	sum := sha256.Sum256([]byte(*password))
	return &sum
}

func decryptFileContent(content []byte, password [32]byte) ([]byte, error) {
	decryptedContent, err := decryptAES(content, password)
	if err != nil {
		return nil, err
	}
	return decryptedContent, nil
}

func encryptFileContent(content []byte, password [32]byte) ([]byte, error) {
	encryptedContent, err := encryptAES(content, password)
	if err != nil {
		return nil, err
	}
	return encryptedContent, nil
}

func encryptAES(plaintext []byte, key [32]byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padtext := append(plaintext, bytes.Repeat([]byte{byte(padding)}, padding)...)

	ciphertext := make([]byte, aes.BlockSize+len(padtext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], padtext)

	return ciphertext, nil
}

func decryptAES(ciphertext []byte, key [32]byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(ciphertext, ciphertext)

	padding := int(ciphertext[len(ciphertext)-1])
	if len(ciphertext) < padding {
		return nil, fmt.Errorf("invalid padding size")
	}
	return ciphertext[:len(ciphertext)-padding], nil
}
