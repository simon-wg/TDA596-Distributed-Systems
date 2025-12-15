package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"raft/pkg/chord"
	"strings"
)

func main() {
	address := flag.String("a", "", "Address to listen on")
	port := flag.Int("p", -1, "Port to listen on")
	joinAddress := flag.String("ja", "", "Address to join")
	joinPort := flag.Int("jp", -1, "Port to join")
	stabilizationTime := flag.Int("ts", -1, "Time in milliseconds between invocation of stabilize")
	fixFingerTime := flag.Int("tff", -1, "Time in milliseconds between invocations of fix fingers")
	checkPredTime := flag.Int("tcp", -1, "Time in milliseconds between invocation of check predecess")
	successorLimit := flag.Int("r", -1, "Number of successors maintained by chord client")
	identifier := flag.String("i", "", "The identifier assigned to Chord client. Overwrites ID computed by SHA1 sum of IP and Port.")

	flag.Parse()

	// TODO: Add validation for address

	if *port == -1 {
		fmt.Println("Please provide a port using -p flag")
		return
	}

	// TODO: Add validation for join port and join address

	if *stabilizationTime == -1 || *stabilizationTime < 1 || *stabilizationTime > 60000 {
		fmt.Println("Please specify stabilization time in the range [1,60000] using the --ts flag")
		return
	}
	if *fixFingerTime == -1 || *fixFingerTime < 1 || *fixFingerTime > 60000 {
		fmt.Println("Please specify time to fix fingers in the range [1,60000] using the --tff flag")
		return
	}
	if *checkPredTime == -1 || *checkPredTime < 1 || *checkPredTime > 60000 {
		fmt.Println("Please specify 'check predeccessor' time in the range [1,60000] using the --tcp flag")
		return
	}
	if *successorLimit == -1 || *successorLimit < 1 || *successorLimit > 32 {
		fmt.Println("Please specify successor limit in the range [1,32] using the --r flag")
		return
	}
	if *identifier != "" && len(*identifier) != 40 && !isHexString(*identifier) {
		fmt.Println("Identifier must be a 40 character long hex string")
		return
	}

	node := chord.InitNode(*address, *port, *successorLimit, *identifier, *stabilizationTime, *fixFingerTime, *checkPredTime)
	if joinAddress != nil && *joinPort != -1 {
		node.Join(fmt.Sprintf("%s:%d", *joinAddress, *joinPort))
	} else {
		node.Create()
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if len(input) == 0 {
			fmt.Println("Please enter a command")
			continue
		}
		slices := strings.Fields(input)
		cmd := slices[0]

		switch cmd {
		case "l":
			fallthrough
		case "Lookup":
			if len(slices) < 2 {
				fmt.Println("Please provide a file name to lookup")
			}
			fileName := slices[1]
			var content, ownerAddress, ownerId, ipPort string
			var found bool
			if len(slices) > 2 {
				content, found, ownerAddress, ownerId, ipPort = node.Lookup(fileName, &slices[2])
			} else {
				content, found, ownerAddress, ownerId, ipPort = node.Lookup(fileName, nil)
			}
			if found {
				fmt.Printf("Found file '%s' on node %s (ID: %s)\n", fileName, ownerAddress, ownerId)
				fmt.Printf("IP Address: %s\n", strings.Split(ipPort, ":")[0])
				fmt.Printf("Port: %s\n", strings.Split(ipPort, ":")[1])
				fmt.Printf("Content:\n%s\n", content)
			} else {
				fmt.Printf("File '%s' not found.\n", fileName)
				if ownerAddress != "" {
					fmt.Printf("Owner node: %s (ID: %s)\n", ownerAddress, ownerId)
				}
			}
		case "s":
			fallthrough
		case "StoreFile":
			if len(slices) < 2 {
				fmt.Println("Please provide a file path to store")
			}
			filePath := slices[1]
			if len(slices) > 2 {
				node.StoreFile(filePath, &slices[2])
			} else {
				node.StoreFile(filePath, nil)
			}
		case "p":
			fallthrough
		case "PrintState":
			node.PrintState()
		default:
			slog.Warn("Unknown command")
		}
		continue
	}
}
func isHexString(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
