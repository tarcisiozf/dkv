package main

import (
	"bufio"
	"fmt"
	"github.com/tarcisiozf/dkv/dkv"
	"os"
	"strings"
)

func main() {
	nodes := os.Args[1:]
	if len(nodes) == 0 {
		fmt.Println("Usage: dkv-cli <node addr> <node 2 addr> ...")
		return
	}

	client, err := dkv.NewClient(
		dkv.WithBootstrap(nodes...),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create client: %v", err))
	}
	if err := client.Connect(); err != nil {
		panic(fmt.Errorf("failed to connect to nodes: %v", err))
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		print("> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		parts := strings.Split(input, " ")
		cmd, args := parts[0], parts[1:]
		cmd = strings.TrimSpace(strings.ToLower(cmd))
		if len(cmd) == 0 {
			continue
		}

		switch cmd {
		case "get":
			getCmd(client, args)
			break
		case "set":
			setCmd(client, args)
			break
		case "delete":
			deleteCmd(client, args)
			break
		case "slot":
			slotCmd(client, args)
			break
		case "help":
			printUsage()
			break
		case "info":
			printInfo(client)
			break
		case "exit", "quit":
			return
		default:
			fmt.Println("Unknown command:", cmd)
			fmt.Println("Type 'help' for a list of commands.")
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Errorf("error reading input: %v", err))
	}
}

func slotCmd(client *dkv.Client, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: slot <ley>")
		return
	}

	slotID := client.SlotID(args[0])
	if slotID < 0 {
		fmt.Println("Error: Invalid key or slot ID not found")
		return
	}

	fmt.Printf("Slot ID for key '%s': %d\n", args[0], slotID)
}

func printInfo(client *dkv.Client) {
	fmt.Println("Nodes:")
	for _, node := range client.Nodes() {
		status := "HEALTHY"
		if !node.Healthy {
			status = "UNHEALTHY"
		}
		fmt.Printf("  - %s: %s\n", node.Info.ID, status)
		fmt.Printf("      Address: %s\n", node.Info.Address)
		fmt.Printf("      Port: %s\n", node.Info.Port)
		fmt.Printf("      Slot Range: %s\n", node.Info.SlotRange)
		fmt.Printf("      Role: %s\n", node.Info.Role)
		fmt.Printf("      Status: %s\n", node.Info.Status)
		fmt.Printf("      Mode: %s\n", node.Info.Mode)
		if node.Info.ReplicaOf != "" {
			fmt.Printf("      Replica Of: %s\n", node.Info.ReplicaOf)
		}
		fmt.Println()
	}
}

func getCmd(client *dkv.Client, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: get <key>")
		return
	}

	value, exists, err := client.Get(args[0])
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if !exists {
		fmt.Println("Key not found")
		return
	}
	fmt.Println("Value:", value)
}

func setCmd(client *dkv.Client, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: set <key> <value>")
		return
	}

	err := client.Set(args[0], args[1])
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Value set successfully")
}

func deleteCmd(client *dkv.Client, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: delete <key>")
		return
	}

	err := client.Delete(args[0])
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Key deleted successfully")
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  get <key>: Get the value for the specified key")
	fmt.Println("  set <key> <value>: Set the value for the specified key")
	fmt.Println("  delete <key>: Delete the specified key")
	fmt.Println("  slot <key>: Show the slot ID for the specified key")
	fmt.Println("  info: Show information about the client")
	fmt.Println("  help: Show this help message")
	fmt.Println("  exit or quit: Exit the program")
}
