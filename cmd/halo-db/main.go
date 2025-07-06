package main

import (
	"bufio"
	"fmt"
	"halo-db/pkg/constants"
	"halo-db/pkg/partition"
	"halo-db/pkg/types"
	"os"
	"strings"
	"unicode"
)

func main() {
	pm, err := partition.NewPartitionManager(constants.NumPartitions, constants.DataDir)
	if err != nil {
		fmt.Printf("Failed to initialize partition manager: %v\n", err)
		os.Exit(1)
	}

	defer func() {
		if err := pm.Close(); err != nil {
			fmt.Printf("Error closing partition manager: %v\n", err)
		}
	}()

	fmt.Printf("HaloDB - Partitioned Key-Value Store (%d partitions)\n", constants.NumPartitions)
	fmt.Println("Commands: put <key> <value>, get <key>, delete <key>, list, clear, stats, tree, quit")
	fmt.Println("Note: Use quotes for values with spaces: put key \"value with spaces\"")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("halo-db> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := parseCommand(input)
		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		switch command {
		case "quit", "exit":
			return
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				fmt.Println("Example: put user:1925 \"Halil Bülent Orhon\"")
				continue
			}
			key := parts[1]
			value := types.Value(parts[2])
			if err := pm.Put(key, value); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			value, err := pm.Get(key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s\n", string(value))
			}
		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := parts[1]
			if err := pm.Delete(key); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "list":
			keys := pm.List()
			if len(keys) == 0 {
				fmt.Println("No keys found")
			} else {
				for _, key := range keys {
					fmt.Println(key)
				}
			}
		case "clear":
			if err := pm.Clear(); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "stats":
			stats := pm.GetStats()
			for key, value := range stats {
				fmt.Printf("%s: %v\n", key, value)
			}
		case "tree":
			stats := pm.GetStats()
			fmt.Printf("Total keys: %v\n", stats["total_keys"])
			fmt.Printf("Partitions: %v\n", stats["num_partitions"])
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

func parseCommand(input string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	escapeNext := false

	for _, r := range input {
		if escapeNext {
			current.WriteRune(r)
			escapeNext = false
			continue
		}

		if r == '\\' {
			escapeNext = true
			continue
		}

		if r == '"' {
			inQuotes = !inQuotes
			continue
		}

		if unicode.IsSpace(r) && !inQuotes {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}
