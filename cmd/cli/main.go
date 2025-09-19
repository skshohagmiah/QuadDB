package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverAddr string
	timeout    int
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "gomsg",
		Short: "gomsg - Fast data platform CLI",
		Long:  `gomsg is a fast, simple data platform that replaces Redis + RabbitMQ + Kafka`,
	}

	// Global flags
	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "localhost:9000", "Server address")
	rootCmd.PersistentFlags().IntVar(&timeout, "timeout", 30, "Request timeout in seconds")

	// Add subcommands
	rootCmd.AddCommand(kvCmd())
	rootCmd.AddCommand(queueCmd())
	rootCmd.AddCommand(streamCmd())
	rootCmd.AddCommand(clusterCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}