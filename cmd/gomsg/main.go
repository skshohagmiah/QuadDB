package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gomsg/config"
	"gomsg/pkg/server"
	"gomsg/storage"
)

var (
	configPath = flag.String("config", "", "Path to configuration file")
	dataDir    = flag.String("data-dir", "./data", "Data directory")
	port       = flag.Int("port", 9000, "Server port")
	host       = flag.String("host", "localhost", "Server host")
	nodeID     = flag.String("node-id", "", "Node ID for clustering")
	bootstrap  = flag.Bool("bootstrap", false, "Bootstrap cluster")
	join       = flag.String("join", "", "Join existing cluster")
	cluster    = flag.Bool("cluster", false, "Enable clustering")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		cfg = config.GetDefaultConfig()
		log.Printf("Using default configuration: %v", err)
	}

	// Override config with command line flags
	if *dataDir != "./data" {
		cfg.Storage.DataDir = *dataDir
	}
	if *port != 9000 {
		cfg.Server.Port = *port
	}
	if *host != "localhost" {
		cfg.Server.Host = *host
	}
	if *cluster {
		cfg.Cluster.Enabled = true
		if *nodeID != "" {
			cfg.Cluster.NodeID = *nodeID
		}
		if *bootstrap {
			cfg.Cluster.Bootstrap = true
		}
		if *join != "" {
			cfg.Cluster.JoinAddresses = []string{*join}
		}
	}

	// Initialize storage
	store, err := storage.NewBadgerStorage(cfg.Storage.DataDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Create and start server
	srv, err := server.NewServer(cfg, store)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Start server
	log.Printf("Starting gomsg server with config: %+v", cfg)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("gomsg server stopped")
}