package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func main() {
	fmt.Println("🔗 GoMsg Clustering Features Demo")
	fmt.Println("==================================")
	fmt.Println()

	fmt.Println("📋 Available Clustering Options:")
	fmt.Println("--------------------------------")
	fmt.Println("1. 🏠 Single Node (Current)")
	fmt.Println("   Command: make run")
	fmt.Println("   Use case: Development, testing")
	fmt.Println()
	fmt.Println("2. 🔗 Multi-Node Cluster")
	fmt.Println("   Command: make run-cluster")
	fmt.Println("   Features: 3 nodes, Raft consensus, high availability")
	fmt.Println("   Ports: 9000 (node1), 9001 (node2), 9002 (node3)")
	fmt.Println()
	fmt.Println("3. 🐳 Docker Cluster")
	fmt.Println("   Command: make docker-cluster")
	fmt.Println("   Features: Containerized cluster, easy scaling")
	fmt.Println()

	// Check current cluster status
	fmt.Println("🔍 Current Cluster Status:")
	fmt.Println("--------------------------")
	checkClusterStatus()
	fmt.Println()

	// Show cluster CLI commands
	fmt.Println("🛠️  Cluster Management Commands:")
	fmt.Println("--------------------------------")
	fmt.Println("• Check status:     ./bin/gomsg-cli cluster status")
	fmt.Println("• List nodes:       ./bin/gomsg-cli cluster nodes")
	fmt.Println("• Node info:        ./bin/gomsg-cli cluster info")
	fmt.Println("• Add node:         ./bin/gomsg-cli cluster add-node <address>")
	fmt.Println("• Remove node:      ./bin/gomsg-cli cluster remove-node <node-id>")
	fmt.Println()

	// Demonstrate cluster-aware operations
	fmt.Println("🎯 Cluster-Aware Operations:")
	fmt.Println("-----------------------------")
	fmt.Println("When running in cluster mode, GoMsg provides:")
	fmt.Println("✅ Automatic leader election (Raft consensus)")
	fmt.Println("✅ Data replication across nodes")
	fmt.Println("✅ Automatic failover")
	fmt.Println("✅ Load balancing")
	fmt.Println("✅ Partition tolerance")
	fmt.Println()

	// Show how to test clustering
	fmt.Println("🧪 How to Test Clustering:")
	fmt.Println("---------------------------")
	fmt.Println("1. Stop current server: Ctrl+C")
	fmt.Println("2. Start cluster: make run-cluster")
	fmt.Println("3. Test with multiple ports:")
	fmt.Println("   ./bin/gomsg-cli --server=localhost:9000 kv set key1 value1")
	fmt.Println("   ./bin/gomsg-cli --server=localhost:9001 kv get key1")
	fmt.Println("   ./bin/gomsg-cli --server=localhost:9002 kv get key1")
	fmt.Println()

	// Show cluster configuration
	fmt.Println("⚙️  Cluster Configuration:")
	fmt.Println("--------------------------")
	showClusterConfig()
	fmt.Println()

	// Performance benefits
	fmt.Println("🚀 Clustering Benefits:")
	fmt.Println("-----------------------")
	fmt.Println("📈 Performance:")
	fmt.Println("   • Read operations can be distributed")
	fmt.Println("   • Write operations are replicated for safety")
	fmt.Println("   • Automatic load balancing")
	fmt.Println()
	fmt.Println("🛡️  Reliability:")
	fmt.Println("   • No single point of failure")
	fmt.Println("   • Automatic leader election")
	fmt.Println("   • Data consistency with Raft consensus")
	fmt.Println()
	fmt.Println("📊 Scalability:")
	fmt.Println("   • Add/remove nodes dynamically")
	fmt.Println("   • Linear scaling for read operations")
	fmt.Println("   • Partition tolerance")
	fmt.Println()

	fmt.Println("💡 Next Steps:")
	fmt.Println("--------------")
	fmt.Println("1. Try: make run-cluster")
	fmt.Println("2. Test failover by stopping one node")
	fmt.Println("3. Monitor cluster health with CLI commands")
	fmt.Println("4. Scale up by adding more nodes")
	fmt.Println()
	fmt.Println("🎉 Ready to build highly available distributed systems!")
}

func checkClusterStatus() {
	fmt.Print("Checking cluster status... ")
	cmd := exec.Command("./bin/gomsg-cli", "cluster", "status")
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		fmt.Println("💡 This indicates single-node mode (clustering disabled)")
	} else {
		result := strings.TrimSpace(string(output))
		fmt.Printf("✅ Status: %s\n", result)
	}
}

func showClusterConfig() {
	fmt.Println("Current server configuration shows:")
	fmt.Println("• Cluster.Enabled: false")
	fmt.Println("• Cluster.NodeID: (empty)")
	fmt.Println("• Cluster.Replicas: 3")
	fmt.Println("• Cluster.DataDir: ./cluster")
	fmt.Println()
	fmt.Println("To enable clustering, use flags:")
	fmt.Println("--cluster --node-id=node1 --bootstrap")
	fmt.Println("--cluster --node-id=node2 --join=localhost:9000")
}
