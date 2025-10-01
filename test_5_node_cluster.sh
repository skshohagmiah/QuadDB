#!/bin/bash

echo "🚀 GoMsg 5-Node Cluster Test"
echo "============================"
echo

# Clean up any existing data
echo "🧹 Cleaning up existing data..."
rm -rf data cluster logs pids 2>/dev/null
mkdir -p data/{node1,node2,node3,node4,node5} logs pids

# Function to wait for server to start
wait_for_server() {
    local port=$1
    local node_name=$2
    local max_attempts=30
    local attempt=1
    
    echo "  Waiting for $node_name on port $port..."
    while [ $attempt -le $max_attempts ]; do
        if ./bin/gomsg-cli --server=localhost:$port cluster status >/dev/null 2>&1; then
            echo "  ✅ $node_name is ready and responding"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    
    echo "  ❌ $node_name failed to start after $max_attempts seconds"
    return 1
}

# Function to stop all nodes
stop_all_nodes() {
    echo "🛑 Stopping all cluster nodes..."
    
    if [ -d "pids" ]; then
        for pidfile in pids/*.pid; do
            if [ -f "$pidfile" ]; then
                local pid=$(cat "$pidfile")
                local node=$(basename "$pidfile" .pid)
                echo "  Stopping $node (PID: $pid)..."
                kill $pid 2>/dev/null || true
                rm "$pidfile"
            fi
        done
    fi
    
    # Also kill any remaining gomsg processes
    pkill -f "gomsg.*--cluster" 2>/dev/null || true
    sleep 2
}

# Function to start a node
start_node() {
    local node_id=$1
    local port=$2
    local bootstrap=$3
    local join_addr=$4
    
    echo "🚀 Starting $node_id on port $port..."
    
    local cmd="./bin/gomsg --cluster --node-id=$node_id --port=$port --data-dir=./data/$node_id"
    
    if [ "$bootstrap" = "true" ]; then
        cmd="$cmd --bootstrap"
        echo "  Bootstrap node: $node_id"
    elif [ -n "$join_addr" ]; then
        cmd="$cmd --join=$join_addr"
        echo "  Joining cluster at: $join_addr"
    fi
    
    # Start in background
    $cmd > logs/$node_id.log 2>&1 &
    local pid=$!
    echo $pid > pids/$node_id.pid
    
    echo "  Started $node_id with PID $pid"
}

# Function to test cluster status across all nodes
test_cluster_status() {
    echo "📊 Testing cluster status across all nodes..."
    
    for port in 9000 9001 9002 9003 9004; do
        local node_num=$((port - 8999))
        echo "  Node$node_num (port $port):"
        local status=$(./bin/gomsg-cli --server=localhost:$port cluster status 2>/dev/null)
        if [ $? -eq 0 ]; then
            echo "$status" | sed 's/^/    /'
        else
            echo "    ❌ Failed to get status"
        fi
        echo
    done
}

# Function to test cluster operations
test_cluster_operations() {
    echo "🧪 Testing cluster operations across nodes..."
    
    # Test KV operations
    echo "  Testing KV operations..."
    ./bin/gomsg-cli --server=localhost:9000 kv set test-key "Hello from 5-node cluster!"
    sleep 1
    
    # Try to read from different nodes
    for port in 9000 9001 9002 9003 9004; do
        local node_num=$((port - 8999))
        local value=$(./bin/gomsg-cli --server=localhost:$port kv get test-key 2>/dev/null)
        if [ "$value" = "Hello from 5-node cluster!" ]; then
            echo "    ✅ Node$node_num: KV read successful"
        else
            echo "    ⚠️  Node$node_num: KV read failed or inconsistent"
        fi
    done
    
    # Test Queue operations
    echo "  Testing Queue operations..."
    ./bin/gomsg-cli --server=localhost:9001 queue push test-queue "Message from 5-node cluster"
    sleep 1
    
    local msg=$(./bin/gomsg-cli --server=localhost:9002 queue pop test-queue 2>/dev/null | grep "Data:" | cut -d' ' -f2-)
    if [ "$msg" = "Message from 5-node cluster" ]; then
        echo "    ✅ Queue operations working across nodes"
    else
        echo "    ❌ Queue operations failed: msg='$msg'"
    fi
    
    # Test Stream operations
    echo "  Testing Stream operations..."
    ./bin/gomsg-cli --server=localhost:9003 stream publish test-stream "Event from 5-node cluster"
    echo "    ✅ Stream operations completed"
}

# Function to test leader election
test_leader_election() {
    echo "🗳️  Testing leader election..."
    
    # Find current leader
    local leader_port=""
    for port in 9000 9001 9002 9003 9004; do
        local status=$(./bin/gomsg-cli --server=localhost:$port cluster status 2>/dev/null)
        if echo "$status" | grep -q "Raft State: leader"; then
            leader_port=$port
            break
        fi
    done
    
    if [ -n "$leader_port" ]; then
        local node_num=$((leader_port - 8999))
        echo "  Current leader: Node$node_num (port $leader_port)"
        
        # Get leader info from cluster
        local leader_info=$(./bin/gomsg-cli --server=localhost:$leader_port cluster status | grep "Leader:")
        echo "  $leader_info"
    else
        echo "  ❌ No leader found"
    fi
}

# Function to show cluster nodes
show_cluster_nodes() {
    echo "👥 Cluster nodes:"
    ./bin/gomsg-cli --server=localhost:9000 cluster nodes 2>/dev/null | sed 's/^/  /'
}

# Main execution
main() {
    # Setup
    trap stop_all_nodes EXIT
    
    echo "🏁 Starting 5-node GoMsg cluster..."
    echo
    
    # Step 1: Start bootstrap node
    start_node "node1" 9000 "true" ""
    if ! wait_for_server 9000 "Node1"; then
        echo "❌ Failed to start bootstrap node"
        exit 1
    fi
    
    sleep 3
    echo
    
    # Step 2: Start node2
    start_node "node2" 9001 "false" "localhost:9000"
    if ! wait_for_server 9001 "Node2"; then
        echo "⚠️  Node2 failed to start, but continuing..."
    fi
    
    sleep 3
    echo
    
    # Step 3: Start node3
    start_node "node3" 9002 "false" "localhost:9000"
    if ! wait_for_server 9002 "Node3"; then
        echo "⚠️  Node3 failed to start, but continuing..."
    fi
    
    sleep 3
    echo
    
    # Step 4: Start node4
    start_node "node4" 9003 "false" "localhost:9000"
    if ! wait_for_server 9003 "Node4"; then
        echo "⚠️  Node4 failed to start, but continuing..."
    fi
    
    sleep 3
    echo
    
    # Step 5: Start node5
    start_node "node5" 9004 "false" "localhost:9000"
    if ! wait_for_server 9004 "Node5"; then
        echo "⚠️  Node5 failed to start, but continuing..."
    fi
    
    sleep 5
    echo
    
    echo "✅ 5-node cluster startup completed!"
    echo
    
    # Test cluster
    test_cluster_status
    show_cluster_nodes
    test_leader_election
    test_cluster_operations
    echo
    
    echo "🎉 5-node cluster testing completed!"
    echo
    echo "💡 Manual testing commands:"
    echo "  ./bin/gomsg-cli --server=localhost:9000 cluster status"
    echo "  ./bin/gomsg-cli --server=localhost:9001 cluster nodes"
    echo "  ./bin/gomsg-cli --server=localhost:9002 kv set key value"
    echo "  ./bin/gomsg-cli --server=localhost:9003 queue push myqueue message"
    echo "  ./bin/gomsg-cli --server=localhost:9004 stream publish mytopic event"
    echo
    echo "📋 Log files available in logs/ directory"
    echo "🛑 Press Ctrl+C to stop all nodes"
    
    # Keep running until interrupted
    while true; do
        sleep 30
        echo "  5-node cluster running... ($(date))"
        
        # Show brief status
        local healthy_nodes=0
        for port in 9000 9001 9002 9003 9004; do
            if ./bin/gomsg-cli --server=localhost:$port cluster status >/dev/null 2>&1; then
                healthy_nodes=$((healthy_nodes + 1))
            fi
        done
        echo "  Healthy nodes: $healthy_nodes/5"
    done
}

# Run main function
main "$@"
