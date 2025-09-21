package cluster

import (
	"hash/fnv"
	"sort"
	"sync"
)

// PartitionMap manages partition assignments and replication
type PartitionMap struct {
	mu sync.RWMutex
	
	partitions        int32
	replicationFactor int32
	
	// partition -> []nodeID (first is primary, rest are replicas)
	assignments map[int32][]string
}

// NewPartitionMap creates a new partition map
func NewPartitionMap(partitions, replicationFactor int32) *PartitionMap {
	return &PartitionMap{
		partitions:        partitions,
		replicationFactor: replicationFactor,
		assignments:       make(map[int32][]string),
	}
}

// Bootstrap assigns all partitions to a single node (for cluster startup)
func (pm *PartitionMap) Bootstrap(nodeID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	for i := int32(0); i < pm.partitions; i++ {
		pm.assignments[i] = []string{nodeID}
	}
}

// Rebalance redistributes partitions across nodes with replication
func (pm *PartitionMap) Rebalance(nodes []string, replicationFactor int32) {
	if len(nodes) == 0 {
		return
	}
	
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	// Sort nodes for deterministic assignment
	sortedNodes := make([]string, len(nodes))
	copy(sortedNodes, nodes)
	sort.Strings(sortedNodes)
	
	// Calculate replicas needed (min of replicationFactor and available nodes)
	replicas := int(replicationFactor)
	if replicas > len(sortedNodes) {
		replicas = len(sortedNodes)
	}
	
	// Assign each partition to nodes
	for partition := int32(0); partition < pm.partitions; partition++ {
		owners := make([]string, 0, replicas)
		
		// Use consistent hashing to determine primary and replicas
		for i := 0; i < replicas; i++ {
			nodeIndex := (int(partition) + i) % len(sortedNodes)
			owners = append(owners, sortedNodes[nodeIndex])
		}
		
		pm.assignments[partition] = owners
	}
}

// GetOwners returns the nodes that own a partition (primary + replicas)
func (pm *PartitionMap) GetOwners(partition int32) []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	owners, exists := pm.assignments[partition]
	if !exists {
		return nil
	}
	
	// Return a copy to prevent external modification
	result := make([]string, len(owners))
	copy(result, owners)
	return result
}

// GetPrimary returns the primary owner of a partition
func (pm *PartitionMap) GetPrimary(partition int32) string {
	owners := pm.GetOwners(partition)
	if len(owners) > 0 {
		return owners[0]
	}
	return ""
}

// GetPartition returns the partition for a given key using consistent hashing
func (pm *PartitionMap) GetPartition(key string) int32 {
	if pm.partitions == 1 {
		return 0
	}
	
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()
	
	return int32(hash % uint32(pm.partitions))
}

// GetPartitionsForNode returns all partitions owned by a node
func (pm *PartitionMap) GetPartitionsForNode(nodeID string) []int32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	partitions := make([]int32, 0)
	for partition, owners := range pm.assignments {
		for _, owner := range owners {
			if owner == nodeID {
				partitions = append(partitions, partition)
				break
			}
		}
	}
	
	return partitions
}

// GetPrimaryPartitionsForNode returns partitions where the node is primary
func (pm *PartitionMap) GetPrimaryPartitionsForNode(nodeID string) []int32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	partitions := make([]int32, 0)
	for partition, owners := range pm.assignments {
		if len(owners) > 0 && owners[0] == nodeID {
			partitions = append(partitions, partition)
		}
	}
	
	return partitions
}

// AssignPartition manually assigns a partition to specific nodes
func (pm *PartitionMap) AssignPartition(partition int32, owners []string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	pm.assignments[partition] = make([]string, len(owners))
	copy(pm.assignments[partition], owners)
}

// RemoveNode removes a node from all partition assignments
func (pm *PartitionMap) RemoveNode(nodeID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	for partition, owners := range pm.assignments {
		newOwners := make([]string, 0, len(owners))
		for _, owner := range owners {
			if owner != nodeID {
				newOwners = append(newOwners, owner)
			}
		}
		pm.assignments[partition] = newOwners
	}
}

// GetAssignments returns all partition assignments
func (pm *PartitionMap) GetAssignments() map[int32][]string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	result := make(map[int32][]string)
	for partition, owners := range pm.assignments {
		result[partition] = make([]string, len(owners))
		copy(result[partition], owners)
	}
	
	return result
}

// GetStats returns partition distribution statistics
func (pm *PartitionMap) GetStats() *PartitionStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	stats := &PartitionStats{
		TotalPartitions: pm.partitions,
		NodeStats:       make(map[string]*NodePartitionStats),
	}
	
	// Count partitions per node
	for _, owners := range pm.assignments {
		for i, owner := range owners {
			if stats.NodeStats[owner] == nil {
				stats.NodeStats[owner] = &NodePartitionStats{}
			}
			
			stats.NodeStats[owner].TotalPartitions++
			if i == 0 { // Primary
				stats.NodeStats[owner].PrimaryPartitions++
			} else { // Replica
				stats.NodeStats[owner].ReplicaPartitions++
			}
		}
		
		if len(owners) == 0 {
			stats.UnassignedPartitions++
		}
	}
	
	return stats
}
