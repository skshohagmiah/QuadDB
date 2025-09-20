package cluster

import (
	"sort"
)

// Partitioner provides consistent hashing of keys to partitions with virtual nodes.
type Partitioner struct {
	partitions int32
	vnodes    int
}

// NewPartitioner creates a partitioner for N partitions with V virtual nodes (>=1).
func NewPartitioner(partitions int32, vnodes int) *Partitioner {
	if partitions <= 0 { partitions = 1 }
	if vnodes <= 0 { vnodes = 16 }
	return &Partitioner{partitions: partitions, vnodes: vnodes}
}

// Partition returns a partition ID for the given key.
func (p *Partitioner) Partition(key string) int32 {
	if p.partitions == 1 { return 0 }
	// Simple hash then modulo; for virtual nodes we can mix hash but keep it simple here
	h := HashKey(key)
	return int32(h % uint32(p.partitions))
}

// Assignments evenly assign partitions across sorted members (used by consumers).
func Assignments(partitions int32, members []string, self string) []int32 {
	if partitions <= 0 || len(members) == 0 { return nil }
	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	res := make([]int32, 0)
	for p := int32(0); p < partitions; p++ {
		owner := sorted[int(p)%len(sorted)]
		if owner == self { res = append(res, p) }
	}
	return res
}
