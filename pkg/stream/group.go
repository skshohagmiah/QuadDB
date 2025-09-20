package stream

import (
	"context"
	"sort"
	"strings"
	"time"
)

const (
	groupMemberKeyPrefix = "stream:group:members:"
	// full key format: stream:group:members:<topic>:<group>:<memberID>
)

func groupMemberKey(topic, group, member string) string {
	return strings.Join([]string{groupMemberKeyPrefix, topic, group, member}, ":")
}

func groupMemberScanPattern(topic, group string) string {
	// pattern to match all members of the group
	return strings.Join([]string{groupMemberKeyPrefix, topic, group, "*"}, ":")
}

// heartbeat registers or refreshes this member's presence with TTL.
func heartbeat(ctx context.Context, st Storage, topic, group, member string, ttl time.Duration) error {
	// Use empty value; presence is enough.
	return st.Set(ctx, groupMemberKey(topic, group, member), []byte("1"), ttl)
}

// listMembers returns the sorted list of active members of a group.
func listMembers(ctx context.Context, st Storage, topic, group string) ([]string, error) {
	keys, err := st.Keys(ctx, groupMemberScanPattern(topic, group), 10000)
	if err != nil {
		return nil, err
	}
	members := make([]string, 0, len(keys))
	prefix := strings.Join([]string{groupMemberKeyPrefix, topic, group, ""}, ":")
	for _, k := range keys {
		// extract member id after prefix
		if strings.HasPrefix(k, prefix) {
			members = append(members, strings.TrimPrefix(k, prefix))
		}
	}
	sort.Strings(members)
	return members, nil
}

// computeAssignments computes which partitions this member should own.
func computeAssignments(partitions int32, members []string, self string) []int32 {
	if partitions <= 0 || len(members) == 0 {
		return nil
	}
	res := make([]int32, 0)
	for p := int32(0); p < partitions; p++ {
		owner := members[int(p)%len(members)]
		if owner == self {
			res = append(res, p)
		}
	}
	return res
}
