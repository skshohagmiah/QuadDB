package cluster

import (
	"encoding/json"
	"hash/fnv"
)

// CommandType describes the replicated operation type.
type CommandType string

const (
	CmdKVSet        CommandType = "KV_SET"
	CmdKVDelete     CommandType = "KV_DELETE"
	CmdStreamCreate CommandType = "STREAM_CREATE_TOPIC"
	CmdStreamDelete CommandType = "STREAM_DELETE_TOPIC"
)

// Command is the envelope replicated via Raft.
type Command struct {
	Version int             `json:"v"`
	Type    CommandType     `json:"t"`
	Payload json.RawMessage `json:"p"`
}

// Marshal encodes the command to bytes.
func (c Command) Marshal() ([]byte, error) { return json.Marshal(c) }

// HashKey is a helper to hash a string consistently (used for partitioning helpers).
func HashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
