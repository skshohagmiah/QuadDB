//go:build !raft_enabled

package raft

// This file exists so that the raft package builds when the 'raft_enabled' build
// tag is not set. All real implementations are behind the raft_enabled tag.
