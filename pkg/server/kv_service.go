package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"

	commonpb "gomsg/api/generated/common"
	kvpb "gomsg/api/generated/kv"
	"gomsg/pkg/cluster"
	raftex "gomsg/pkg/cluster/raft"
	"gomsg/storage"
)

// KVService implements the KV gRPC service
type KVService struct {
	kvpb.UnimplementedKVServiceServer
	storage   storage.Storage
	submitter *raftex.Submitter
	cluster   *cluster.Manager
}

// NewKVService creates a new KV service
func NewKVService(store storage.Storage, submitter *raftex.Submitter, mgr *cluster.Manager) *KVService {
	return &KVService{storage: store, submitter: submitter, cluster: mgr}
}

func (s *KVService) leaderRedirectStatus() *commonpb.Status {
	leader := ""
	addr := ""
	if s.cluster != nil {
		if ln, ok := s.cluster.GetLeader(); ok {
			leader = ln.ID
			addr = ln.Address
		}
	}
	msg := "not leader"
	if leader != "" {
		msg = fmt.Sprintf("not leader; leader=%s@%s", leader, addr)
	}
	return &commonpb.Status{Success: false, Message: msg, Code: int32(codes.FailedPrecondition)}
}

// Set stores a key-value pair
func (s *KVService) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if req.Key == "" {
		return &kvpb.SetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	var ttl time.Duration
	if req.Ttl > 0 {
		ttl = time.Duration(req.Ttl) * time.Second
	}

	// If raft is configured, only leader handles writes via Raft
	if s.submitter != nil {
		if !s.submitter.IsLeader() {
			return &kvpb.SetResponse{Status: s.leaderRedirectStatus()}, nil
		}
		payload, _ := json.Marshal(struct {
			Key string `json:"k"`
			Value []byte `json:"v"`
			TTLSeconds int64 `json:"ttl"`
		}{Key: req.Key, Value: req.Value, TTLSeconds: int64(req.Ttl)})
		cmd := raftex.Command{Version: 1, Type: raftex.CmdKVSet, Payload: payload}
		if err := s.submitter.Submit(ctx, cmd, 0); err != nil {
			return &kvpb.SetResponse{Status: &commonpb.Status{Success: false, Message: err.Error(), Code: int32(codes.Internal)}}, nil
		}
		return &kvpb.SetResponse{Status: &commonpb.Status{Success: true, Message: "OK", Code: int32(codes.OK)}}, nil
	}

	err := s.storage.Set(ctx, req.Key, req.Value, ttl)
	if err != nil {
		return &kvpb.SetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.SetResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Get retrieves a value by key
func (s *KVService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if req.Key == "" {
		return &kvpb.GetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	value, found, err := s.storage.Get(ctx, req.Key)
	if err != nil {
		return &kvpb.GetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.GetResponse{
		Value: value,
		Found: found,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Del deletes one or more keys
func (s *KVService) Del(ctx context.Context, req *kvpb.DelRequest) (*kvpb.DelResponse, error) {
	if len(req.Keys) == 0 {
		return &kvpb.DelResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "at least one key required",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	// If raft is configured, only leader handles writes via Raft
	if s.submitter != nil {
		if !s.submitter.IsLeader() {
			return &kvpb.DelResponse{Status: s.leaderRedirectStatus()}, nil
		}
		payload, _ := json.Marshal(struct{ Keys []string `json:"ks"` }{Keys: req.Keys})
		cmd := raftex.Command{Version: 1, Type: raftex.CmdKVDelete, Payload: payload}
		if err := s.submitter.Submit(ctx, cmd, 0); err != nil {
			return &kvpb.DelResponse{Status: &commonpb.Status{Success: false, Message: err.Error(), Code: int32(codes.Internal)}}, nil
		}
		return &kvpb.DelResponse{DeletedCount: int32(len(req.Keys)), Status: &commonpb.Status{Success: true, Message: "OK", Code: int32(codes.OK)}}, nil
	}

	deleted, err := s.storage.Delete(ctx, req.Keys...)
	if err != nil {
		return &kvpb.DelResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.DelResponse{
		DeletedCount: int32(deleted),
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Exists checks if a key exists
func (s *KVService) Exists(ctx context.Context, req *kvpb.ExistsRequest) (*kvpb.ExistsResponse, error) {
	if req.Key == "" {
		return &kvpb.ExistsResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	exists, err := s.storage.Exists(ctx, req.Key)
	if err != nil {
		return &kvpb.ExistsResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.ExistsResponse{
		Exists: exists,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Incr atomically increments a counter
func (s *KVService) Incr(ctx context.Context, req *kvpb.IncrRequest) (*kvpb.IncrResponse, error) {
	if req.Key == "" {
		return &kvpb.IncrResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	delta := req.By
	if delta == 0 {
		delta = 1
	}

	value, err := s.storage.Increment(ctx, req.Key, delta)
	if err != nil {
		return &kvpb.IncrResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.IncrResponse{
		Value: value,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Decr atomically decrements a counter
func (s *KVService) Decr(ctx context.Context, req *kvpb.DecrRequest) (*kvpb.DecrResponse, error) {
	if req.Key == "" {
		return &kvpb.DecrResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	delta := req.By
	if delta == 0 {
		delta = 1
	}

	value, err := s.storage.Decrement(ctx, req.Key, delta)
	if err != nil {
		return &kvpb.DecrResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.DecrResponse{
		Value: value,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// MGet gets multiple values by keys
func (s *KVService) MGet(ctx context.Context, req *kvpb.MGetRequest) (*kvpb.MGetResponse, error) {
	if len(req.Keys) == 0 {
		return &kvpb.MGetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "at least one key required",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	values, err := s.storage.MGet(ctx, req.Keys)
	if err != nil {
		return &kvpb.MGetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	// Build response in the same order as the request keys
	var kvPairs []*commonpb.KeyValue
	for _, key := range req.Keys {
		if val, ok := values[key]; ok {
			kvPairs = append(kvPairs, &commonpb.KeyValue{Key: key, Value: val})
		}
	}

	return &kvpb.MGetResponse{
		Values: kvPairs,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// MSet sets multiple key-value pairs
func (s *KVService) MSet(ctx context.Context, req *kvpb.MSetRequest) (*kvpb.MSetResponse, error) {
	if len(req.Values) == 0 {
		return &kvpb.MSetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "at least one key-value pair required",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	pairs := make(map[string][]byte)
	for _, kv := range req.Values {
		if kv.Key == "" {
			return &kvpb.MSetResponse{
				Status: &commonpb.Status{
					Success: false,
					Message: "key cannot be empty",
					Code:    int32(codes.InvalidArgument),
				},
			}, nil
		}
		pairs[kv.Key] = kv.Value
	}

	err := s.storage.MSet(ctx, pairs)
	if err != nil {
		return &kvpb.MSetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.MSetResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Keys returns keys matching a pattern
func (s *KVService) Keys(ctx context.Context, req *kvpb.KeysRequest) (*kvpb.KeysResponse, error) {
	pattern := req.Pattern
	if pattern == "" {
		pattern = "*"
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000
	}

	keys, err := s.storage.Keys(ctx, pattern, limit)
	if err != nil {
		return &kvpb.KeysResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.KeysResponse{
		Keys: keys,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Expire sets TTL for a key
func (s *KVService) Expire(ctx context.Context, req *kvpb.ExpireRequest) (*kvpb.ExpireResponse, error) {
	if req.Key == "" {
		return &kvpb.ExpireResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	if req.Ttl <= 0 {
		return &kvpb.ExpireResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "TTL must be positive",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	ttl := time.Duration(req.Ttl) * time.Second
	err := s.storage.Expire(ctx, req.Key, ttl)
	if err != nil {
		return &kvpb.ExpireResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.ExpireResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// TTL returns remaining TTL for a key
func (s *KVService) TTL(ctx context.Context, req *kvpb.TTLRequest) (*kvpb.TTLResponse, error) {
	if req.Key == "" {
		return &kvpb.TTLResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "key cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	ttl, err := s.storage.TTL(ctx, req.Key)
	if err != nil {
		return &kvpb.TTLResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &kvpb.TTLResponse{
		Ttl: int64(ttl.Seconds()),
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}