# gomsg Go Client SDK

Typed Go client for gomsg's gRPC services: KV, Queue, Cluster.

## Install

In your Go module:

```bash
go get gomsg/clients/go
```

Ensure your project has access to gomsg's generated protobuf Go packages under `gomsg/api/generated/*`.

## Usage

```go
package main

import (
  "context"
  "fmt"
  "time"
  gc "gomsg/clients/go"
)

func main() {
  ctx := context.Background()
  cli, err := gc.New(ctx, "localhost:9000", nil)
  if err != nil { panic(err) }
  defer cli.Close()

  // KV
  _, _ = cli.KV.Set(ctx, &kv.SetRequest{Key: "user:1:name", Value: []byte("alice"), Ttl: 0})
  get, _ := cli.KV.Get(ctx, &kv.GetRequest{Key: "user:1:name"})
  fmt.Println("name=", string(get.Value))

  // Queue
  push, _ := cli.Queue.Push(ctx, &queue.PushRequest{Queue: "emails", Data: []byte("hello")})
  fmt.Println("msgID=", push.MessageId)

  // Cluster
  leader, _ := cli.Cluster.GetLeader(ctx, &cluster.GetLeaderRequest{})
  fmt.Println("leader=", leader.Leader.GetId(), leader.Leader.GetAddress())

  _ = time.Second // avoid unused import if you tweak
}
```

## Notes
- By default, the server may run without the Stream service unless stubs are generated and enabled. The client currently targets KV, Queue, and Cluster.
- For TLS/mTLS, replace `insecure.NewCredentials()` in `client.New()` with your own `TransportCredentials`.
