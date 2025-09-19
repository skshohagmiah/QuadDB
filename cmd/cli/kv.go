package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "gomsg/api/generated/kv"
)

func kvCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kv",
		Short: "Key-Value operations",
		Long:  "Perform key-value operations like Redis",
	}

	cmd.AddCommand(kvSetCmd())
	cmd.AddCommand(kvGetCmd())
	cmd.AddCommand(kvDelCmd())
	cmd.AddCommand(kvExistsCmd())
	cmd.AddCommand(kvKeysCmd())
	cmd.AddCommand(kvIncrCmd())
	cmd.AddCommand(kvDecrCmd())
	cmd.AddCommand(kvTTLCmd())

	return cmd
}

func kvSetCmd() *cobra.Command {
	var ttl int64
	
	cmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.SetRequest{
				Key:   args[0],
				Value: []byte(args[1]),
				Ttl:   ttl,
			}

			resp, err := client.Set(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Println("OK")
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int64Var(&ttl, "ttl", 0, "Time to live in seconds")

	return cmd
}

func kvGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Get a value by key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.GetRequest{Key: args[0]}
			resp, err := client.Get(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				if resp.Found {
					fmt.Printf("%s\n", string(resp.Value))
				} else {
					fmt.Println("(nil)")
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func kvDelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "del <key> [key...]",
		Short: "Delete one or more keys",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.DelRequest{Keys: args}
			resp, err := client.Del(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("(integer) %d\n", resp.DeletedCount)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func kvExistsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "exists <key>",
		Short: "Check if a key exists",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.ExistsRequest{Key: args[0]}
			resp, err := client.Exists(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				if resp.Exists {
					fmt.Println("(integer) 1")
				} else {
					fmt.Println("(integer) 0")
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func kvKeysCmd() *cobra.Command {
	var limit int32
	
	cmd := &cobra.Command{
		Use:   "keys <pattern>",
		Short: "Find keys matching a pattern",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.KeysRequest{
				Pattern: args[0],
				Limit:   limit,
			}

			resp, err := client.Keys(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, key := range resp.Keys {
					fmt.Printf("%d) %s\n", i+1, key)
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int32Var(&limit, "limit", 100, "Maximum number of keys to return")

	return cmd
}

func kvIncrCmd() *cobra.Command {
	var by int64
	
	cmd := &cobra.Command{
		Use:   "incr <key>",
		Short: "Increment a counter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.IncrRequest{
				Key: args[0],
				By:  by,
			}

			resp, err := client.Incr(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("(integer) %d\n", resp.Value)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int64Var(&by, "by", 1, "Increment by this amount")

	return cmd
}

func kvDecrCmd() *cobra.Command {
	var by int64
	
	cmd := &cobra.Command{
		Use:   "decr <key>",
		Short: "Decrement a counter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.DecrRequest{
				Key: args[0],
				By:  by,
			}

			resp, err := client.Decr(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("(integer) %d\n", resp.Value)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int64Var(&by, "by", 1, "Decrement by this amount")

	return cmd
}

func kvTTLCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ttl <key>",
		Short: "Get remaining TTL for a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := kvpb.NewKVServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &kvpb.TTLRequest{Key: args[0]}
			resp, err := client.TTL(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("(integer) %d\n", resp.Ttl)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}