package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	queuepb "gomsg/api/generated/queue"
)

func queueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Queue operations",
		Long:  "Perform queue operations like RabbitMQ",
	}

	cmd.AddCommand(queuePushCmd())
	cmd.AddCommand(queuePopCmd())
	cmd.AddCommand(queuePeekCmd())
	cmd.AddCommand(queueStatsCmd())
	cmd.AddCommand(queueListCmd())
	cmd.AddCommand(queuePurgeCmd())

	return cmd
}

func queuePushCmd() *cobra.Command {
	var delay int64
	
	cmd := &cobra.Command{
		Use:   "push <queue> <message>",
		Short: "Push a message to a queue",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.PushRequest{
				Queue: args[0],
				Data:  []byte(args[1]),
				Delay: delay,
			}

			resp, err := client.Push(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("Message ID: %s\n", resp.MessageId)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int64Var(&delay, "delay", 0, "Delay in seconds before message becomes available")

	return cmd
}

func queuePopCmd() *cobra.Command {
	var popTimeout int32
	
	cmd := &cobra.Command{
		Use:   "pop <queue>",
		Short: "Pop a message from a queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.PopRequest{
				Queue:   args[0],
				Timeout: popTimeout,
			}

			resp, err := client.Pop(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				if resp.Message != nil {
					fmt.Printf("ID: %s\n", resp.Message.Id)
					fmt.Printf("Data: %s\n", string(resp.Message.Data))
					fmt.Printf("Created: %s\n", time.Unix(resp.Message.CreatedAt, 0).Format(time.RFC3339))
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int32Var(&popTimeout, "timeout", 30, "Pop timeout in seconds")

	return cmd
}

func queuePeekCmd() *cobra.Command {
	var limit int32
	
	cmd := &cobra.Command{
		Use:   "peek <queue>",
		Short: "Peek at messages in a queue without removing them",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.PeekRequest{
				Queue: args[0],
				Limit: limit,
			}

			resp, err := client.Peek(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, msg := range resp.Messages {
					fmt.Printf("%d) ID: %s, Data: %s\n", i+1, msg.Id, string(msg.Data))
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int32Var(&limit, "limit", 10, "Maximum number of messages to peek")

	return cmd
}

func queueStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats <queue>",
		Short: "Get queue statistics",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.StatsRequest{Queue: args[0]}
			resp, err := client.Stats(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success && resp.Stats != nil {
				fmt.Printf("Queue: %s\n", resp.Stats.Name)
				fmt.Printf("Size: %d\n", resp.Stats.Size)
				fmt.Printf("Consumers: %d\n", resp.Stats.Consumers)
				fmt.Printf("Pending: %d\n", resp.Stats.Pending)
				fmt.Printf("Processed: %d\n", resp.Stats.Processed)
				fmt.Printf("Failed: %d\n", resp.Stats.Failed)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func queueListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.ListRequest{}
			resp, err := client.List(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, queue := range resp.Queues {
					fmt.Printf("%d) %s\n", i+1, queue)
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func queuePurgeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "purge <queue>",
		Short: "Remove all messages from a queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := queuepb.NewQueueServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &queuepb.PurgeRequest{Queue: args[0]}
			resp, err := client.Purge(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("Purged %d messages\n", resp.PurgedCount)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}