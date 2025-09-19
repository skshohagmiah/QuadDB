package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streampb "gomsg/api/generated/stream"
)

func streamCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream operations",
		Long:  "Perform stream operations like Kafka",
	}

	cmd.AddCommand(streamPublishCmd())
	cmd.AddCommand(streamReadCmd())
	cmd.AddCommand(streamCreateCmd())
	cmd.AddCommand(streamListCmd())
	cmd.AddCommand(streamInfoCmd())

	return cmd
}

func streamPublishCmd() *cobra.Command {
	var partitionKey string
	
	cmd := &cobra.Command{
		Use:   "publish <topic> <message>",
		Short: "Publish a message to a stream",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := streampb.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &streampb.PublishRequest{
				Topic:        args[0],
				Data:         []byte(args[1]),
				PartitionKey: partitionKey,
			}

			resp, err := client.Publish(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("Message ID: %s\n", resp.MessageId)
				fmt.Printf("Offset: %d\n", resp.Offset)
				fmt.Printf("Partition: %d\n", resp.Partition)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&partitionKey, "key", "", "Partition key for message routing")

	return cmd
}

func streamReadCmd() *cobra.Command {
	var offset int64
	var limit int32
	var partition int32
	
	cmd := &cobra.Command{
		Use:   "read <topic>",
		Short: "Read messages from a stream",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := streampb.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &streampb.ReadRequest{
				Topic:      args[0],
				FromOffset: offset,
				Limit:      limit,
				Partition:  partition,
			}

			resp, err := client.Read(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, msg := range resp.Messages {
					fmt.Printf("%d) ID: %s, Offset: %d, Data: %s\n", 
						i+1, msg.Id, msg.Offset, string(msg.Data))
				}
				fmt.Printf("Next offset: %d\n", resp.NextOffset)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int64Var(&offset, "offset", 0, "Starting offset")
	cmd.Flags().Int32Var(&limit, "limit", 10, "Maximum number of messages to read")
	cmd.Flags().Int32Var(&partition, "partition", 0, "Partition to read from")

	return cmd
}

func streamCreateCmd() *cobra.Command {
	var partitions int32
	
	cmd := &cobra.Command{
		Use:   "create <topic>",
		Short: "Create a new topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := streampb.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &streampb.CreateTopicRequest{
				Name:       args[0],
				Partitions: partitions,
			}

			resp, err := client.CreateTopic(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				fmt.Printf("Topic '%s' created successfully\n", args[0])
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}

	cmd.Flags().Int32Var(&partitions, "partitions", 1, "Number of partitions")

	return cmd
}

func streamListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all topics",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := streampb.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &streampb.ListTopicsRequest{}
			resp, err := client.ListTopics(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, topic := range resp.Topics {
					fmt.Printf("%d) %s\n", i+1, topic)
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func streamInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <topic>",
		Short: "Get information about a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := streampb.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &streampb.GetTopicInfoRequest{Topic: args[0]}
			resp, err := client.GetTopicInfo(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success && resp.Info != nil {
				fmt.Printf("Topic: %s\n", resp.Info.Name)
				fmt.Printf("Partitions: %d\n", resp.Info.Partitions)
				fmt.Printf("Total Messages: %d\n", resp.Info.TotalMessages)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}