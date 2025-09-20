package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterpb "gomsg/api/generated/cluster"
)

func clusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster operations",
		Long:  "Perform cluster management operations",
	}

	cmd.AddCommand(clusterNodesCmd())
	cmd.AddCommand(clusterStatusCmd())
	cmd.AddCommand(clusterStatsCmd())
	cmd.AddCommand(clusterLeaderCmd())
	cmd.AddCommand(clusterJoinCmd())
	cmd.AddCommand(clusterLeaveCmd())

	return cmd
}

func clusterNodesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List all cluster nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &clusterpb.GetNodesRequest{}
			resp, err := client.GetNodes(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success {
				for i, node := range resp.Nodes {
					leader := ""
					if node.IsLeader {
						leader = " (Leader)"
					}
					fmt.Printf("%d) %s - %s - %s%s\n", 
						i+1, node.Id, node.Address, node.State, leader)
				}
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func clusterStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Get cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &clusterpb.GetStatusRequest{}
			resp, err := client.GetStatus(ctx, req)
			if err != nil {
				return err
			}

			if resp.ResponseStatus.Success && resp.Status != nil {
				fmt.Printf("Healthy: %t\n", resp.Status.Healthy)
				fmt.Printf("Leader: %s\n", resp.Status.LeaderId)
				fmt.Printf("Total Nodes: %d\n", resp.Status.TotalNodes)
				fmt.Printf("Active Nodes: %d\n", resp.Status.ActiveNodes)
				fmt.Printf("Raft State: %s\n", resp.Status.RaftState)
			} else {
				fmt.Printf("Error: %s\n", resp.ResponseStatus.Message)
			}

			return nil
		},
	}
}

func clusterStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Get cluster statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &clusterpb.GetStatsRequest{}
			resp, err := client.GetStats(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success && resp.Stats != nil {
				fmt.Printf("Total Operations: %d\n", resp.Stats.TotalOperations)
				fmt.Printf("Keys Count: %d\n", resp.Stats.KeysCount)
				fmt.Printf("Queues Count: %d\n", resp.Stats.QueuesCount)
				fmt.Printf("Topics Count: %d\n", resp.Stats.TopicsCount)
				fmt.Printf("Memory Usage: %d bytes\n", resp.Stats.MemoryUsage)
				fmt.Printf("Disk Usage: %d bytes\n", resp.Stats.DiskUsage)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func clusterLeaderCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "leader",
		Short: "Get current leader node",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &clusterpb.GetLeaderRequest{}
			resp, err := client.GetLeader(ctx, req)
			if err != nil {
				return err
			}

			if resp.Status.Success && resp.Leader != nil {
				fmt.Printf("Leader: %s (%s)\n", resp.Leader.Id, resp.Leader.Address)
				fmt.Printf("State: %s\n", resp.Leader.State)
			} else {
				fmt.Printf("Error: %s\n", resp.Status.Message)
			}

			return nil
		},
	}
}

func clusterJoinCmd() *cobra.Command {
	var nodeID string
	var address string
	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join a node to the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if nodeID == "" || address == "" {
				return fmt.Errorf("--node-id and --address are required")
			}
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil { return err }
			defer conn.Close()
			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()
			resp, err := client.Join(ctx, &clusterpb.JoinRequest{NodeId: nodeID, Address: address})
			if err != nil { return err }
			if !resp.Status.Success { return fmt.Errorf("join failed: %s", resp.Status.Message) }
			fmt.Println("Node joined")
			return nil
		},
	}
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID to add")
	cmd.Flags().StringVar(&address, "address", "", "Node address (host:port)")
	return cmd
}

func clusterLeaveCmd() *cobra.Command {
	var nodeID string
	cmd := &cobra.Command{
		Use:   "leave",
		Short: "Remove a node from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if nodeID == "" { return fmt.Errorf("--node-id is required") }
			conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil { return err }
			defer conn.Close()
			client := clusterpb.NewClusterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()
			resp, err := client.Leave(ctx, &clusterpb.LeaveRequest{NodeId: nodeID})
			if err != nil { return err }
			if !resp.Status.Success { return fmt.Errorf("leave failed: %s", resp.Status.Message) }
			fmt.Println("Node removed")
			return nil
		},
	}
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID to remove")
	return cmd
}