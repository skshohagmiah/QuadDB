package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	documentpb "github.com/skshohagmiah/gomsg/api/generated/document"
)

func dbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Document database operations",
		Long:  "Perform document database operations like MongoDB",
	}

	cmd.AddCommand(dbCreateCollectionCmd())
	cmd.AddCommand(dbInsertOneCmd())
	cmd.AddCommand(dbFindOneCmd())
	cmd.AddCommand(dbFindManyCmd())
	cmd.AddCommand(dbUpdateOneCmd())
	cmd.AddCommand(dbDeleteOneCmd())
	cmd.AddCommand(dbListCollectionsCmd())

	return cmd
}

func dbCreateCollectionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create-collection <collection>",
		Short: "Create a new collection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &documentpb.CreateCollectionRequest{
				Collection: args[0],
			}

			resp, err := client.CreateCollection(ctx, req)
			if err != nil {
				return err
			}

			if resp.Success {
				fmt.Printf("Collection '%s' created successfully\n", args[0])
			} else {
				fmt.Printf("Error: %s\n", resp.Message)
			}

			return nil
		},
	}
}

func dbInsertOneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "insert-one <collection> <json-document>",
		Short: "Insert a document into a collection",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			// Parse JSON document
			var docData map[string]interface{}
			if err := json.Unmarshal([]byte(args[1]), &docData); err != nil {
				return fmt.Errorf("invalid JSON: %v", err)
			}

			docStruct, err := structpb.NewStruct(docData)
			if err != nil {
				return fmt.Errorf("failed to convert document: %v", err)
			}

			req := &documentpb.InsertOneRequest{
				Collection: args[0],
				Document:   docStruct,
			}

			resp, err := client.InsertOne(ctx, req)
			if err != nil {
				return err
			}

			fmt.Printf("Document inserted with ID: %s\n", resp.InsertedId)
			return nil
		},
	}
}

func dbFindOneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "find-one <collection> [filter-json]",
		Short: "Find one document in a collection",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &documentpb.FindOneRequest{
				Collection: args[0],
				Query:      &documentpb.Query{},
			}

			// Parse filter if provided
			if len(args) > 1 {
				var filterData map[string]interface{}
				if err := json.Unmarshal([]byte(args[1]), &filterData); err != nil {
					return fmt.Errorf("invalid filter JSON: %v", err)
				}

				filterStruct, err := structpb.NewStruct(filterData)
				if err != nil {
					return fmt.Errorf("failed to convert filter: %v", err)
				}

				req.Query.Filter = filterStruct
			}

			resp, err := client.FindOne(ctx, req)
			if err != nil {
				return err
			}

			if resp.Found {
				docJSON, _ := json.MarshalIndent(resp.Document.Data.AsMap(), "", "  ")
				fmt.Printf("Document ID: %s\n", resp.Document.Id)
				fmt.Printf("Document: %s\n", string(docJSON))
				fmt.Printf("Created: %s\n", resp.Document.CreatedAt.AsTime().Format(time.RFC3339))
				fmt.Printf("Updated: %s\n", resp.Document.UpdatedAt.AsTime().Format(time.RFC3339))
			} else {
				fmt.Println("No document found")
			}

			return nil
		},
	}
}

func dbFindManyCmd() *cobra.Command {
	var limit int32

	cmd := &cobra.Command{
		Use:   "find-many <collection> [filter-json]",
		Short: "Find multiple documents in a collection",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &documentpb.FindManyRequest{
				Collection: args[0],
				Query: &documentpb.Query{
					Limit: limit,
				},
			}

			// Parse filter if provided
			if len(args) > 1 {
				var filterData map[string]interface{}
				if err := json.Unmarshal([]byte(args[1]), &filterData); err != nil {
					return fmt.Errorf("invalid filter JSON: %v", err)
				}

				filterStruct, err := structpb.NewStruct(filterData)
				if err != nil {
					return fmt.Errorf("failed to convert filter: %v", err)
				}

				req.Query.Filter = filterStruct
			}

			resp, err := client.FindMany(ctx, req)
			if err != nil {
				return err
			}

			fmt.Printf("Found %d documents (total: %d)\n", len(resp.Documents), resp.TotalCount)
			for i, doc := range resp.Documents {
				docJSON, _ := json.MarshalIndent(doc.Data.AsMap(), "", "  ")
				fmt.Printf("\nDocument %d:\n", i+1)
				fmt.Printf("ID: %s\n", doc.Id)
				fmt.Printf("Data: %s\n", string(docJSON))
			}

			return nil
		},
	}

	cmd.Flags().Int32Var(&limit, "limit", 10, "Maximum number of documents to return")
	return cmd
}

func dbUpdateOneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "update-one <collection> <filter-json> <update-json>",
		Short: "Update one document in a collection",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			// Parse filter
			var filterData map[string]interface{}
			if err := json.Unmarshal([]byte(args[1]), &filterData); err != nil {
				return fmt.Errorf("invalid filter JSON: %v", err)
			}

			filterStruct, err := structpb.NewStruct(filterData)
			if err != nil {
				return fmt.Errorf("failed to convert filter: %v", err)
			}

			// Parse update
			var updateData map[string]interface{}
			if err := json.Unmarshal([]byte(args[2]), &updateData); err != nil {
				return fmt.Errorf("invalid update JSON: %v", err)
			}

			update := &documentpb.Update{}
			if setData, ok := updateData["$set"]; ok {
				if setMap, ok := setData.(map[string]interface{}); ok {
					setStruct, err := structpb.NewStruct(setMap)
					if err != nil {
						return fmt.Errorf("failed to convert $set: %v", err)
					}
					update.Set = setStruct
				}
			}

			req := &documentpb.UpdateOneRequest{
				Collection: args[0],
				Filter:     filterStruct,
				Update:     update,
			}

			resp, err := client.UpdateOne(ctx, req)
			if err != nil {
				return err
			}

			fmt.Printf("Matched: %d, Modified: %d\n", resp.Result.MatchedCount, resp.Result.ModifiedCount)
			return nil
		},
	}
}

func dbDeleteOneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete-one <collection> <filter-json>",
		Short: "Delete one document from a collection",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			// Parse filter
			var filterData map[string]interface{}
			if err := json.Unmarshal([]byte(args[1]), &filterData); err != nil {
				return fmt.Errorf("invalid filter JSON: %v", err)
			}

			filterStruct, err := structpb.NewStruct(filterData)
			if err != nil {
				return fmt.Errorf("failed to convert filter: %v", err)
			}

			req := &documentpb.DeleteOneRequest{
				Collection: args[0],
				Filter:     filterStruct,
			}

			resp, err := client.DeleteOne(ctx, req)
			if err != nil {
				return err
			}

			fmt.Printf("Deleted: %d documents\n", resp.Result.DeletedCount)
			return nil
		},
	}
}

func dbListCollectionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-collections",
		Short: "List all collections",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := documentpb.NewDocumentServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			req := &documentpb.ListCollectionsRequest{}

			resp, err := client.ListCollections(ctx, req)
			if err != nil {
				return err
			}

			if len(resp.Collections) == 0 {
				fmt.Println("No collections found")
			} else {
				fmt.Printf("Collections (%d):\n", len(resp.Collections))
				for i, collection := range resp.Collections {
					fmt.Printf("%d) %s\n", i+1, collection)
				}
			}

			return nil
		},
	}
}
