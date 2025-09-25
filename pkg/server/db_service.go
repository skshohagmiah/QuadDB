package server

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	documentpb "github.com/skshohagmiah/fluxdl/api/generated/db"
	"github.com/skshohagmiah/fluxdl/storage"
)

// DBService implements the document database gRPC service
type DBService struct {
	documentpb.UnimplementedDocumentServiceServer
	storage storage.Storage
}

// NewDBService creates a new database service
func NewDBService(store storage.Storage) *DBService {
	return &DBService{
		storage: store,
	}
}

// Collection management operations

func (s *DBService) CreateCollection(ctx context.Context, req *documentpb.CreateCollectionRequest) (*documentpb.CreateCollectionResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	options := storage.CollectionOptions{}
	if req.Options != nil {
		options.Capped = req.Options.Capped
		options.MaxSize = req.Options.MaxSize
		options.MaxDocuments = req.Options.MaxDocuments
		if req.Options.Validator != nil {
			options.Validator = req.Options.Validator.AsMap()
		}
	}

	err := s.storage.DocCreateCollection(ctx, req.Collection, options)
	if err != nil {
		log.Printf("Failed to create collection %s: %v", req.Collection, err)
		return &documentpb.CreateCollectionResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &documentpb.CreateCollectionResponse{
		Success: true,
		Message: fmt.Sprintf("Collection %s created successfully", req.Collection),
	}, nil
}

func (s *DBService) DropCollection(ctx context.Context, req *documentpb.DropCollectionRequest) (*documentpb.DropCollectionResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	err := s.storage.DocDropCollection(ctx, req.Collection)
	if err != nil {
		log.Printf("Failed to drop collection %s: %v", req.Collection, err)
		return &documentpb.DropCollectionResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &documentpb.DropCollectionResponse{
		Success: true,
		Message: fmt.Sprintf("Collection %s dropped successfully", req.Collection),
	}, nil
}

func (s *DBService) ListCollections(ctx context.Context, req *documentpb.ListCollectionsRequest) (*documentpb.ListCollectionsResponse, error) {
	collections, err := s.storage.DocListCollections(ctx)
	if err != nil {
		log.Printf("Failed to list collections: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.ListCollectionsResponse{
		Collections: collections,
	}, nil
}

func (s *DBService) GetCollectionInfo(ctx context.Context, req *documentpb.GetCollectionInfoRequest) (*documentpb.GetCollectionInfoResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	info, err := s.storage.DocGetCollectionInfo(ctx, req.Collection)
	if err != nil {
		log.Printf("Failed to get collection info for %s: %v", req.Collection, err)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Convert storage.CollectionInfo to protobuf
	pbInfo := &documentpb.CollectionInfo{
		Name:          info.Name,
		DocumentCount: info.DocumentCount,
		SizeBytes:     info.SizeBytes,
		CreatedAt:     timestamppb.New(info.CreatedAt),
		Options: &documentpb.CollectionOptions{
			Capped:       info.Options.Capped,
			MaxSize:      info.Options.MaxSize,
			MaxDocuments: info.Options.MaxDocuments,
		},
	}

	// Convert indexes
	for _, idx := range info.Indexes {
		pbIdx := &documentpb.IndexSpec{
			Name:       idx.Name,
			Unique:     idx.Unique,
			Sparse:     idx.Sparse,
			TtlSeconds: idx.TTLSeconds,
		}

		// Convert keys map
		if keysStruct, err := structpb.NewStruct(idx.Keys); err == nil {
			pbIdx.Keys = keysStruct
		}

		// Convert partial filter
		if idx.PartialFilter != nil {
			if filterStruct, err := structpb.NewStruct(idx.PartialFilter); err == nil {
				pbIdx.PartialFilter = filterStruct
			}
		}

		pbInfo.Indexes = append(pbInfo.Indexes, pbIdx)
	}

	// Convert validator
	if info.Options.Validator != nil {
		if validatorStruct, err := structpb.NewStruct(info.Options.Validator); err == nil {
			pbInfo.Options.Validator = validatorStruct
		}
	}

	return &documentpb.GetCollectionInfoResponse{
		Info: pbInfo,
	}, nil
}

// Document operations

func (s *DBService) InsertOne(ctx context.Context, req *documentpb.InsertOneRequest) (*documentpb.InsertOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Document == nil {
		return nil, status.Error(codes.InvalidArgument, "document cannot be empty")
	}

	document := req.Document.AsMap()
	
	insertedID, err := s.storage.DocInsertOne(ctx, req.Collection, document)
	if err != nil {
		log.Printf("Failed to insert document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.InsertOneResponse{
		InsertedId: insertedID,
		Result: &documentpb.WriteResult{
			Acknowledged:  true,
			InsertedCount: 1,
			InsertedIds:   []string{insertedID},
		},
	}, nil
}

func (s *DBService) InsertMany(ctx context.Context, req *documentpb.InsertManyRequest) (*documentpb.InsertManyResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if len(req.Documents) == 0 {
		return nil, status.Error(codes.InvalidArgument, "documents cannot be empty")
	}

	var documents []map[string]interface{}
	for _, doc := range req.Documents {
		documents = append(documents, doc.AsMap())
	}

	insertedIDs, err := s.storage.DocInsertMany(ctx, req.Collection, documents, req.Ordered)
	if err != nil {
		log.Printf("Failed to insert documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.InsertManyResponse{
		InsertedIds: insertedIDs,
		Result: &documentpb.WriteResult{
			Acknowledged:  true,
			InsertedCount: int64(len(insertedIDs)),
			InsertedIds:   insertedIDs,
		},
	}, nil
}

func (s *DBService) FindOne(ctx context.Context, req *documentpb.FindOneRequest) (*documentpb.FindOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	var filter map[string]interface{}
	var projection map[string]interface{}

	if req.Query != nil && req.Query.Filter != nil {
		filter = req.Query.Filter.AsMap()
	}

	if req.Query != nil && req.Query.Projection != nil {
		projection = req.Query.Projection.AsMap()
	}

	result, err := s.storage.DocFindOne(ctx, req.Collection, filter, projection)
	if err != nil {
		log.Printf("Failed to find document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if result.ID == "" {
		return &documentpb.FindOneResponse{
			Found: false,
		}, nil
	}

	// Convert result to protobuf
	dataStruct, err := structpb.NewStruct(result.Data)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to convert document data")
	}

	return &documentpb.FindOneResponse{
		Found: true,
		Document: &documentpb.Document{
			Id:        result.ID,
			Data:      dataStruct,
			CreatedAt: timestamppb.New(result.CreatedAt),
			UpdatedAt: timestamppb.New(result.UpdatedAt),
			Version:   result.Version,
		},
	}, nil
}

func (s *DBService) FindMany(ctx context.Context, req *documentpb.FindManyRequest) (*documentpb.FindManyResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	query := storage.DocumentQuery{}
	
	if req.Query != nil {
		if req.Query.Filter != nil {
			query.Filter = req.Query.Filter.AsMap()
		}
		if req.Query.Sort != nil {
			query.Sort = req.Query.Sort.AsMap()
		}
		if req.Query.Projection != nil {
			query.Projection = req.Query.Projection.AsMap()
		}
		query.Limit = req.Query.Limit
		query.Skip = req.Query.Skip
	}

	results, totalCount, err := s.storage.DocFindMany(ctx, req.Collection, query)
	if err != nil {
		log.Printf("Failed to find documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Convert results to protobuf
	var pbDocuments []*documentpb.Document
	for _, result := range results {
		dataStruct, err := structpb.NewStruct(result.Data)
		if err != nil {
			log.Printf("Failed to convert document data: %v", err)
			continue
		}

		pbDocuments = append(pbDocuments, &documentpb.Document{
			Id:        result.ID,
			Data:      dataStruct,
			CreatedAt: timestamppb.New(result.CreatedAt),
			UpdatedAt: timestamppb.New(result.UpdatedAt),
			Version:   result.Version,
		})
	}

	return &documentpb.FindManyResponse{
		Documents:  pbDocuments,
		TotalCount: totalCount,
	}, nil
}

func (s *DBService) UpdateOne(ctx context.Context, req *documentpb.UpdateOneRequest) (*documentpb.UpdateOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	if req.Update == nil {
		return nil, status.Error(codes.InvalidArgument, "update cannot be empty")
	}

	filter := req.Filter.AsMap()
	update := s.convertUpdateFromProto(req.Update)

	result, err := s.storage.DocUpdateOne(ctx, req.Collection, filter, update, req.Upsert)
	if err != nil {
		log.Printf("Failed to update document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.UpdateOneResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

func (s *DBService) UpdateMany(ctx context.Context, req *documentpb.UpdateManyRequest) (*documentpb.UpdateManyResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	if req.Update == nil {
		return nil, status.Error(codes.InvalidArgument, "update cannot be empty")
	}

	filter := req.Filter.AsMap()
	update := s.convertUpdateFromProto(req.Update)

	result, err := s.storage.DocUpdateMany(ctx, req.Collection, filter, update)
	if err != nil {
		log.Printf("Failed to update documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.UpdateManyResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

func (s *DBService) DeleteOne(ctx context.Context, req *documentpb.DeleteOneRequest) (*documentpb.DeleteOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	filter := req.Filter.AsMap()

	result, err := s.storage.DocDeleteOne(ctx, req.Collection, filter)
	if err != nil {
		log.Printf("Failed to delete document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.DeleteOneResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

func (s *DBService) DeleteMany(ctx context.Context, req *documentpb.DeleteManyRequest) (*documentpb.DeleteManyResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	filter := req.Filter.AsMap()

	result, err := s.storage.DocDeleteMany(ctx, req.Collection, filter)
	if err != nil {
		log.Printf("Failed to delete documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.DeleteManyResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

func (s *DBService) ReplaceOne(ctx context.Context, req *documentpb.ReplaceOneRequest) (*documentpb.ReplaceOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	if req.Replacement == nil {
		return nil, status.Error(codes.InvalidArgument, "replacement cannot be empty")
	}

	filter := req.Filter.AsMap()
	replacement := req.Replacement.AsMap()

	result, err := s.storage.DocReplaceOne(ctx, req.Collection, filter, replacement, req.Upsert)
	if err != nil {
		log.Printf("Failed to replace document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.ReplaceOneResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

func (s *DBService) UpsertOne(ctx context.Context, req *documentpb.UpsertOneRequest) (*documentpb.UpsertOneResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "filter cannot be empty")
	}

	if req.Document == nil {
		return nil, status.Error(codes.InvalidArgument, "document cannot be empty")
	}

	filter := req.Filter.AsMap()
	document := req.Document.AsMap()

	result, err := s.storage.DocReplaceOne(ctx, req.Collection, filter, document, true)
	if err != nil {
		log.Printf("Failed to upsert document in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.UpsertOneResponse{
		Result: s.convertWriteResultToProto(result),
	}, nil
}

// Aggregation operations

func (s *DBService) Aggregate(ctx context.Context, req *documentpb.AggregateRequest) (*documentpb.AggregateResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	var pipeline []map[string]interface{}
	for _, stage := range req.Pipeline {
		stageMap := make(map[string]interface{})
		stageMap[stage.Stage] = stage.Spec.AsMap()
		pipeline = append(pipeline, stageMap)
	}

	results, err := s.storage.DocAggregate(ctx, req.Collection, pipeline)
	if err != nil {
		log.Printf("Failed to aggregate documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Convert results to protobuf
	var pbResults []*structpb.Struct
	for _, result := range results {
		if resultStruct, err := structpb.NewStruct(result); err == nil {
			pbResults = append(pbResults, resultStruct)
		}
	}

	return &documentpb.AggregateResponse{
		Results: pbResults,
	}, nil
}

func (s *DBService) Count(ctx context.Context, req *documentpb.CountRequest) (*documentpb.CountResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	var filter map[string]interface{}
	if req.Filter != nil {
		filter = req.Filter.AsMap()
	}

	count, err := s.storage.DocCount(ctx, req.Collection, filter)
	if err != nil {
		log.Printf("Failed to count documents in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &documentpb.CountResponse{
		Count: count,
	}, nil
}

func (s *DBService) Distinct(ctx context.Context, req *documentpb.DistinctRequest) (*documentpb.DistinctResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Field == "" {
		return nil, status.Error(codes.InvalidArgument, "field cannot be empty")
	}

	var filter map[string]interface{}
	if req.Filter != nil {
		filter = req.Filter.AsMap()
	}

	values, err := s.storage.DocDistinct(ctx, req.Collection, req.Field, filter)
	if err != nil {
		log.Printf("Failed to get distinct values in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Convert values to protobuf
	var pbValues []*structpb.Value
	for _, value := range values {
		if pbValue, err := structpb.NewValue(value); err == nil {
			pbValues = append(pbValues, pbValue)
		}
	}

	return &documentpb.DistinctResponse{
		Values: pbValues,
	}, nil
}

// Index management

func (s *DBService) CreateIndex(ctx context.Context, req *documentpb.CreateIndexRequest) (*documentpb.CreateIndexResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.Index == nil {
		return nil, status.Error(codes.InvalidArgument, "index specification cannot be empty")
	}

	index := storage.IndexSpec{
		Name:       req.Index.Name,
		Unique:     req.Index.Unique,
		Sparse:     req.Index.Sparse,
		TTLSeconds: req.Index.TtlSeconds,
	}

	if req.Index.Keys != nil {
		index.Keys = req.Index.Keys.AsMap()
	}

	if req.Index.PartialFilter != nil {
		index.PartialFilter = req.Index.PartialFilter.AsMap()
	}

	err := s.storage.DocCreateIndex(ctx, req.Collection, index)
	if err != nil {
		log.Printf("Failed to create index %s in %s: %v", req.Index.Name, req.Collection, err)
		return &documentpb.CreateIndexResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &documentpb.CreateIndexResponse{
		Success: true,
		Message: fmt.Sprintf("Index %s created successfully", req.Index.Name),
	}, nil
}

func (s *DBService) DropIndex(ctx context.Context, req *documentpb.DropIndexRequest) (*documentpb.DropIndexResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	if req.IndexName == "" {
		return nil, status.Error(codes.InvalidArgument, "index name cannot be empty")
	}

	err := s.storage.DocDropIndex(ctx, req.Collection, req.IndexName)
	if err != nil {
		log.Printf("Failed to drop index %s in %s: %v", req.IndexName, req.Collection, err)
		return &documentpb.DropIndexResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &documentpb.DropIndexResponse{
		Success: true,
		Message: fmt.Sprintf("Index %s dropped successfully", req.IndexName),
	}, nil
}

func (s *DBService) ListIndexes(ctx context.Context, req *documentpb.ListIndexesRequest) (*documentpb.ListIndexesResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	indexes, err := s.storage.DocListIndexes(ctx, req.Collection)
	if err != nil {
		log.Printf("Failed to list indexes in %s: %v", req.Collection, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Convert indexes to protobuf
	var pbIndexes []*documentpb.IndexSpec
	for _, idx := range indexes {
		pbIdx := &documentpb.IndexSpec{
			Name:       idx.Name,
			Unique:     idx.Unique,
			Sparse:     idx.Sparse,
			TtlSeconds: idx.TTLSeconds,
		}

		if keysStruct, err := structpb.NewStruct(idx.Keys); err == nil {
			pbIdx.Keys = keysStruct
		}

		if idx.PartialFilter != nil {
			if filterStruct, err := structpb.NewStruct(idx.PartialFilter); err == nil {
				pbIdx.PartialFilter = filterStruct
			}
		}

		pbIndexes = append(pbIndexes, pbIdx)
	}

	return &documentpb.ListIndexesResponse{
		Indexes: pbIndexes,
	}, nil
}

// Bulk operations (simplified implementation)
func (s *DBService) BulkWrite(ctx context.Context, req *documentpb.BulkWriteRequest) (*documentpb.BulkWriteResponse, error) {
	if req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name cannot be empty")
	}

	result := &documentpb.WriteResult{Acknowledged: true}
	var errors []string

	for _, op := range req.Operations {
		switch operation := op.Operation.(type) {
		case *documentpb.BulkOperation_InsertOne:
			if operation.InsertOne.Document != nil {
				document := operation.InsertOne.Document.AsMap()
				if insertedID, err := s.storage.DocInsertOne(ctx, req.Collection, document); err != nil {
					if req.Ordered {
						return nil, status.Error(codes.Internal, err.Error())
					}
					errors = append(errors, err.Error())
				} else {
					result.InsertedCount++
					result.InsertedIds = append(result.InsertedIds, insertedID)
				}
			}
		case *documentpb.BulkOperation_UpdateOne:
			if operation.UpdateOne.Filter != nil && operation.UpdateOne.Update != nil {
				filter := operation.UpdateOne.Filter.AsMap()
				update := s.convertUpdateFromProto(operation.UpdateOne.Update)
				if writeResult, err := s.storage.DocUpdateOne(ctx, req.Collection, filter, update, operation.UpdateOne.Upsert); err != nil {
					if req.Ordered {
						return nil, status.Error(codes.Internal, err.Error())
					}
					errors = append(errors, err.Error())
				} else {
					result.MatchedCount += writeResult.MatchedCount
					result.ModifiedCount += writeResult.ModifiedCount
					result.UpsertedIds = append(result.UpsertedIds, writeResult.UpsertedIDs...)
				}
			}
		case *documentpb.BulkOperation_DeleteOne:
			if operation.DeleteOne.Filter != nil {
				filter := operation.DeleteOne.Filter.AsMap()
				if writeResult, err := s.storage.DocDeleteOne(ctx, req.Collection, filter); err != nil {
					if req.Ordered {
						return nil, status.Error(codes.Internal, err.Error())
					}
					errors = append(errors, err.Error())
				} else {
					result.DeletedCount += writeResult.DeletedCount
				}
			}
		}
	}

	return &documentpb.BulkWriteResponse{
		Result: result,
		Errors: errors,
	}, nil
}

// Transaction operations (simplified - not fully implemented)
func (s *DBService) BeginTransaction(ctx context.Context, req *documentpb.BeginTransactionRequest) (*documentpb.BeginTransactionResponse, error) {
	// For now, return a simple transaction context
	// In a full implementation, this would create an actual transaction
	return &documentpb.BeginTransactionResponse{
		Transaction: &documentpb.TransactionContext{
			TransactionId: "tx_" + fmt.Sprintf("%d", ctx.Value("request_id")),
			StartedAt:     timestamppb.Now(),
			TimeoutSeconds: req.TimeoutSeconds,
		},
	}, nil
}

func (s *DBService) CommitTransaction(ctx context.Context, req *documentpb.CommitTransactionRequest) (*documentpb.CommitTransactionResponse, error) {
	// Simplified implementation - always succeeds
	return &documentpb.CommitTransactionResponse{
		Success: true,
		Message: "Transaction committed successfully",
	}, nil
}

func (s *DBService) AbortTransaction(ctx context.Context, req *documentpb.AbortTransactionRequest) (*documentpb.AbortTransactionResponse, error) {
	// Simplified implementation - always succeeds
	return &documentpb.AbortTransactionResponse{
		Success: true,
		Message: "Transaction aborted successfully",
	}, nil
}

// Helper methods

func (s *DBService) convertUpdateFromProto(update *documentpb.Update) storage.DocumentUpdate {
	result := storage.DocumentUpdate{}

	if update.Set != nil {
		result.Set = update.Set.AsMap()
	}
	if update.Unset != nil {
		result.Unset = update.Unset.AsMap()
	}
	if update.Inc != nil {
		result.Inc = update.Inc.AsMap()
	}
	if update.Push != nil {
		result.Push = update.Push.AsMap()
	}
	if update.Pull != nil {
		result.Pull = update.Pull.AsMap()
	}
	if update.AddToSet != nil {
		result.AddToSet = update.AddToSet.AsMap()
	}

	return result
}

func (s *DBService) convertWriteResultToProto(result storage.DocumentWriteResult) *documentpb.WriteResult {
	return &documentpb.WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIds:   result.InsertedIDs,
		UpsertedIds:   result.UpsertedIDs,
	}
}
