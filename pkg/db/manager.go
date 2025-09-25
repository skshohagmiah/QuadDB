package db

import (
	"context"

	"github.com/skshohagmiah/fluxdl/storage"
)

// Manager handles document database operations
type Manager struct {
	storage storage.Storage
}

// NewManager creates a new database manager
func NewManager(store storage.Storage) *Manager {
	return &Manager{
		storage: store,
	}
}

// Collection represents a database collection
type Collection struct {
	name    string
	manager *Manager
}

// Collection returns a collection instance for the given name
func (m *Manager) Collection(name string) *Collection {
	return &Collection{
		name:    name,
		manager: m,
	}
}

// CreateCollection creates a new collection with options
func (m *Manager) CreateCollection(ctx context.Context, name string, options *CollectionOptions) error {
	storageOptions := storage.CollectionOptions{}
	if options != nil {
		storageOptions.Capped = options.Capped
		storageOptions.MaxSize = options.MaxSize
		storageOptions.MaxDocuments = options.MaxDocuments
		storageOptions.Validator = options.Validator
	}
	return m.storage.DocCreateCollection(ctx, name, storageOptions)
}

// DropCollection drops a collection
func (m *Manager) DropCollection(ctx context.Context, name string) error {
	return m.storage.DocDropCollection(ctx, name)
}

// ListCollections lists all collections
func (m *Manager) ListCollections(ctx context.Context) ([]string, error) {
	return m.storage.DocListCollections(ctx)
}

// GetCollectionInfo gets information about a collection
func (m *Manager) GetCollectionInfo(ctx context.Context, name string) (*CollectionInfo, error) {
	info, err := m.storage.DocGetCollectionInfo(ctx, name)
	if err != nil {
		return nil, err
	}

	result := &CollectionInfo{
		Name:          info.Name,
		DocumentCount: info.DocumentCount,
		SizeBytes:     info.SizeBytes,
		CreatedAt:     info.CreatedAt,
		Options: CollectionOptions{
			Capped:       info.Options.Capped,
			MaxSize:      info.Options.MaxSize,
			MaxDocuments: info.Options.MaxDocuments,
			Validator:    info.Options.Validator,
		},
	}

	for _, idx := range info.Indexes {
		result.Indexes = append(result.Indexes, IndexSpec{
			Name:          idx.Name,
			Keys:          idx.Keys,
			Unique:        idx.Unique,
			Sparse:        idx.Sparse,
			TTLSeconds:    idx.TTLSeconds,
			PartialFilter: idx.PartialFilter,
		})
	}

	return result, nil
}

// Collection operations

// Insert inserts a single document
func (c *Collection) Insert(ctx context.Context, document map[string]interface{}) (string, error) {
	return c.manager.storage.DocInsertOne(ctx, c.name, document)
}

// InsertOne inserts a single document (alias for Insert)
func (c *Collection) InsertOne(ctx context.Context, document map[string]interface{}) (string, error) {
	return c.Insert(ctx, document)
}

// InsertMany inserts multiple documents
func (c *Collection) InsertMany(ctx context.Context, documents []map[string]interface{}, ordered bool) ([]string, error) {
	return c.manager.storage.DocInsertMany(ctx, c.name, documents, ordered)
}

// Find finds multiple documents
func (c *Collection) Find(ctx context.Context, filter map[string]interface{}, options *FindOptions) ([]*Document, error) {
	// Get all documents from storage (simple key-value retrieval)
	allDocs, _, err := c.manager.storage.DocFindMany(ctx, c.name, storage.DocumentQuery{})
	if err != nil {
		return nil, err
	}

	// Apply filtering at the db layer
	var filteredDocs []*Document
	for _, result := range allDocs {
		if matchesFilter(result.Data, filter) {
			var projection map[string]interface{}
			if options != nil {
				projection = options.Projection
			}
			doc := &Document{
				ID:        result.ID,
				Data:      applyProjection(result.Data, projection),
				CreatedAt: result.CreatedAt,
				UpdatedAt: result.UpdatedAt,
				Version:   result.Version,
			}
			filteredDocs = append(filteredDocs, doc)
		}
	}

	// Apply sorting if specified
	if options != nil && options.SortFields != nil {
		sortDocuments(filteredDocs, options.SortFields)
	}

	// Apply skip and limit
	if options != nil {
		start := int(options.SkipNum)
		if start > len(filteredDocs) {
			start = len(filteredDocs)
		}

		end := start + int(options.LimitNum)
		if options.LimitNum == 0 || end > len(filteredDocs) {
			end = len(filteredDocs)
		}

		filteredDocs = filteredDocs[start:end]
	}

	return filteredDocs, nil
}

// FindOne finds a single document
func (c *Collection) FindOne(ctx context.Context, filter map[string]interface{}, projection map[string]interface{}) (*Document, error) {
	result, err := c.manager.storage.DocFindOne(ctx, c.name, filter, projection)
	if err != nil {
		return nil, err
	}

	if result.ID == "" {
		return nil, nil // Not found
	}

	return &Document{
		ID:        result.ID,
		Data:      result.Data,
		CreatedAt: result.CreatedAt,
		UpdatedAt: result.UpdatedAt,
		Version:   result.Version,
	}, nil
}

// Update updates a single document
func (c *Collection) Update(ctx context.Context, filter map[string]interface{}, update *UpdateOperation, upsert bool) (*WriteResult, error) {
	storageUpdate := storage.DocumentUpdate{}
	if update != nil {
		storageUpdate.Set = update.Set
		storageUpdate.Unset = update.Unset
		storageUpdate.Inc = update.Inc
		storageUpdate.Push = update.Push
		storageUpdate.Pull = update.Pull
		storageUpdate.AddToSet = update.AddToSet
	}

	result, err := c.manager.storage.DocUpdateOne(ctx, c.name, filter, storageUpdate, upsert)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIDs:   result.InsertedIDs,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// UpdateOne updates a single document (alias for Update)
func (c *Collection) UpdateOne(ctx context.Context, filter map[string]interface{}, update *UpdateOperation, upsert bool) (*WriteResult, error) {
	return c.Update(ctx, filter, update, upsert)
}

// UpdateMany updates multiple documents
func (c *Collection) UpdateMany(ctx context.Context, filter map[string]interface{}, update *UpdateOperation) (*WriteResult, error) {
	storageUpdate := storage.DocumentUpdate{}
	if update != nil {
		storageUpdate.Set = update.Set
		storageUpdate.Unset = update.Unset
		storageUpdate.Inc = update.Inc
		storageUpdate.Push = update.Push
		storageUpdate.Pull = update.Pull
		storageUpdate.AddToSet = update.AddToSet
	}

	result, err := c.manager.storage.DocUpdateMany(ctx, c.name, filter, storageUpdate)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIDs:   result.InsertedIDs,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// Delete deletes a single document
func (c *Collection) Delete(ctx context.Context, filter map[string]interface{}) (*WriteResult, error) {
	result, err := c.manager.storage.DocDeleteOne(ctx, c.name, filter)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIDs:   result.InsertedIDs,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// DeleteOne deletes a single document (alias for Delete)
func (c *Collection) DeleteOne(ctx context.Context, filter map[string]interface{}) (*WriteResult, error) {
	return c.Delete(ctx, filter)
}

// DeleteMany deletes multiple documents
func (c *Collection) DeleteMany(ctx context.Context, filter map[string]interface{}) (*WriteResult, error) {
	result, err := c.manager.storage.DocDeleteMany(ctx, c.name, filter)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIDs:   result.InsertedIDs,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// Replace replaces a single document
func (c *Collection) Replace(ctx context.Context, filter map[string]interface{}, replacement map[string]interface{}, upsert bool) (*WriteResult, error) {
	result, err := c.manager.storage.DocReplaceOne(ctx, c.name, filter, replacement, upsert)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Acknowledged:  result.Acknowledged,
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		InsertedIDs:   result.InsertedIDs,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// ReplaceOne replaces a single document (alias for Replace)
func (c *Collection) ReplaceOne(ctx context.Context, filter map[string]interface{}, replacement map[string]interface{}, upsert bool) (*WriteResult, error) {
	return c.Replace(ctx, filter, replacement, upsert)
}

// Count counts documents matching a filter
func (c *Collection) Count(ctx context.Context, filter map[string]interface{}) (int64, error) {
	return c.manager.storage.DocCount(ctx, c.name, filter)
}

// Distinct gets distinct values for a field
func (c *Collection) Distinct(ctx context.Context, field string, filter map[string]interface{}) ([]interface{}, error) {
	return c.manager.storage.DocDistinct(ctx, c.name, field, filter)
}

// Aggregate performs aggregation operations
func (c *Collection) Aggregate(ctx context.Context, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	return c.manager.storage.DocAggregate(ctx, c.name, pipeline)
}

// Index operations

// CreateIndex creates an index
func (c *Collection) CreateIndex(ctx context.Context, index *IndexSpec) error {
	storageIndex := storage.IndexSpec{
		Name:          index.Name,
		Keys:          index.Keys,
		Unique:        index.Unique,
		Sparse:        index.Sparse,
		TTLSeconds:    index.TTLSeconds,
		PartialFilter: index.PartialFilter,
	}
	return c.manager.storage.DocCreateIndex(ctx, c.name, storageIndex)
}

// DropIndex drops an index
func (c *Collection) DropIndex(ctx context.Context, indexName string) error {
	return c.manager.storage.DocDropIndex(ctx, c.name, indexName)
}

// ListIndexes lists all indexes
func (c *Collection) ListIndexes(ctx context.Context) ([]*IndexSpec, error) {
	indexes, err := c.manager.storage.DocListIndexes(ctx, c.name)
	if err != nil {
		return nil, err
	}

	var result []*IndexSpec
	for _, idx := range indexes {
		result = append(result, &IndexSpec{
			Name:          idx.Name,
			Keys:          idx.Keys,
			Unique:        idx.Unique,
			Sparse:        idx.Sparse,
			TTLSeconds:    idx.TTLSeconds,
			PartialFilter: idx.PartialFilter,
		})
	}

	return result, nil
}

// Convenience methods for common operations

// Set is a convenience method for $set updates
func (c *Collection) Set(ctx context.Context, filter map[string]interface{}, fields map[string]interface{}) (*WriteResult, error) {
	update := &UpdateOperation{
		Set: fields,
	}
	return c.Update(ctx, filter, update, false)
}

// Increment is a convenience method for $inc updates
func (c *Collection) Increment(ctx context.Context, filter map[string]interface{}, fields map[string]interface{}) (*WriteResult, error) {
	update := &UpdateOperation{
		Inc: fields,
	}
	return c.Update(ctx, filter, update, false)
}

// Push is a convenience method for $push updates
func (c *Collection) Push(ctx context.Context, filter map[string]interface{}, fields map[string]interface{}) (*WriteResult, error) {
	update := &UpdateOperation{
		Push: fields,
	}
	return c.Update(ctx, filter, update, false)
}

// Upsert is a convenience method for upsert operations
func (c *Collection) Upsert(ctx context.Context, filter map[string]interface{}, document map[string]interface{}) (*WriteResult, error) {
	return c.Replace(ctx, filter, document, true)
}
