package db

import (
	"time"
)

// Document represents a document in the database
type Document struct {
	ID        string                 `json:"_id"`
	Data      map[string]interface{} `json:"data"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
	Version   int64                  `json:"version"`
}

// CollectionOptions represents options for creating a collection
type CollectionOptions struct {
	Capped       bool                   `json:"capped,omitempty"`
	MaxSize      int64                  `json:"maxSize,omitempty"`
	MaxDocuments int64                  `json:"maxDocuments,omitempty"`
	Validator    map[string]interface{} `json:"validator,omitempty"`
}

// CollectionInfo represents information about a collection
type CollectionInfo struct {
	Name          string            `json:"name"`
	DocumentCount int64             `json:"documentCount"`
	SizeBytes     int64             `json:"sizeBytes"`
	Indexes       []IndexSpec       `json:"indexes"`
	CreatedAt     time.Time         `json:"createdAt"`
	Options       CollectionOptions `json:"options"`
}

// IndexSpec represents an index specification
type IndexSpec struct {
	Name          string                 `json:"name"`
	Keys          map[string]interface{} `json:"keys"`
	Unique        bool                   `json:"unique"`
	Sparse        bool                   `json:"sparse"`
	TTLSeconds    int32                  `json:"ttlSeconds"`
	PartialFilter map[string]interface{} `json:"partialFilter"`
}

// UpdateOperation represents an update operation
type UpdateOperation struct {
	Set      map[string]interface{} `json:"$set,omitempty"`
	Unset    map[string]interface{} `json:"$unset,omitempty"`
	Inc      map[string]interface{} `json:"$inc,omitempty"`
	Push     map[string]interface{} `json:"$push,omitempty"`
	Pull     map[string]interface{} `json:"$pull,omitempty"`
	AddToSet map[string]interface{} `json:"$addToSet,omitempty"`
}

// WriteResult represents the result of a write operation
type WriteResult struct {
	Acknowledged  bool     `json:"acknowledged"`
	InsertedCount int64    `json:"insertedCount"`
	MatchedCount  int64    `json:"matchedCount"`
	ModifiedCount int64    `json:"modifiedCount"`
	DeletedCount  int64    `json:"deletedCount"`
	InsertedIDs   []string `json:"insertedIds"`
	UpsertedIDs   []string `json:"upsertedIds"`
}

// FindOptions represents options for find operations
type FindOptions struct {
	SortFields map[string]interface{} `json:"sort,omitempty"`
	LimitNum   int32                  `json:"limit,omitempty"`
	SkipNum    int32                  `json:"skip,omitempty"`
	Projection map[string]interface{} `json:"projection,omitempty"`
}

// Helper functions for creating common structures

// NewFilter creates a new filter map
func NewFilter() map[string]interface{} {
	return make(map[string]interface{})
}

// NewUpdate creates a new update operation
func NewUpdate() *UpdateOperation {
	return &UpdateOperation{}
}

// NewFindOptions creates new find options
func NewFindOptions() *FindOptions {
	return &FindOptions{}
}

// NewIndex creates a new index specification
func NewIndex(name string) *IndexSpec {
	return &IndexSpec{
		Name: name,
		Keys: make(map[string]interface{}),
	}
}
