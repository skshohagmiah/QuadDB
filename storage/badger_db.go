package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// Document database implementation for BadgerDB

// DocCreateCollection creates a new collection
func (bs *BadgerStorage) DocCreateCollection(ctx context.Context, collection string, options CollectionOptions) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// Check if collection already exists
	collectionKey := fmt.Sprintf("doc:collection:%s", collection)
	
	return bs.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(collectionKey))
		if err == nil {
			return fmt.Errorf("collection %s already exists", collection)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Create collection metadata
		collectionInfo := CollectionInfo{
			Name:          collection,
			DocumentCount: 0,
			SizeBytes:     0,
			Indexes:       []IndexSpec{},
			CreatedAt:     time.Now(),
			Options:       options,
		}

		data, err := json.Marshal(collectionInfo)
		if err != nil {
			return err
		}

		return txn.Set([]byte(collectionKey), data)
	})
}

// DocDropCollection drops a collection and all its documents
func (bs *BadgerStorage) DocDropCollection(ctx context.Context, collection string) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		// Delete collection metadata
		collectionKey := fmt.Sprintf("doc:collection:%s", collection)
		if err := txn.Delete([]byte(collectionKey)); err != nil {
			return err
		}

		// Delete all documents in the collection
		prefix := fmt.Sprintf("doc:data:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete all indexes for the collection
		indexPrefix := fmt.Sprintf("doc:index:%s:", collection)
		for it.Seek([]byte(indexPrefix)); it.ValidForPrefix([]byte(indexPrefix)); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

// DocListCollections lists all collections
func (bs *BadgerStorage) DocListCollections(ctx context.Context) ([]string, error) {
	var collections []string

	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := "doc:collection:"
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			key := string(it.Item().Key())
			collection := strings.TrimPrefix(key, prefix)
			collections = append(collections, collection)
		}

		return nil
	})

	sort.Strings(collections)
	return collections, err
}

// DocGetCollectionInfo gets information about a collection
func (bs *BadgerStorage) DocGetCollectionInfo(ctx context.Context, collection string) (CollectionInfo, error) {
	var info CollectionInfo

	collectionKey := fmt.Sprintf("doc:collection:%s", collection)
	
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(collectionKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("collection %s not found", collection)
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &info)
		})
	})

	return info, err
}

// DocInsertOne inserts a single document
func (bs *BadgerStorage) DocInsertOne(ctx context.Context, collection string, document map[string]interface{}) (string, error) {
	if collection == "" {
		return "", fmt.Errorf("collection name cannot be empty")
	}

	// Generate document ID if not provided
	docID, ok := document["_id"].(string)
	if !ok || docID == "" {
		docID = uuid.New().String()
		document["_id"] = docID
	}

	now := time.Now()
	document["_createdAt"] = now
	document["_updatedAt"] = now
	document["_version"] = int64(1)

	docKey := fmt.Sprintf("doc:data:%s:%s", collection, docID)

	return docID, bs.db.Update(func(txn *badger.Txn) error {
		// Check if document already exists
		_, err := txn.Get([]byte(docKey))
		if err == nil {
			return fmt.Errorf("document with ID %s already exists", docID)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Ensure collection exists
		if err := bs.ensureCollectionExists(txn, collection); err != nil {
			return err
		}

		// Store document
		data, err := json.Marshal(document)
		if err != nil {
			return err
		}

		if err := txn.Set([]byte(docKey), data); err != nil {
			return err
		}

		// Update collection stats
		return bs.updateCollectionStats(txn, collection, 1, int64(len(data)))
	})
}

// DocInsertMany inserts multiple documents
func (bs *BadgerStorage) DocInsertMany(ctx context.Context, collection string, documents []map[string]interface{}, ordered bool) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	var insertedIDs []string
	now := time.Now()

	return insertedIDs, bs.db.Update(func(txn *badger.Txn) error {
		// Ensure collection exists
		if err := bs.ensureCollectionExists(txn, collection); err != nil {
			return err
		}

		var totalSize int64
		for i, document := range documents {
			// Generate document ID if not provided
			docID, ok := document["_id"].(string)
			if !ok || docID == "" {
				docID = uuid.New().String()
				document["_id"] = docID
			}

			document["_createdAt"] = now
			document["_updatedAt"] = now
			document["_version"] = int64(1)

			docKey := fmt.Sprintf("doc:data:%s:%s", collection, docID)

			// Check if document already exists
			_, err := txn.Get([]byte(docKey))
			if err == nil {
				if ordered {
					return fmt.Errorf("document with ID %s already exists at index %d", docID, i)
				}
				continue // Skip existing document in unordered mode
			}
			if err != badger.ErrKeyNotFound {
				return err
			}

			// Store document
			data, err := json.Marshal(document)
			if err != nil {
				if ordered {
					return err
				}
				continue // Skip invalid document in unordered mode
			}

			if err := txn.Set([]byte(docKey), data); err != nil {
				if ordered {
					return err
				}
				continue // Skip on error in unordered mode
			}

			insertedIDs = append(insertedIDs, docID)
			totalSize += int64(len(data))
		}

		// Update collection stats
		return bs.updateCollectionStats(txn, collection, int64(len(insertedIDs)), totalSize)
	})
}

// DocFindOne finds a single document
func (bs *BadgerStorage) DocFindOne(ctx context.Context, collection string, filter map[string]interface{}, projection map[string]interface{}) (DocumentResult, error) {
	var result DocumentResult

	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("doc:data:%s:", collection)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var document map[string]interface{}
				if err := json.Unmarshal(val, &document); err != nil {
					return err
				}

				if bs.matchesFilter(document, filter) {
					result = DocumentResult{
						ID:        document["_id"].(string),
						Data:      bs.applyProjection(document, projection),
						CreatedAt: bs.parseTime(document["_createdAt"]),
						UpdatedAt: bs.parseTime(document["_updatedAt"]),
						Version:   bs.parseInt64(document["_version"]),
					}
					return nil
				}
				return nil
			})

			if err != nil {
				return err
			}

			if result.ID != "" {
				break
			}
		}

		return nil
	})

	return result, err
}

// DocFindMany finds multiple documents
func (bs *BadgerStorage) DocFindMany(ctx context.Context, collection string, query DocumentQuery) ([]DocumentResult, int64, error) {
	var results []DocumentResult
	var totalCount int64

	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("doc:data:%s:", collection)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		var allMatches []DocumentResult
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var document map[string]interface{}
				if err := json.Unmarshal(val, &document); err != nil {
					return err
				}

				if bs.matchesFilter(document, query.Filter) {
					totalCount++
					allMatches = append(allMatches, DocumentResult{
						ID:        document["_id"].(string),
						Data:      bs.applyProjection(document, query.Projection),
						CreatedAt: bs.parseTime(document["_createdAt"]),
						UpdatedAt: bs.parseTime(document["_updatedAt"]),
						Version:   bs.parseInt64(document["_version"]),
					})
				}
				return nil
			})

			if err != nil {
				return err
			}
		}

		// Apply sorting
		if query.Sort != nil {
			bs.sortDocuments(allMatches, query.Sort)
		}

		// Apply skip and limit
		start := int(query.Skip)
		if start > len(allMatches) {
			start = len(allMatches)
		}

		end := start + int(query.Limit)
		if query.Limit == 0 || end > len(allMatches) {
			end = len(allMatches)
		}

		results = allMatches[start:end]
		return nil
	})

	return results, totalCount, err
}

// Helper methods

func (bs *BadgerStorage) ensureCollectionExists(txn *badger.Txn, collection string) error {
	collectionKey := fmt.Sprintf("doc:collection:%s", collection)
	_, err := txn.Get([]byte(collectionKey))
	if err == badger.ErrKeyNotFound {
		// Create collection with default options
		collectionInfo := CollectionInfo{
			Name:          collection,
			DocumentCount: 0,
			SizeBytes:     0,
			Indexes:       []IndexSpec{},
			CreatedAt:     time.Now(),
			Options:       CollectionOptions{},
		}

		data, err := json.Marshal(collectionInfo)
		if err != nil {
			return err
		}

		return txn.Set([]byte(collectionKey), data)
	}
	return err
}

func (bs *BadgerStorage) updateCollectionStats(txn *badger.Txn, collection string, docDelta int64, sizeDelta int64) error {
	collectionKey := fmt.Sprintf("doc:collection:%s", collection)
	
	item, err := txn.Get([]byte(collectionKey))
	if err != nil {
		return err
	}

	var info CollectionInfo
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &info)
	})
	if err != nil {
		return err
	}

	info.DocumentCount += docDelta
	info.SizeBytes += sizeDelta

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	return txn.Set([]byte(collectionKey), data)
}

func (bs *BadgerStorage) matchesFilter(document map[string]interface{}, filter map[string]interface{}) bool {
	if filter == nil || len(filter) == 0 {
		return true
	}

	for key, expectedValue := range filter {
		actualValue, exists := document[key]
		if !exists {
			return false
		}

		// Handle MongoDB-style operators
		if filterMap, ok := expectedValue.(map[string]interface{}); ok {
			for operator, operandValue := range filterMap {
				switch operator {
				case "$eq":
					if actualValue != operandValue {
						return false
					}
				case "$ne":
					if actualValue == operandValue {
						return false
					}
				case "$gt":
					if !bs.compareValues(actualValue, operandValue, ">") {
						return false
					}
				case "$gte":
					if !bs.compareValues(actualValue, operandValue, ">=") {
						return false
					}
				case "$lt":
					if !bs.compareValues(actualValue, operandValue, "<") {
						return false
					}
				case "$lte":
					if !bs.compareValues(actualValue, operandValue, "<=") {
						return false
					}
				case "$in":
					if values, ok := operandValue.([]interface{}); ok {
						found := false
						for _, v := range values {
							if actualValue == v {
								found = true
								break
							}
						}
						if !found {
							return false
						}
					}
				case "$nin":
					if values, ok := operandValue.([]interface{}); ok {
						for _, v := range values {
							if actualValue == v {
								return false
							}
						}
					}
				}
			}
		} else {
			// Simple equality check
			if actualValue != expectedValue {
				return false
			}
		}
	}

	return true
}

func (bs *BadgerStorage) compareValues(a, b interface{}, operator string) bool {
	// Convert to comparable types
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	
	// Try numeric comparison first
	if aNum, aErr := strconv.ParseFloat(aStr, 64); aErr == nil {
		if bNum, bErr := strconv.ParseFloat(bStr, 64); bErr == nil {
			switch operator {
			case ">":
				return aNum > bNum
			case ">=":
				return aNum >= bNum
			case "<":
				return aNum < bNum
			case "<=":
				return aNum <= bNum
			}
		}
	}

	// Fall back to string comparison
	switch operator {
	case ">":
		return aStr > bStr
	case ">=":
		return aStr >= bStr
	case "<":
		return aStr < bStr
	case "<=":
		return aStr <= bStr
	}

	return false
}

func (bs *BadgerStorage) applyProjection(document map[string]interface{}, projection map[string]interface{}) map[string]interface{} {
	if projection == nil || len(projection) == 0 {
		return document
	}

	result := make(map[string]interface{})
	
	// Check if it's an inclusion or exclusion projection
	isInclusion := false
	for _, value := range projection {
		if value == 1 || value == true {
			isInclusion = true
			break
		}
	}

	if isInclusion {
		// Include only specified fields
		for field, include := range projection {
			if include == 1 || include == true {
				if value, exists := document[field]; exists {
					result[field] = value
				}
			}
		}
		// Always include _id unless explicitly excluded
		if _, excluded := projection["_id"]; !excluded {
			if id, exists := document["_id"]; exists {
				result["_id"] = id
			}
		}
	} else {
		// Exclude specified fields
		for key, value := range document {
			exclude := false
			if projValue, exists := projection[key]; exists && (projValue == 0 || projValue == false) {
				exclude = true
			}
			if !exclude {
				result[key] = value
			}
		}
	}

	return result
}

func (bs *BadgerStorage) sortDocuments(documents []DocumentResult, sortSpec map[string]interface{}) {
	sort.Slice(documents, func(i, j int) bool {
		for field, order := range sortSpec {
			aVal := bs.getFieldValue(documents[i].Data, field)
			bVal := bs.getFieldValue(documents[j].Data, field)
			
			cmp := bs.compareForSort(aVal, bVal)
			if cmp != 0 {
				if order == -1 {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

func (bs *BadgerStorage) getFieldValue(document map[string]interface{}, field string) interface{} {
	return document[field]
}

func (bs *BadgerStorage) compareForSort(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	
	// Try numeric comparison first
	if aNum, aErr := strconv.ParseFloat(aStr, 64); aErr == nil {
		if bNum, bErr := strconv.ParseFloat(bStr, 64); bErr == nil {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (bs *BadgerStorage) parseTime(value interface{}) time.Time {
	if t, ok := value.(time.Time); ok {
		return t
	}
	if str, ok := value.(string); ok {
		if t, err := time.Parse(time.RFC3339, str); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (bs *BadgerStorage) parseInt64(value interface{}) int64 {
	if i, ok := value.(int64); ok {
		return i
	}
	if f, ok := value.(float64); ok {
		return int64(f)
	}
	if str, ok := value.(string); ok {
		if i, err := strconv.ParseInt(str, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

// DocUpdateOne updates a single document
func (bs *BadgerStorage) DocUpdateOne(ctx context.Context, collection string, filter map[string]interface{}, update DocumentUpdate, upsert bool) (DocumentWriteResult, error) {
	if collection == "" {
		return DocumentWriteResult{}, fmt.Errorf("collection name cannot be empty")
	}

	var result DocumentWriteResult
	
	err := bs.db.Update(func(txn *badger.Txn) error {
		// Find the document to update
		var foundDoc map[string]interface{}
		var docID string
		var found bool
		
		// Iterate through documents to find matching one
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					foundDoc = doc
					// Extract document ID from key
					parts := strings.Split(key, ":")
					if len(parts) >= 4 {
						docID = parts[3]
					}
					found = true
					return fmt.Errorf("found") // Break the loop
				}
				return nil
			})
			
			if err != nil && err.Error() == "found" {
				break
			} else if err != nil {
				return err
			}
		}
		
		if !found {
			if upsert {
				// Create new document
				docID = uuid.New().String()
				foundDoc = make(map[string]interface{})
				
				// Apply filter as initial values for upsert
				for k, v := range filter {
					foundDoc[k] = v
				}
			} else {
				result.MatchedCount = 0
				result.ModifiedCount = 0
				return nil
			}
		} else {
			result.MatchedCount = 1
		}
		
		// Apply update operations
		updatedDoc := bs.applyUpdate(foundDoc, update)
		
		// Add system fields
		now := time.Now()
		updatedDoc["_updatedAt"] = now
		if !found {
			updatedDoc["_id"] = docID
			updatedDoc["_createdAt"] = now
			updatedDoc["_version"] = int64(1)
		} else {
			if version, ok := updatedDoc["_version"].(int64); ok {
				updatedDoc["_version"] = version + 1
			} else {
				updatedDoc["_version"] = int64(1)
			}
		}
		
		// Save updated document
		docKey := fmt.Sprintf("doc:collection:%s:%s", collection, docID)
		docData, err := json.Marshal(updatedDoc)
		if err != nil {
			return err
		}
		
		if err := txn.Set([]byte(docKey), docData); err != nil {
			return err
		}
		
		if found {
			result.ModifiedCount = 1
		} else {
			result.UpsertedIDs = []string{docID}
		}
		
		return nil
	})
	
	return result, err
}

// DocUpdateMany updates multiple documents
func (bs *BadgerStorage) DocUpdateMany(ctx context.Context, collection string, filter map[string]interface{}, update DocumentUpdate) (DocumentWriteResult, error) {
	if collection == "" {
		return DocumentWriteResult{}, fmt.Errorf("collection name cannot be empty")
	}

	var result DocumentWriteResult
	
	err := bs.db.Update(func(txn *badger.Txn) error {
		// Find all matching documents
		var docsToUpdate []struct {
			key string
			doc map[string]interface{}
			id  string
		}
		
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					// Extract document ID from key
					parts := strings.Split(key, ":")
					docID := ""
					if len(parts) >= 4 {
						docID = parts[3]
					}
					
					docsToUpdate = append(docsToUpdate, struct {
						key string
						doc map[string]interface{}
						id  string
					}{key, doc, docID})
				}
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		result.MatchedCount = int64(len(docsToUpdate))
		
		// Update all matching documents
		for _, docInfo := range docsToUpdate {
			updatedDoc := bs.applyUpdate(docInfo.doc, update)
			
			// Update system fields
			now := time.Now()
			updatedDoc["_updatedAt"] = now
			if version, ok := updatedDoc["_version"].(int64); ok {
				updatedDoc["_version"] = version + 1
			} else {
				updatedDoc["_version"] = int64(1)
			}
			
			// Save updated document
			docData, err := json.Marshal(updatedDoc)
			if err != nil {
				return err
			}
			
			if err := txn.Set([]byte(docInfo.key), docData); err != nil {
				return err
			}
			
			result.ModifiedCount++
		}
		
		return nil
	})
	
	return result, err
}

// DocDeleteOne deletes a single document
func (bs *BadgerStorage) DocDeleteOne(ctx context.Context, collection string, filter map[string]interface{}) (DocumentWriteResult, error) {
	if collection == "" {
		return DocumentWriteResult{}, fmt.Errorf("collection name cannot be empty")
	}

	var result DocumentWriteResult
	
	err := bs.db.Update(func(txn *badger.Txn) error {
		// Find the document to delete
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			var shouldDelete bool
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					shouldDelete = true
				}
				return nil
			})
			
			if err != nil {
				return err
			}
			
			if shouldDelete {
				if err := txn.Delete([]byte(key)); err != nil {
					return err
				}
				result.DeletedCount = 1
				break
			}
		}
		
		return nil
	})
	
	return result, err
}

// DocDeleteMany deletes multiple documents
func (bs *BadgerStorage) DocDeleteMany(ctx context.Context, collection string, filter map[string]interface{}) (DocumentWriteResult, error) {
	if collection == "" {
		return DocumentWriteResult{}, fmt.Errorf("collection name cannot be empty")
	}

	var result DocumentWriteResult
	
	err := bs.db.Update(func(txn *badger.Txn) error {
		// Find all matching documents to delete
		var keysToDelete []string
		
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					keysToDelete = append(keysToDelete, key)
				}
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		// Delete all matching documents
		for _, key := range keysToDelete {
			if err := txn.Delete([]byte(key)); err != nil {
				return err
			}
			result.DeletedCount++
		}
		
		return nil
	})
	
	return result, err
}

// DocReplaceOne replaces a single document
func (bs *BadgerStorage) DocReplaceOne(ctx context.Context, collection string, filter map[string]interface{}, replacement map[string]interface{}, upsert bool) (DocumentWriteResult, error) {
	if collection == "" {
		return DocumentWriteResult{}, fmt.Errorf("collection name cannot be empty")
	}

	var result DocumentWriteResult
	
	err := bs.db.Update(func(txn *badger.Txn) error {
		// Find the document to replace
		var docKey string
		var docID string
		var found bool
		
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					docKey = key
					// Extract document ID from key
					parts := strings.Split(key, ":")
					if len(parts) >= 4 {
						docID = parts[3]
					}
					found = true
					return fmt.Errorf("found") // Break the loop
				}
				return nil
			})
			
			if err != nil && err.Error() == "found" {
				break
			} else if err != nil {
				return err
			}
		}
		
		if !found {
			if upsert {
				// Create new document
				docID = uuid.New().String()
				docKey = fmt.Sprintf("doc:collection:%s:%s", collection, docID)
			} else {
				result.MatchedCount = 0
				result.ModifiedCount = 0
				return nil
			}
		} else {
			result.MatchedCount = 1
		}
		
		// Create replacement document
		newDoc := make(map[string]interface{})
		for k, v := range replacement {
			newDoc[k] = v
		}
		
		// Add system fields
		now := time.Now()
		newDoc["_id"] = docID
		newDoc["_updatedAt"] = now
		if !found {
			newDoc["_createdAt"] = now
			newDoc["_version"] = int64(1)
		} else {
			newDoc["_version"] = int64(1) // Reset version for replacement
		}
		
		// Save replacement document
		docData, err := json.Marshal(newDoc)
		if err != nil {
			return err
		}
		
		if err := txn.Set([]byte(docKey), docData); err != nil {
			return err
		}
		
		if found {
			result.ModifiedCount = 1
		} else {
			result.UpsertedIDs = []string{docID}
		}
		
		return nil
	})
	
	return result, err
}

// DocAggregate performs aggregation operations
func (bs *BadgerStorage) DocAggregate(ctx context.Context, collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	var results []map[string]interface{}
	
	err := bs.db.View(func(txn *badger.Txn) error {
		// Start with all documents in the collection
		var documents []map[string]interface{}
		
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				documents = append(documents, doc)
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		// Apply pipeline stages
		currentDocs := documents
		for _, stage := range pipeline {
			currentDocs = bs.applyAggregationStage(currentDocs, stage)
		}
		
		results = currentDocs
		return nil
	})
	
	return results, err
}

// DocCount counts documents matching a filter
func (bs *BadgerStorage) DocCount(ctx context.Context, collection string, filter map[string]interface{}) (int64, error) {
	if collection == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	var count int64
	
	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					count++
				}
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return count, err
}

// DocDistinct returns distinct values for a field
func (bs *BadgerStorage) DocDistinct(ctx context.Context, collection string, field string, filter map[string]interface{}) ([]interface{}, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}
	if field == "" {
		return nil, fmt.Errorf("field name cannot be empty")
	}

	distinctValues := make(map[interface{}]bool)
	var results []interface{}
	
	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("doc:collection:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Skip collection metadata
			if key == fmt.Sprintf("doc:collection:%s", collection) {
				continue
			}
			
			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := json.Unmarshal(val, &doc); err != nil {
					return err
				}
				
				if bs.matchesFilter(doc, filter) {
					if value, exists := doc[field]; exists {
						if !distinctValues[value] {
							distinctValues[value] = true
							results = append(results, value)
						}
					}
				}
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return results, err
}

// DocCreateIndex creates an index on a collection
func (bs *BadgerStorage) DocCreateIndex(ctx context.Context, collection string, index IndexSpec) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if index.Name == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		// Store index specification
		indexKey := fmt.Sprintf("doc:index:%s:%s", collection, index.Name)
		indexData, err := json.Marshal(index)
		if err != nil {
			return err
		}
		
		return txn.Set([]byte(indexKey), indexData)
	})
}

// DocDropIndex drops an index from a collection
func (bs *BadgerStorage) DocDropIndex(ctx context.Context, collection string, indexName string) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		indexKey := fmt.Sprintf("doc:index:%s:%s", collection, indexName)
		return txn.Delete([]byte(indexKey))
	})
}

// DocListIndexes lists all indexes for a collection
func (bs *BadgerStorage) DocListIndexes(ctx context.Context, collection string) ([]IndexSpec, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	var indexes []IndexSpec
	
	err := bs.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("doc:index:%s:", collection)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var index IndexSpec
				if err := json.Unmarshal(val, &index); err != nil {
					return err
				}
				indexes = append(indexes, index)
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return indexes, err
}

// Helper method to apply update operations
func (bs *BadgerStorage) applyUpdate(doc map[string]interface{}, update DocumentUpdate) map[string]interface{} {
	result := make(map[string]interface{})
	
	// Copy original document
	for k, v := range doc {
		result[k] = v
	}
	
	// Apply $set operations
	if update.Set != nil {
		for k, v := range update.Set {
			result[k] = v
		}
	}
	
	// Apply $unset operations
	if update.Unset != nil {
		for k := range update.Unset {
			delete(result, k)
		}
	}
	
	// Apply $inc operations
	if update.Inc != nil {
		for k, v := range update.Inc {
			if current, exists := result[k]; exists {
				if currentNum, ok := current.(float64); ok {
					if incNum, ok := v.(float64); ok {
						result[k] = currentNum + incNum
					}
				}
			} else {
				result[k] = v
			}
		}
	}
	
	return result
}

// Helper method to apply aggregation pipeline stages
func (bs *BadgerStorage) applyAggregationStage(docs []map[string]interface{}, stage map[string]interface{}) []map[string]interface{} {
	for stageType, stageSpec := range stage {
		switch stageType {
		case "$match":
			if filter, ok := stageSpec.(map[string]interface{}); ok {
				var filtered []map[string]interface{}
				for _, doc := range docs {
					if bs.matchesFilter(doc, filter) {
						filtered = append(filtered, doc)
					}
				}
				docs = filtered
			}
		case "$limit":
			if limit, ok := stageSpec.(float64); ok {
				limitInt := int(limit)
				if limitInt < len(docs) {
					docs = docs[:limitInt]
				}
			}
		case "$skip":
			if skip, ok := stageSpec.(float64); ok {
				skipInt := int(skip)
				if skipInt < len(docs) {
					docs = docs[skipInt:]
				} else {
					docs = []map[string]interface{}{}
				}
			}
		case "$sort":
			if sortSpec, ok := stageSpec.(map[string]interface{}); ok {
				bs.sortDocuments(bs.convertToDocumentResults(docs), sortSpec)
			}
		}
	}
	return docs
}

// Helper method to convert documents to DocumentResult format for sorting
func (bs *BadgerStorage) convertToDocumentResults(docs []map[string]interface{}) []DocumentResult {
	results := make([]DocumentResult, len(docs))
	for i, doc := range docs {
		results[i] = DocumentResult{Data: doc}
	}
	return results
}
