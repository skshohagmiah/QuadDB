package db

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// Document operations and business logic

// matchesFilter checks if a document matches the given filter
func matchesFilter(document map[string]interface{}, filter map[string]interface{}) bool {
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
					if !compareValues(actualValue, operandValue, ">") {
						return false
					}
				case "$gte":
					if !compareValues(actualValue, operandValue, ">=") {
						return false
					}
				case "$lt":
					if !compareValues(actualValue, operandValue, "<") {
						return false
					}
				case "$lte":
					if !compareValues(actualValue, operandValue, "<=") {
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

// compareValues compares two values with the given operator
func compareValues(a, b interface{}, operator string) bool {
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

// applyProjection applies field projection to a document
func applyProjection(document map[string]interface{}, projection map[string]interface{}) map[string]interface{} {
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

// sortDocuments sorts documents by the given sort specification
func sortDocuments(documents []*Document, sortSpec map[string]interface{}) {
	sort.Slice(documents, func(i, j int) bool {
		for field, order := range sortSpec {
			aVal := getFieldValue(documents[i].Data, field)
			bVal := getFieldValue(documents[j].Data, field)
			
			cmp := compareForSort(aVal, bVal)
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

// getFieldValue gets a field value from a document
func getFieldValue(document map[string]interface{}, field string) interface{} {
	return document[field]
}

// compareForSort compares two values for sorting
func compareForSort(a, b interface{}) int {
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

// applyUpdate applies update operations to a document
func applyUpdate(document map[string]interface{}, update *UpdateOperation) {
	if update == nil {
		return
	}

	// Apply $set operations
	for field, value := range update.Set {
		document[field] = value
	}

	// Apply $unset operations
	for field := range update.Unset {
		delete(document, field)
	}

	// Apply $inc operations
	for field, delta := range update.Inc {
		if current, exists := document[field]; exists {
			if currentNum, ok := toNumber(current); ok {
				if deltaNum, ok := toNumber(delta); ok {
					document[field] = currentNum + deltaNum
				}
			}
		} else {
			if deltaNum, ok := toNumber(delta); ok {
				document[field] = deltaNum
			}
		}
	}

	// Apply $push operations (simplified - adds to array)
	for field, value := range update.Push {
		if current, exists := document[field]; exists {
			if arr, ok := current.([]interface{}); ok {
				document[field] = append(arr, value)
			}
		} else {
			document[field] = []interface{}{value}
		}
	}

	// Apply $addToSet operations (simplified - adds unique values to array)
	for field, value := range update.AddToSet {
		if current, exists := document[field]; exists {
			if arr, ok := current.([]interface{}); ok {
				// Check if value already exists
				found := false
				for _, item := range arr {
					if fmt.Sprintf("%v", item) == fmt.Sprintf("%v", value) {
						found = true
						break
					}
				}
				if !found {
					document[field] = append(arr, value)
				}
			}
		} else {
			document[field] = []interface{}{value}
		}
	}
}

// toNumber converts a value to float64 if possible
func toNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// documentsEqual checks if two documents are equal
func documentsEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists {
			return false
		}
		if fmt.Sprintf("%v", valueA) != fmt.Sprintf("%v", valueB) {
			return false
		}
	}

	return true
}

// applyAggregationPipeline applies aggregation pipeline stages
func applyAggregationPipeline(documents []map[string]interface{}, pipeline []map[string]interface{}) []map[string]interface{} {
	result := documents

	for _, stage := range pipeline {
		for stageName, stageSpec := range stage {
			switch stageName {
			case "$match":
				if filter, ok := stageSpec.(map[string]interface{}); ok {
					var filtered []map[string]interface{}
					for _, doc := range result {
						if matchesFilter(doc, filter) {
							filtered = append(filtered, doc)
						}
					}
					result = filtered
				}
			case "$limit":
				if limit, ok := stageSpec.(float64); ok {
					limitInt := int(limit)
					if limitInt < len(result) {
						result = result[:limitInt]
					}
				}
			case "$skip":
				if skip, ok := stageSpec.(float64); ok {
					skipInt := int(skip)
					if skipInt < len(result) {
						result = result[skipInt:]
					} else {
						result = []map[string]interface{}{}
					}
				}
			case "$sort":
				if sortSpec, ok := stageSpec.(map[string]interface{}); ok {
					sortDocumentsBySpec(result, sortSpec)
				}
			}
		}
	}

	return result
}

// sortDocumentsBySpec sorts raw document maps by sort specification
func sortDocumentsBySpec(documents []map[string]interface{}, sortSpec map[string]interface{}) {
	// Convert to Document slice for sorting
	docs := make([]*Document, len(documents))
	for i, doc := range documents {
		docs[i] = &Document{Data: doc}
	}
	
	sortDocuments(docs, sortSpec)
	
	// Convert back
	for i, doc := range docs {
		documents[i] = doc.Data
	}
}

// parseTime parses a time value from various formats
func parseTime(value interface{}) time.Time {
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

// parseInt64 parses an int64 value from various formats
func parseInt64(value interface{}) int64 {
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
