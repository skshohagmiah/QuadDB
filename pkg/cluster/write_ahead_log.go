package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WriteAheadLog provides durability for replication tasks
type WriteAheadLog struct {
	mu sync.RWMutex

	// Configuration
	dir        string
	maxFileSize int64
	syncPolicy  SyncPolicy

	// Current log file
	currentFile *os.File
	currentSize int64
	fileIndex   int

	// Background sync
	syncChan chan struct{}
	ctx      chan struct{}
	wg       sync.WaitGroup

	// Metrics
	totalWrites uint64
	totalSyncs  uint64
	totalBytes  uint64
}

// SyncPolicy defines when to sync WAL to disk
type SyncPolicy int

const (
	SyncEveryWrite SyncPolicy = iota // Sync after every write (safest, slowest)
	SyncPeriodic                     // Sync every N milliseconds (balanced)
	SyncOnClose                      // Sync only on close (fastest, least safe)
)

// WALEntry represents a single entry in the write-ahead log
type WALEntry struct {
	Timestamp time.Time       `json:"timestamp"`
	Type      string          `json:"type"`
	Task      ReplicationTask `json:"task"`
	Checksum  uint32          `json:"checksum"`
}

// NewWriteAheadLog creates a new write-ahead log
func NewWriteAheadLog(dir string) *WriteAheadLog {
	return &WriteAheadLog{
		dir:         dir,
		maxFileSize: 100 * 1024 * 1024, // 100MB per file
		syncPolicy:  SyncPeriodic,
		syncChan:    make(chan struct{}, 1),
		ctx:         make(chan struct{}),
	}
}

// Start initializes the write-ahead log
func (wal *WriteAheadLog) Start() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Create directory if it doesn't exist
	if err := os.MkdirAll(wal.dir, 0755); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Find the latest log file
	if err := wal.openCurrentFile(); err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Start background sync worker
	if wal.syncPolicy == SyncPeriodic {
		wal.wg.Add(1)
		go wal.syncWorker()
	}

	return nil
}

// Stop gracefully shuts down the write-ahead log
func (wal *WriteAheadLog) Stop() error {
	close(wal.ctx)
	wal.wg.Wait()

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.currentFile != nil {
		wal.currentFile.Sync()
		wal.currentFile.Close()
	}

	return nil
}

// WriteTask writes a replication task to the WAL
func (wal *WriteAheadLog) WriteTask(task ReplicationTask) error {
	entry := WALEntry{
		Timestamp: time.Now(),
		Type:      "replication_task",
		Task:      task,
	}

	// Calculate checksum for integrity
	entry.Checksum = wal.calculateChecksum(entry)

	// Serialize entry
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Add newline delimiter
	data = append(data, '\n')

	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Check if we need to rotate the log file
	if wal.currentSize+int64(len(data)) > wal.maxFileSize {
		if err := wal.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate WAL file: %w", err)
		}
	}

	// Write to current file
	n, err := wal.currentFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	wal.currentSize += int64(n)
	wal.totalWrites++
	wal.totalBytes += uint64(n)

	// Sync based on policy
	switch wal.syncPolicy {
	case SyncEveryWrite:
		return wal.currentFile.Sync()
	case SyncPeriodic:
		// Trigger async sync
		select {
		case wal.syncChan <- struct{}{}:
		default:
		}
	}

	return nil
}

// ReadUnprocessedTasks reads tasks that haven't been processed yet
func (wal *WriteAheadLog) ReadUnprocessedTasks() ([]ReplicationTask, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var tasks []ReplicationTask

	// Read from all log files
	files, err := wal.getLogFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to get log files: %w", err)
	}

	for _, filename := range files {
		fileTasks, err := wal.readTasksFromFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read tasks from %s: %w", filename, err)
		}
		tasks = append(tasks, fileTasks...)
	}

	return tasks, nil
}

// openCurrentFile opens or creates the current log file
func (wal *WriteAheadLog) openCurrentFile() error {
	// Find the highest numbered log file
	files, err := wal.getLogFiles()
	if err != nil {
		return err
	}

	if len(files) > 0 {
		// Open the latest file for appending
		latestFile := files[len(files)-1]
		wal.currentFile, err = os.OpenFile(latestFile, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}

		// Get current size
		stat, err := wal.currentFile.Stat()
		if err != nil {
			return err
		}
		wal.currentSize = stat.Size()

		// Extract file index from filename
		fmt.Sscanf(filepath.Base(latestFile), "wal-%d.log", &wal.fileIndex)
	} else {
		// Create first log file
		wal.fileIndex = 0
		filename := filepath.Join(wal.dir, fmt.Sprintf("wal-%06d.log", wal.fileIndex))
		wal.currentFile, err = os.Create(filename)
		if err != nil {
			return err
		}
		wal.currentSize = 0
	}

	return nil
}

// rotateFile creates a new log file
func (wal *WriteAheadLog) rotateFile() error {
	// Close current file
	if wal.currentFile != nil {
		wal.currentFile.Sync()
		wal.currentFile.Close()
	}

	// Create new file
	wal.fileIndex++
	filename := filepath.Join(wal.dir, fmt.Sprintf("wal-%06d.log", wal.fileIndex))
	
	var err error
	wal.currentFile, err = os.Create(filename)
	if err != nil {
		return err
	}

	wal.currentSize = 0
	return nil
}

// getLogFiles returns all log files sorted by index
func (wal *WriteAheadLog) getLogFiles() ([]string, error) {
	entries, err := os.ReadDir(wal.dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".log" {
			files = append(files, filepath.Join(wal.dir, entry.Name()))
		}
	}

	return files, nil
}

// readTasksFromFile reads all tasks from a specific log file
func (wal *WriteAheadLog) readTasksFromFile(filename string) ([]ReplicationTask, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tasks []ReplicationTask
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			// Log corruption - skip this entry
			continue
		}

		// Verify checksum
		expectedChecksum := wal.calculateChecksum(entry)
		if entry.Checksum != expectedChecksum {
			// Corrupted entry - skip
			continue
		}

		tasks = append(tasks, entry.Task)
	}

	return tasks, scanner.Err()
}

// syncWorker periodically syncs the WAL to disk
func (wal *WriteAheadLog) syncWorker() {
	defer wal.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond) // Sync every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-wal.ctx:
			return
		case <-wal.syncChan:
			wal.syncToDisk()
		case <-ticker.C:
			wal.syncToDisk()
		}
	}
}

// syncToDisk syncs the current file to disk
func (wal *WriteAheadLog) syncToDisk() {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if wal.currentFile != nil {
		wal.currentFile.Sync()
		wal.totalSyncs++
	}
}

// calculateChecksum calculates a simple checksum for integrity checking
func (wal *WriteAheadLog) calculateChecksum(entry WALEntry) uint32 {
	// Simple checksum based on task data
	var sum uint32
	for _, b := range []byte(entry.Task.Key) {
		sum += uint32(b)
	}
	for _, b := range entry.Task.Value {
		sum += uint32(b)
	}
	return sum
}

// Cleanup removes old log files (call after successful replication)
func (wal *WriteAheadLog) Cleanup(olderThan time.Time) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	files, err := wal.getLogFiles()
	if err != nil {
		return err
	}

	for _, filename := range files {
		stat, err := os.Stat(filename)
		if err != nil {
			continue
		}

		// Don't delete current file
		if filename == wal.currentFile.Name() {
			continue
		}

		// Delete if older than threshold
		if stat.ModTime().Before(olderThan) {
			os.Remove(filename)
		}
	}

	return nil
}

// GetMetrics returns WAL performance metrics
func (wal *WriteAheadLog) GetMetrics() WALMetrics {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return WALMetrics{
		TotalWrites: wal.totalWrites,
		TotalSyncs:  wal.totalSyncs,
		TotalBytes:  wal.totalBytes,
		CurrentSize: wal.currentSize,
		FileIndex:   wal.fileIndex,
	}
}

// WALMetrics provides insights into WAL performance
type WALMetrics struct {
	TotalWrites uint64
	TotalSyncs  uint64
	TotalBytes  uint64
	CurrentSize int64
	FileIndex   int
}
