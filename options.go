package bitcask

import "os"

type Options struct {
	DirPath         string      // DB directory path
	MaxDataFileSize int64       // Maximum log file size
	SyncWrites      bool        // Whether to sync writes to disk
	IndexType       IndexerType // Indexer type
}

type IteratorOptions struct {
	Prefix  []byte // Prefix to filter keys
	Reverse bool   // Reverse the iteration order
}

// WriteBatchOptions specifies the options for a write batch.
type WriteBatchOptions struct {
	MaxBatchNum uint // Maximum number of entries in a batch
	SyncWrites  bool // Whether to sync writes to disk when committing a batch
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
	BPTree // B+ tree, it will persist the index to disk
)

var DefaultOptions = Options{
	DirPath:         os.TempDir(),
	MaxDataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:      false,
	IndexType:       BTree,
}

var DefaultIteratorOptions = &IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
