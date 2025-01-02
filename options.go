package bitcask

import "os"

type Options struct {
	DirPath         string      // DB directory path
	MaxDataFileSize int64       // Maximum log file size
	SyncWrites      bool        // Whether to sync writes to disk
	IndexType       IndexerType // Indexer type
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
)

var DefaultOptions = &Options{
	DirPath:         os.TempDir(),
	MaxDataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:      false,
	IndexType:       BTree,
}
