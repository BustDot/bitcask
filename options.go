package bitcask

type Options struct {
	DirPath        string      // DB directory path
	MaxLogFileSize int64       // Maximum log file size
	SyncWrites     bool        // Whether to sync writes to disk
	IndexType      IndexerType // Indexer type
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
)
