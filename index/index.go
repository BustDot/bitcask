package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer is an interface that defines the methods of an index.
type Indexer interface {
	// Put inserts a key and its position into the index.
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get retrieves the position of a key from the index.
	Get(key []byte) *data.LogRecordPos

	// Delete deletes a key from the index.
	Delete(key []byte) bool

	// Size returns the number of keys in the index.
	Size() int

	// Iterator returns an iterator for the index.
	Iterator(reverse bool) Iterator

	// Close closes the index.
	Close() error
}

type IndexType = int8

const (
	BTree IndexType = iota + 1
	ART
	BPTree
)

// NewIndexer creates a new index based on the index type.
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	// Rewind go back to the first key
	Rewind()

	// Seek set the iterator to the first key that is greater / less or equal to the given key
	Seek(key []byte)

	// Next set the iterator to the next key
	Next()

	// Valid if the iterator has reached the end
	Valid() bool

	// Key the current key
	Key() []byte

	// Value the current value
	Value() *data.LogRecordPos

	// Close the iterator
	Close()
}
