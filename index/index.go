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
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
