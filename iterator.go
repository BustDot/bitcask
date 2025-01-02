package bitcask

import (
	"bitcask/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   *IteratorOptions
}

func (db *DB) NewIterator(options *IteratorOptions) *Iterator {
	return &Iterator{
		indexIter: db.index.Iterator(options.Reverse),
		db:        db,
		options:   options,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek set the iterator to the first key that is greater / less or equal to the given key
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next set the iterator to the next key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid if the iterator has reached the end
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key the current key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value the current value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close the iterator
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
