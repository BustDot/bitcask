package index

import (
	"bitcask/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree is a B+ tree index.
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree creates a new B+ tree index.
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to create B+ tree index")
	}
	// Create the index bucket if it does not exist.
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create B+ tree index bucket")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		return false
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		return nil
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		return 0
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to create B+ tree iterator")
	}

	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return bpi.currKey != nil
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
