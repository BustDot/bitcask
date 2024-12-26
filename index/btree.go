package index

import (
	"bitcask/data"
	"github.com/google/btree"
	"sync"
)

// Btree is a struct that represents a B-tree index.
type Btree struct {
	tree *btree.BTree // concurrent write is not supported
	lock *sync.RWMutex
}

// NewBtree creates a new B-tree index.
func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}
