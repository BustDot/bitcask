package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *Btree) Size() int {
	return bt.tree.Len()
}

func (bt *Btree) Close() error {
	return nil
}

func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

type BtreeIterator struct {
	currIndex int     // current index of the iterator
	reverse   bool    // reverse the order of the iterator
	values    []*Item // positions of the keys
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// saveValues is a closure that saves the values of the tree into the values slice
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &BtreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *BtreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *BtreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *BtreeIterator) Next() {
	bti.currIndex++
}

func (bti *BtreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *BtreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *BtreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *BtreeIterator) Close() {
	bti.values = nil
}
