package index

import (
	"bitcask/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART creates a new ART index.
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: &sync.RWMutex{},
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	if value, ok := art.tree.Search(key); ok {
		return value.(*data.LogRecordPos)
	}
	return nil
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

type artIterator struct {
	currIndex int     // current index of the iterator
	reverse   bool    // reverse the order of the iterator
	values    []*Item // positions of the keys
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())

	if reverse {
		idx = tree.Size() - 1
	}

	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	ai.currIndex++
}

func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

func (ai *artIterator) Close() {
	ai.values = nil
}
