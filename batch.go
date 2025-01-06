package bitcask

import (
	"bitcask/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // pending writes
}

// NewWriteBatch creates a new write batch.
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch with B+ tree index without seq-no file")
	}
	return &WriteBatch{
		options:       options,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put adds a key-value pair to the write batch.
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete deletes a key from the write batch.
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// If the key is not exist, return nil.
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit writes the pending writes to the log file.
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrBatchTooLarge
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// Get the current txn sequence number.
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// Write the pending writes to the log file.
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// Mark the end of the transaction.
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// Sync the log file if SyncWrites is enabled.
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// Update the index.
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		} else if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// Clear the pending writes.
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}
