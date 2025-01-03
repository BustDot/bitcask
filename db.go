package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // The file IDs of the data files, can be only used for initial index.
	activeFile *data.DataFile            // The active data file, which is the file that is currently being written to.
	olderFiles map[uint32]*data.DataFile // The older data files, which can only be read.
	index      index.Indexer             // The index that maps keys to log record positions in memory.
	seqNo      uint64                    // The transaction sequence number of the database.
}

// Open opens a database with the given options.
func Open(options Options) (*DB, error) {
	// Check the options.
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// Check if the directory exists. If not, create it.
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Initialize the DB instance.
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// Load the data files.
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// Load the index from the data files.
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return err
		}
	}
	for _, of := range db.olderFiles {
		if of != nil {
			if err := of.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Sync synchronizes the database with the disk.
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put writes a key-value pair to the database. Key cannot be empty.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// Update the index.
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// Get the log record position from the index.
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	// Get the value from the data file.
	return db.getValueByPosition(logRecordPos)
}

// Delete deletes a key from the database.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: nil,
		Type:  data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// Update the index.
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKeys returns all the keys in the database.
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold iterates over all the key-value pairs in the database and calls the given function for each pair. When the function returns false, the iteration stops.
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(key, value) {
			break
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// Get the data file that contains the log record.
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// Read the log record from the data file.
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord appends a log record to the active data file.
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// Init the log file if it doesn't exist.
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// Encode the log record.
	encRecord, size := data.EncodeLogRecord(logRecord)
	// Check if the log record is too large. If it is, open a new data file.
	if db.activeFile.WriteOffset+size > db.options.MaxDataFileSize {
		// Persist the log record to the current data file.
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// Persist the log record to the disk if SyncWrites is enabled.
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// setActiveFile sets the active data file.
// Must be called with db.mu held.
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if options.MaxDataFileSize <= 0 {
		return ErrMaxLogFileSizeInvalid
	}
	return nil
}

// loadDataFiles loads the data files from the directory.
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//	对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个，id是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles loads the index from the data files.
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		switch typ {
		case data.LogRecordNormal:
			ok = db.index.Put(key, pos)
		case data.LogRecordDeleted:
			ok = db.index.Delete(key)
		default:
			ok = false
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// Temporary store the transaction records
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo

	// Traverse all the data files and read the log records from them.
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// Build the index
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// Get the sequence number of the log record key
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo { // non-transaction log record
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else { // transaction log record
				// Txn has finished
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// Update the seqNo
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// Move to the next log record
			offset += size
		}

		// Update the write offset of the active data file
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	// Update the sequence number
	db.seqNo = currentSeqNo
	return nil
}

// parseLogRecordKey parses the log record key and returns the key and the sequence number.
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	return key[n:], seqNo
}
