package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"sync"
)

type DB struct {
	options    *Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // The active data file, which is the file that is currently being written to.
	olderFiles map[uint32]*data.DataFile // The older data files, which can only be read.
	index      index.Indexer             // The index that maps keys to log record positions in memory.
}

func NewDB() *DB {
	return &DB{}
}

// Put writes a key-value pair to the database. Key cannot be empty.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(log_record)
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
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// appendLogRecord appends a log record to the active data file.
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Init the log file if it doesn't exist.
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// Encode the log record.
	encRecord, size := data.EncodeLogRecord(logRecord)
	// Check if the log record is too large. If it is, open a new data file.
	if db.activeFile.WriteOffset+size > db.options.MaxLogFileSize {
		// Persist the log record to the current data file.
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// Persist the log record to the disk if SyncWrites is enabled.
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	db.activeFile.WriteOffset += size

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: db.activeFile.WriteOffset}
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
