package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    *Options
	mu         *sync.RWMutex
	fileIds    []int                     // The file IDs of the data files, can be only used for initial index.
	activeFile *data.DataFile            // The active data file, which is the file that is currently being written to.
	olderFiles map[uint32]*data.DataFile // The older data files, which can only be read.
	index      index.Indexer             // The index that maps keys to log record positions in memory.
}

// Open opens a database with the given options.
func Open(options *Options) (*DB, error) {
	// Check the options.
	if err := checkOptions(*options); err != nil {
		return nil, err
	}

	// Create the directory if it doesn't exist.
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Init DB struct.
	db := &DB{
		options:    options,
		mu:         &sync.RWMutex{},
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// Load the data file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// Load the index
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
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
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		// todo: handle the old position
		return nil
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if options.MaxLogFileSize <= 0 {
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
	// Get the file ends with ".data"
	for _, dirEntry := range dirEntries {
		if strings.HasSuffix(dirEntry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(dirEntry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// Check if the file name is valid.
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// Sort the file IDs.
	sort.Ints(fileIds)
	// Open the data files.
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // The last file is the active file.
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}
	db.fileIds = fileIds
	return nil
}

// loadIndexFromDataFiles loads the index from the data files.
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

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
				if errors.Is(err, io.EOF) {
					break
				} else {
					return err
				}
			}
			// Update the index.
			logRecordPos := data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDelete {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, &logRecordPos)
			}
			// Move to the next log record.
			offset += size
		}

		// If the last file is the active file, update the active file's write offset.
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}
