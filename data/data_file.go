package data

import "bitcask/fio"

const DataFileNameSuffix = ".data" // Data file name suffix

type DataFile struct {
	FileId      uint32        // File ID
	WriteOffset int64         // Write offset
	IOManager   fio.IOManager // File I/O manager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil

}

func (df *DataFile) Write(b []byte) error {
	return nil
}

// Sync synchronizes the file's in-memory state with the underlying storage device.
func (df *DataFile) Sync() error {
	return nil
}
