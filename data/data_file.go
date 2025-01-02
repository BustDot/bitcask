package data

import (
	"bitcask/fio"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = fmt.Errorf("invalid CRC")
)

const DataFileNameSuffix = ".data" // Data file name suffix

type DataFile struct {
	FileId      uint32        // File ID
	WriteOffset int64         // Write offset
	IOManager   fio.IOManager // File I/O manager
}

// OpenDataFile opens a data file with the given path and file ID.
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(fmt.Sprintf("%09d", fileId) + DataFileNameSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// ReadLogRecord reads a log record from the data file with the given offset.
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// Only need to read to the end of the file.
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// Read the log record header.
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	// If read the end of the file, return EOF.
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize int64 = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.Type}
	// Read the log record.
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// Verify the CRC.
	if header.crc != getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize]) {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil

}

func (df *DataFile) Write(b []byte) error {
	n, err := df.IOManager.Write(b)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

// Sync synchronizes the file's in-memory state with the underlying storage device.
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// Close closes the file.
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
