package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc type keySize valueSize
// 4   1    5       5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord is a struct that writes the log record to the log file.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordHeader is a struct that represents the header of a log record.
type LogRecordHeader struct {
	crc       uint32
	Type      LogRecordType
	KeySize   uint32
	ValueSize uint32
}

// LogRecordPos is a struct that represents the position of a log record in the log file.
type LogRecordPos struct {
	Fid    uint32 // File ID
	Offset int64  // Offset in the file
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// DecodeLogRecord decodes a log record from the given buffer.
func DecodeLogRecord(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
