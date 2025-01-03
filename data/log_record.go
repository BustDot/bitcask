package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

// TransactionRecord is a struct that temporarily store a transaction record.
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord encodes a log record to a byte slice.
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// Initialize the buffer with the maximum header size.
	header := make([]byte, maxLogRecordHeaderSize)
	// The fifth byte is the type of the log record.
	header[4] = logRecord.Type
	var index = 5
	// Encode the key size.
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// Encode the value size.
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	//fmt.Println("header length:", index, "crc:", crc, "key size:", len(logRecord.Key), "value size:", len(logRecord.Value))
	return encBytes, int64(size)
}

// EncodeLogRecordPos encodes a log record position to a byte slice.
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeLogRecordPos decodes a log record position from the given buffer.
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

// DecodeLogRecordHeader decodes a log record from the given buffer.
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) < 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:  binary.LittleEndian.Uint32(buf[:4]),
		Type: buf[4],
	}
	var index = 5
	// Get the key size.
	keySize, keySizeLen := binary.Varint(buf[index:])
	header.KeySize = uint32(keySize)
	index += keySizeLen

	// Get the value size.
	valueSize, valueSizeLen := binary.Varint(buf[index:])
	header.ValueSize = uint32(valueSize)
	index += valueSizeLen

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
