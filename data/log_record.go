package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// LogRecord is a struct that writes the log record to the log file.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos is a struct that represents the position of a log record in the log file.
type LogRecordPos struct {
	Fid    uint32 // File ID
	Offset int64  // Offset in the file
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
