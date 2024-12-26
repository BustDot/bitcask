package data

// LogRecordPos is a struct that represents the position of a log record in the log file.
type LogRecordPos struct {
	Fid    uint32 // File ID
	Offset int64  // Offset in the file
}
