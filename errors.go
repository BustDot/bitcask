package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDirPathIsEmpty         = errors.New("directory path is empty")
	ErrMaxLogFileSizeInvalid  = errors.New("maximum log file size is invalid")
	ErrDataDirectoryCorrupted = errors.New("the database directory is corrupted")
	ErrBatchTooLarge          = errors.New("write batch is too large")
)
