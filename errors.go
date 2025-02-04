package bitcask

import "errors"

var (
	ErrKeyIsEmpty                = errors.New("key is empty")
	ErrIndexUpdateFailed         = errors.New("index update failed")
	ErrKeyNotFound               = errors.New("key not found")
	ErrDataFileNotFound          = errors.New("data file not found")
	ErrDirPathIsEmpty            = errors.New("directory path is empty")
	ErrMaxLogFileSizeInvalid     = errors.New("maximum log file size is invalid")
	ErrDataDirectoryCorrupted    = errors.New("the database directory is corrupted")
	ErrBatchTooLarge             = errors.New("write batch is too large")
	ErrMergeIsInProgress         = errors.New("merge is already in progress")
	ErrDatabaseLocked            = errors.New("database is locked by another process")
	ErrDataFileMergeRatioInvalid = errors.New("data file merge ratio is invalid, it should be in the range of (0, 1]")
	ErrMergeRatioUnreached       = errors.New("the reclaimable data size is less than the merge ratio")
	ErrDiskSpaceNotEnough        = errors.New("available disk space is not enough for merge")
)
