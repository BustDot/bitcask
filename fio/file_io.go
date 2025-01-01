package fio

import "os"

// FileIO is a standard file I/O manager.
type FileIO struct {
	fd *os.File
}

// NewFileIOManager creates a new FileIO instance.
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DATA_FILE_PERM)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	fi, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
