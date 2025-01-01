package fio

const DATA_FILE_PERM = 0644

// IOManager is an interface that defines the methods of an I/O manager.
type IOManager interface {
	// Read reads data from the given pos in the file.
	Read([]byte, int64) (int, error)

	// Write writes data to the given pos in the file.
	Write([]byte) (int, error)

	// Sync synchronizes the file.
	Sync() error

	// Close closes the file.
	Close() error

	// Size returns the size of the file.
	Size() (int64, error)
}

// NewIOManager creates a new I/O manager based on the file name.
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
