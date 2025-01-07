package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO  FileIOType = iota // Standard file I/O
	MemoryMapFIO                   // Memory-mapped file I/O
)

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
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case MemoryMapFIO:
		return NewMMapIOManager(fileName)
	case StandardFIO:
		return NewFileIOManager(fileName)
	default:
		panic("unknown I/O type")
	}
}
