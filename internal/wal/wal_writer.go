package wal

// WalWriter defines methods for writing WAL data.
type WalWriter interface {
	Open(filename string, offset int64) error
	Write(p []byte) (n int, err error)
	Flush() error
	Close() error
	Seek(offset int64, whence int) (int64, error)
}
