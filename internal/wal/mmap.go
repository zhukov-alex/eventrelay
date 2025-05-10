package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// MmapWriter is an experimental WAL writer using memory-mapped files.
type MmapWriter struct {
	file           *os.File
	data           []byte
	offset         int
	segmentSize    int
	lastSyncOffset int
}

func NewMmapWriter(size int) *MmapWriter {
	return &MmapWriter{segmentSize: size}
}

func (w *MmapWriter) Open(filename string, offset int64) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if err := f.Truncate(int64(w.segmentSize)); err != nil {
		f.Close()
		return err
	}

	data, err := unix.Mmap(int(f.Fd()), 0, w.segmentSize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return err
	}

	w.file = f
	w.data = data
	w.offset = int(offset)
	w.lastSyncOffset = w.offset
	return nil
}

func (w *MmapWriter) Write(p []byte) (int, error) {
	if len(p)+w.offset > w.segmentSize {
		return 0, unix.ENOMEM
	}
	copy(w.data[w.offset:], p)
	w.offset += len(p)
	return len(p), nil
}

//func (w *MmapWriter) Flush() error {
//	if w.offset == 0 {
//		return nil
//	}
//	return unix.Msync(w.data[:w.offset], unix.MS_SYNC)
//}

func (w *MmapWriter) Flush() error {
	if w.offset == 0 {
		return nil
	}

	pageSize := unix.Getpagesize()

	// Align previous sync offset to page boundary
	start := w.lastSyncOffset - (w.lastSyncOffset % pageSize)
	if start < 0 {
		start = 0
	}

	end := w.offset
	length := end - start
	if length <= 0 {
		return nil
	}

	// Only sync the modified region
	if err := unix.Msync(w.data[start:end], unix.MS_SYNC); err != nil {
		return err
	}

	// Advise kernel to evict flushed region
	_ = unix.Madvise(w.data[start:end], unix.MADV_DONTNEED)

	// Update last synced offset
	w.lastSyncOffset = end
	return nil
}

func (w *MmapWriter) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_CUR:
		newOffset = int64(w.offset) + offset
	case os.SEEK_END:
		newOffset = int64(w.segmentSize) + offset
	default:
		return 0, unix.EINVAL
	}

	if newOffset < 0 || newOffset > int64(w.segmentSize) {
		return 0, unix.EINVAL
	}
	w.offset = int(newOffset)
	return newOffset, nil
}

func (w *MmapWriter) Close() error {
	if w.data != nil {
		_ = unix.Munmap(w.data)
	}

	if w.file != nil {
		if err := w.file.Truncate(int64(w.offset)); err != nil {
			return err
		}
		return w.file.Close()
	}

	return nil
}

