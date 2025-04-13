package wal

import (
	"bufio"
	"io"
	"os"
)

type BufferedWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func NewBufferedWriter() *BufferedWriter {
	return &BufferedWriter{}
}

func (w *BufferedWriter) Open(filename string, offset int64) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return err
		}
	}
	w.file = f
	w.writer = bufio.NewWriter(f)
	return nil
}

func (w *BufferedWriter) Write(p []byte) (int, error) {
	if w.writer == nil {
		return 0, os.ErrInvalid
	}
	return w.writer.Write(p)
}

func (w *BufferedWriter) Flush() error {
	if w.writer == nil {
		return nil
	}
	return w.writer.Flush()
}

func (w *BufferedWriter) Close() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *BufferedWriter) Seek(offset int64, whence int) (int64, error) {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return 0, err
		}
	}
	if w.file == nil {
		return 0, os.ErrInvalid
	}
	return w.file.Seek(offset, whence)
}
