package writer

import (
	"bufio"
	"context"
	"os"
)

type FlatFileWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func NewFlatFileWriter(filepath string) (*FlatFileWriter, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	return &FlatFileWriter{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (w *FlatFileWriter) Write(ctx context.Context, items []string) error {
	for _, item := range items {
		if _, err := w.writer.WriteString(item + "\n"); err != nil {
			return err
		}
	}
	return w.writer.Flush()
}

func (w *FlatFileWriter) Close() error {
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
