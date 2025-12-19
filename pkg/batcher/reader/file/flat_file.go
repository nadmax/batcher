package reader

import (
	"bufio"
	"context"
	"fmt"
	"os"
)

type FlatFileReader struct {
	file    *os.File
	scanner *bufio.Scanner
	closed  bool
}

func NewFlatFileReader(filepath string) (*FlatFileReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	return &FlatFileReader{
		file:    file,
		scanner: bufio.NewScanner(file),
	}, nil
}

func (r *FlatFileReader) Read(ctx context.Context) (*string, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	if r.scanner.Scan() {
		line := r.scanner.Text()
		return &line, nil
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("EOF")
}

func (r *FlatFileReader) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}
