package reader

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

type FixedWidthReader struct {
	file       *os.File
	scanner    *bufio.Scanner
	columnDefs []ColumnDefinition
	closed     bool
}

type ColumnDefinition struct {
	Name  string
	Start int
	End   int
}

func NewFixedWidthReader(filepath string, columnDefs []ColumnDefinition) (*FixedWidthReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	return &FixedWidthReader{
		file:       file,
		scanner:    bufio.NewScanner(file),
		columnDefs: columnDefs,
	}, nil
}

func (r *FixedWidthReader) Read(ctx context.Context) (*map[string]string, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	if r.scanner.Scan() {
		line := r.scanner.Text()
		record := make(map[string]string)

		for _, col := range r.columnDefs {
			if col.End <= len(line) {
				record[col.Name] = strings.TrimSpace(line[col.Start:col.End])
			} else if col.Start < len(line) {
				record[col.Name] = strings.TrimSpace(line[col.Start:])
			} else {
				record[col.Name] = ""
			}
		}

		return &record, nil
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("EOF")
}

func (r *FixedWidthReader) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}
