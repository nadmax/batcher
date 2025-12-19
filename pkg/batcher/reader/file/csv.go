package reader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type CSVReader struct {
	file      *os.File
	csvReader *csv.Reader
	headers   []string
	closed    bool
}

func NewCSVReader(filepath string, hasHeader bool) (*CSVReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)
	reader := &CSVReader{
		file:      file,
		csvReader: csvReader,
	}

	if hasHeader {
		headers, err := csvReader.Read()
		if err != nil {
			file.Close()
			return nil, err
		}
		reader.headers = headers
	}

	return reader, nil
}

func (r *CSVReader) Read(ctx context.Context) (*map[string]string, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	record, err := r.csvReader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("EOF")
	}
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for i, value := range record {
		if i < len(r.headers) {
			result[r.headers[i]] = value
		} else {
			result[fmt.Sprintf("column_%d", i)] = value
		}
	}

	return &result, nil
}

func (r *CSVReader) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}
