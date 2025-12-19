package writer

import (
	"context"
	"encoding/csv"
	"os"
)

type CSVWriter struct {
	file        *os.File
	csvWriter   *csv.Writer
	headers     []string
	wroteHeader bool
}

func NewCSVWriter(filepath string, headers []string) (*CSVWriter, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	csvWriter := csv.NewWriter(file)
	writer := &CSVWriter{
		file:      file,
		csvWriter: csvWriter,
		headers:   headers,
	}

	if len(headers) > 0 {
		if err := csvWriter.Write(headers); err != nil {
			file.Close()
			return nil, err
		}
		writer.wroteHeader = true
	}

	return writer, nil
}

func (w *CSVWriter) Write(ctx context.Context, items []map[string]string) error {
	for _, item := range items {
		record := make([]string, len(w.headers))
		for i, header := range w.headers {
			record[i] = item[header]
		}
		if err := w.csvWriter.Write(record); err != nil {
			return err
		}
	}
	w.csvWriter.Flush()
	return w.csvWriter.Error()
}

func (w *CSVWriter) Close() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
