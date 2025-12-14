package writer

import (
	"context"
	"encoding/json"
	"os"
)

type JSONWriter[T any] struct {
	file    *os.File
	encoder *json.Encoder
	first   bool
}

func NewJSONWriter[T any](filepath string) (*JSONWriter[T], error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	if _, err := file.WriteString("[\n"); err != nil {
		file.Close()
		return nil, err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("  ", "  ")

	return &JSONWriter[T]{
		file:    file,
		encoder: encoder,
		first:   true,
	}, nil
}

func (w *JSONWriter[T]) Write(ctx context.Context, items []T) error {
	for _, item := range items {
		if !w.first {
			if _, err := w.file.WriteString(",\n"); err != nil {
				return err
			}
		}
		w.first = false

		if err := w.encoder.Encode(item); err != nil {
			return err
		}
	}
	return nil
}

func (w *JSONWriter[T]) Close() error {
	if w.file != nil {
		w.file.WriteString("\n]")
		return w.file.Close()
	}
	return nil
}
