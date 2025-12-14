package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type JSONReader[T any] struct {
	file    *os.File
	decoder *json.Decoder
	isArray bool
	started bool
	closed  bool
}

func NewJSONReader[T any](filepath string) (*JSONReader[T], error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(file)

	return &JSONReader[T]{
		file:    file,
		decoder: decoder,
		isArray: true,
	}, nil
}

func (r *JSONReader[T]) Read(ctx context.Context) (*T, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	if !r.started {
		token, err := r.decoder.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("EOF")
		}
		if err != nil {
			return nil, err
		}

		if delim, ok := token.(json.Delim); ok && delim == '[' {
			r.isArray = true
		} else {
			return nil, fmt.Errorf("JSON must be an array")
		}
		r.started = true
	}

	if !r.decoder.More() {
		return nil, fmt.Errorf("EOF")
	}

	var item T
	if err := r.decoder.Decode(&item); err != nil {
		return nil, err
	}

	return &item, nil
}

func (r *JSONReader[T]) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}
