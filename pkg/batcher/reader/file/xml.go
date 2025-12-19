package reader

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
)

type XMLReader[T any] struct {
	file     *os.File
	decoder  *xml.Decoder
	rootName string
	started  bool
	closed   bool
}

func NewXMLReader[T any](filepath string, rootName string) (*XMLReader[T], error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	decoder := xml.NewDecoder(file)

	return &XMLReader[T]{
		file:     file,
		decoder:  decoder,
		rootName: rootName,
	}, nil
}

func (r *XMLReader[T]) Read(ctx context.Context) (*T, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	for {
		token, err := r.decoder.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("EOF")
		}
		if err != nil {
			return nil, err
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == r.rootName {
			var item T
			if err := r.decoder.DecodeElement(&item, &se); err != nil {
				return nil, err
			}
			return &item, nil
		}
	}
}

func (r *XMLReader[T]) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}
