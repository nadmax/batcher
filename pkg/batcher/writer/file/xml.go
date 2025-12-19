package writer

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
)

type XMLWriter[T any] struct {
	file     *os.File
	encoder  *xml.Encoder
	rootName string
	started  bool
}

func NewXMLWriter[T any](filepath string, rootName string) (*XMLWriter[T], error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	encoder := xml.NewEncoder(file)
	encoder.Indent("", "  ")

	return &XMLWriter[T]{
		file:     file,
		encoder:  encoder,
		rootName: rootName,
	}, nil
}

func (w *XMLWriter[T]) Write(ctx context.Context, items []T) error {
	if !w.started {
		w.file.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
		w.file.WriteString(fmt.Sprintf("<%s>\n", w.rootName))
		w.started = true
	}

	for _, item := range items {
		if err := w.encoder.Encode(item); err != nil {
			return err
		}
	}
	return nil
}

func (w *XMLWriter[T]) Close() error {
	if w.file != nil {
		w.file.WriteString(fmt.Sprintf("</%s>\n", w.rootName))
		return w.file.Close()
	}
	return nil
}
