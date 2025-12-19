package processor

import "context"

type PassThroughProcessor[T any] struct{}

func (p *PassThroughProcessor[T]) Process(ctx context.Context, item T) (*T, error) {
	return &item, nil
}
