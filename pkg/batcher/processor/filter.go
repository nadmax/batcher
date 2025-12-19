package processor

import "context"

type FilterProcessor[T any] struct {
	Predicate func(T) bool
}

func (p *FilterProcessor[T]) Process(ctx context.Context, item T) (*T, error) {
	if p.Predicate(item) {
		return &item, nil
	}

	return nil, nil
}
