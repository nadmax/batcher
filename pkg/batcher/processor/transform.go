package processor

import "context"

type TransformProcessor[I, O any] struct {
	Transform func(I) (O, error)
}

func (p *TransformProcessor[I, O]) Process(ctx context.Context, item I) (*O, error) {
	result, err := p.Transform(item)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
