package listener

import (
	"context"
)

type SkipListener[I, O any] interface {
	OnSkipInRead(ctx context.Context, err error)
	OnSkipInProcess(ctx context.Context, item I, err error)
	OnSkipInWrite(ctx context.Context, items []O, err error)
}
