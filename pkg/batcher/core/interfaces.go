package core

import (
	"context"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type ItemReader[T any] interface {
	Read(ctx context.Context) (*T, error)
	Close() error
}

type ItemProcessor[I, O any] interface {
	Process(ctx context.Context, item I) (*O, error)
}

type ItemWriter[T any] interface {
	Write(ctx context.Context, items []T) error
	Close() error
}

type Tasklet interface {
	Execute(ctx context.Context) error
}

type Step interface {
	Name() string
	Execute(ctx context.Context, jobExecution *models.JobExecution) (*models.StepExecution, error)
}

type SkipPolicy interface {
	ShouldSkip(err error, skipCount int) bool
}

type ParameterValidator interface {
	Validate(params map[string]any) error
}
