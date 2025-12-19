package listener

import (
	"context"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type StepListener interface {
	BeforeStep(ctx context.Context, execution *models.StepExecution) error
	AfterStep(ctx context.Context, execution *models.StepExecution) error
}
