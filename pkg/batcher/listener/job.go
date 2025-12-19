package listener

import (
	"context"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type JobListener interface {
	BeforeJob(ctx context.Context, execution *models.JobExecution) error
	AfterJob(ctx context.Context, execution *models.JobExecution) error
}
