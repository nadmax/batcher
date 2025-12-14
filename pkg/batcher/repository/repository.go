package repository

import (
	"context"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type ExecutionRepository interface {
	SaveJobExecution(ctx context.Context, execution *models.JobExecution) error
	UpdateJobExecution(ctx context.Context, execution *models.JobExecution) error
	GetLastJobExecution(ctx context.Context, jobName string) (*models.JobExecution, error)

	SaveStepExecution(ctx context.Context, execution *models.StepExecution) error
	UpdateStepExecution(ctx context.Context, execution *models.StepExecution) error
	GetLastStepExecution(ctx context.Context, jobExecutionID int64, stepName string) (*models.StepExecution, error)
}
