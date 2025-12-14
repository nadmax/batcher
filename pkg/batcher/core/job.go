package core

import (
	"context"
	"fmt"
	"time"

	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/models"
	"github.com/nadmax/batcher/pkg/batcher/repository"
)

type Job struct {
	name        string
	steps       []Step
	listeners   []listener.JobListener
	validator   ParameterValidator
	repository  repository.ExecutionRepository
	restartable bool
}

type JobBuilder struct {
	job *Job
}

func NewJob(name string) *JobBuilder {
	return &JobBuilder{
		job: &Job{
			name:        name,
			restartable: false,
		},
	}
}

func (b *JobBuilder) Step(step Step) *JobBuilder {
	b.job.steps = append(b.job.steps, step)
	return b
}

func (b *JobBuilder) Listener(listener listener.JobListener) *JobBuilder {
	b.job.listeners = append(b.job.listeners, listener)
	return b
}

func (b *JobBuilder) Validator(validator ParameterValidator) *JobBuilder {
	b.job.validator = validator
	return b
}

func (b *JobBuilder) Repository(repo repository.ExecutionRepository) *JobBuilder {
	b.job.repository = repo
	return b
}

func (b *JobBuilder) Restartable(restartable bool) *JobBuilder {
	b.job.restartable = restartable
	return b
}

func (b *JobBuilder) Build() *Job {
	return b.job
}

func (j *Job) Run(ctx context.Context, params map[string]any) (*models.JobExecution, error) {
	if j.validator != nil {
		if err := j.validator.Validate(params); err != nil {
			return nil, fmt.Errorf("parameter validation failed: %w", err)
		}
	}

	execution := &models.JobExecution{
		JobName:    j.name,
		Parameters: params,
		Status:     models.StatusStarted,
		StartTime:  time.Now(),
	}

	if j.repository != nil {
		if err := j.repository.SaveJobExecution(ctx, execution); err != nil {
			return nil, err
		}
	}

	for _, listener := range j.listeners {
		if err := listener.BeforeJob(ctx, execution); err != nil {
			return execution, err
		}
	}

	defer func() {
		for _, listener := range j.listeners {
			listener.AfterJob(ctx, execution)
		}
	}()

	for _, step := range j.steps {
		stepExecution, err := step.Execute(ctx, execution)
		if err != nil {
			execution.Status = models.StatusFailed
			execution.ExitCode = err.Error()
			now := time.Now()
			execution.EndTime = &now
			if j.repository != nil {
				j.repository.UpdateJobExecution(ctx, execution)
			}
			return execution, err
		}

		if stepExecution.Status == models.StatusFailed {
			execution.Status = models.StatusFailed
			execution.ExitCode = "Step failed"
			now := time.Now()
			execution.EndTime = &now
			if j.repository != nil {
				j.repository.UpdateJobExecution(ctx, execution)
			}
			return execution, fmt.Errorf("step %s failed", step.Name())
		}
	}

	execution.Status = models.StatusCompleted
	execution.ExitCode = "COMPLETED"
	now := time.Now()
	execution.EndTime = &now

	if j.repository != nil {
		j.repository.UpdateJobExecution(ctx, execution)
	}

	return execution, nil
}
