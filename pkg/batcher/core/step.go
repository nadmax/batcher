package core

import (
	"context"
	"time"

	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/models"
	"github.com/nadmax/batcher/pkg/batcher/repository"
)

type ChunkStep[I, O any] struct {
	name          string
	reader        ItemReader[I]
	processor     ItemProcessor[I, O]
	writer        ItemWriter[O]
	chunkSize     int
	skipLimit     int
	skipPolicy    SkipPolicy
	stepListeners []listener.StepListener
	skipListeners []listener.SkipListener[I, O]
	repository    repository.ExecutionRepository
	restartable   bool
}

type ChunkStepBuilder[I, O any] struct {
	step *ChunkStep[I, O]
}

func NewChunkStep[I, O any](name string) *ChunkStepBuilder[I, O] {
	return &ChunkStepBuilder[I, O]{
		step: &ChunkStep[I, O]{
			name:      name,
			chunkSize: 100,
			skipLimit: 0,
		},
	}
}

func (b *ChunkStepBuilder[I, O]) Reader(reader ItemReader[I]) *ChunkStepBuilder[I, O] {
	b.step.reader = reader
	return b
}

func (b *ChunkStepBuilder[I, O]) Processor(processor ItemProcessor[I, O]) *ChunkStepBuilder[I, O] {
	b.step.processor = processor
	return b
}

func (b *ChunkStepBuilder[I, O]) Writer(writer ItemWriter[O]) *ChunkStepBuilder[I, O] {
	b.step.writer = writer
	return b
}

func (b *ChunkStepBuilder[I, O]) ChunkSize(size int) *ChunkStepBuilder[I, O] {
	b.step.chunkSize = size
	return b
}

func (b *ChunkStepBuilder[I, O]) SkipLimit(limit int) *ChunkStepBuilder[I, O] {
	b.step.skipLimit = limit
	return b
}

func (b *ChunkStepBuilder[I, O]) SkipPolicy(policy SkipPolicy) *ChunkStepBuilder[I, O] {
	b.step.skipPolicy = policy
	return b
}

func (b *ChunkStepBuilder[I, O]) Listener(listener listener.StepListener) *ChunkStepBuilder[I, O] {
	b.step.stepListeners = append(b.step.stepListeners, listener)
	return b
}

func (b *ChunkStepBuilder[I, O]) SkipListener(listener listener.SkipListener[I, O]) *ChunkStepBuilder[I, O] {
	b.step.skipListeners = append(b.step.skipListeners, listener)
	return b
}

func (b *ChunkStepBuilder[I, O]) Repository(repo repository.ExecutionRepository) *ChunkStepBuilder[I, O] {
	b.step.repository = repo
	return b
}

func (b *ChunkStepBuilder[I, O]) Restartable(restartable bool) *ChunkStepBuilder[I, O] {
	b.step.restartable = restartable
	return b
}

func (b *ChunkStepBuilder[I, O]) Build() *ChunkStep[I, O] {
	return b.step
}

func (s *ChunkStep[I, O]) Name() string {
	return s.name
}

func (s *ChunkStep[I, O]) Execute(ctx context.Context, jobExecution *models.JobExecution) (*models.StepExecution, error) {
	execution := &models.StepExecution{
		JobExecutionID: jobExecution.ID,
		StepName:       s.name,
		Status:         models.StatusStarted,
		StartTime:      time.Now(),
	}

	if s.repository != nil {
		if err := s.repository.SaveStepExecution(ctx, execution); err != nil {
			return nil, err
		}
	}

	for _, listener := range s.stepListeners {
		if err := listener.BeforeStep(ctx, execution); err != nil {
			return execution, err
		}
	}

	defer func() {
		for _, listener := range s.stepListeners {
			listener.AfterStep(ctx, execution)
		}
		s.reader.Close()
		s.writer.Close()
	}()

	var chunk []O
	skipCount := 0

	for {
		select {
		case <-ctx.Done():
			execution.Status = models.StatusStopped
			now := time.Now()
			execution.EndTime = &now
			if s.repository != nil {
				s.repository.UpdateStepExecution(ctx, execution)
			}
			return execution, ctx.Err()
		default:
		}

		item, err := s.reader.Read(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			skipCount++
			for _, listener := range s.skipListeners {
				listener.OnSkipInRead(ctx, err)
			}
			if s.skipPolicy != nil && !s.skipPolicy.ShouldSkip(err, skipCount) {
				execution.Status = models.StatusFailed
				now := time.Now()
				execution.EndTime = &now
				if s.repository != nil {
					s.repository.UpdateStepExecution(ctx, execution)
				}
				return execution, err
			}
			execution.IncrementSkipCount()
			continue
		}

		if item == nil {
			break
		}

		execution.IncrementReadCount()

		var processed *O
		if s.processor != nil {
			processed, err = s.processor.Process(ctx, *item)
			if err != nil {
				skipCount++
				for _, listener := range s.skipListeners {
					listener.OnSkipInProcess(ctx, *item, err)
				}
				if s.skipPolicy != nil && !s.skipPolicy.ShouldSkip(err, skipCount) {
					execution.Status = models.StatusFailed
					now := time.Now()
					execution.EndTime = &now
					if s.repository != nil {
						s.repository.UpdateStepExecution(ctx, execution)
					}
					return execution, err
				}
				execution.IncrementSkipCount()
				continue
			}
		} else {
			processed = (*O)(any(item).(*O))
		}

		if processed != nil {
			chunk = append(chunk, *processed)
		}

		if len(chunk) >= s.chunkSize {
			if err := s.writeChunk(ctx, execution, chunk, &skipCount); err != nil {
				return execution, err
			}
			chunk = chunk[:0]
		}
	}

	if len(chunk) > 0 {
		if err := s.writeChunk(ctx, execution, chunk, &skipCount); err != nil {
			return execution, err
		}
	}

	execution.Status = models.StatusCompleted
	now := time.Now()
	execution.EndTime = &now

	if s.repository != nil {
		s.repository.UpdateStepExecution(ctx, execution)
	}

	return execution, nil
}

func (s *ChunkStep[I, O]) writeChunk(ctx context.Context, execution *models.StepExecution, chunk []O, skipCount *int) error {
	if err := s.writer.Write(ctx, chunk); err != nil {
		*skipCount++
		for _, listener := range s.skipListeners {
			listener.OnSkipInWrite(ctx, chunk, err)
		}
		if s.skipPolicy != nil && !s.skipPolicy.ShouldSkip(err, *skipCount) {
			execution.Status = models.StatusFailed
			now := time.Now()
			execution.EndTime = &now
			if s.repository != nil {
				s.repository.UpdateStepExecution(ctx, execution)
			}
			return err
		}
		execution.IncrementSkipCount()
		return nil
	}
	execution.IncrementWriteCount(len(chunk))
	execution.CommitCount++
	if s.repository != nil {
		s.repository.UpdateStepExecution(ctx, execution)
	}
	return nil
}

type TaskletStep struct {
	name          string
	tasklet       Tasklet
	stepListeners []listener.StepListener
	repository    repository.ExecutionRepository
}

func NewTaskletStep(name string, tasklet Tasklet) *TaskletStep {
	return &TaskletStep{
		name:    name,
		tasklet: tasklet,
	}
}

func (s *TaskletStep) Listener(listener listener.StepListener) *TaskletStep {
	s.stepListeners = append(s.stepListeners, listener)
	return s
}

func (s *TaskletStep) Repository(repo repository.ExecutionRepository) *TaskletStep {
	s.repository = repo
	return s
}

func (s *TaskletStep) Name() string {
	return s.name
}

func (s *TaskletStep) Execute(ctx context.Context, jobExecution *models.JobExecution) (*models.StepExecution, error) {
	execution := &models.StepExecution{
		JobExecutionID: jobExecution.ID,
		StepName:       s.name,
		Status:         models.StatusStarted,
		StartTime:      time.Now(),
	}

	if s.repository != nil {
		if err := s.repository.SaveStepExecution(ctx, execution); err != nil {
			return nil, err
		}
	}

	for _, listener := range s.stepListeners {
		if err := listener.BeforeStep(ctx, execution); err != nil {
			return execution, err
		}
	}

	err := s.tasklet.Execute(ctx)

	if err != nil {
		execution.Status = models.StatusFailed
	} else {
		execution.Status = models.StatusCompleted
	}

	now := time.Now()
	execution.EndTime = &now

	for _, listener := range s.stepListeners {
		listener.AfterStep(ctx, execution)
	}

	if s.repository != nil {
		s.repository.UpdateStepExecution(ctx, execution)
	}

	return execution, err
}
