package listener

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type LoggingJobListener struct {
	Logger  *log.Logger
	Verbose bool
}

func NewLoggingJobListener() *LoggingJobListener {
	return &LoggingJobListener{
		Logger:  log.Default(),
		Verbose: false,
	}
}

func NewVerboseLoggingJobListener() *LoggingJobListener {
	return &LoggingJobListener{
		Logger:  log.Default(),
		Verbose: true,
	}
}

func (l *LoggingJobListener) BeforeJob(ctx context.Context, execution *models.JobExecution) error {
	logger := l.getLogger()

	if l.Verbose {
		logger.Printf("[JOB STARTED] %s (ID: %d) at %s",
			execution.JobName,
			execution.ID,
			execution.StartTime.Format(time.RFC3339))

		if len(execution.Parameters) > 0 {
			logger.Printf("  Parameters: %v", execution.Parameters)
		}
	} else {
		logger.Printf("[JOB STARTED] %s", execution.JobName)
	}

	return nil
}

func (l *LoggingJobListener) AfterJob(ctx context.Context, execution *models.JobExecution) error {
	logger := l.getLogger()

	duration := time.Since(execution.StartTime)
	status := execution.GetStatus()

	if l.Verbose {
		logger.Printf("[JOB COMPLETED] %s (ID: %d) - Status: %s, Duration: %s, Exit Code: %s",
			execution.JobName,
			execution.ID,
			status,
			duration.Round(time.Millisecond),
			execution.ExitCode)

		if execution.ExitMessage != "" {
			logger.Printf("  Message: %s", execution.ExitMessage)
		}
	} else {
		logger.Printf("[JOB COMPLETED] %s - Status: %s, Duration: %s",
			execution.JobName,
			status,
			duration.Round(time.Millisecond))
	}

	return nil
}

func (l *LoggingJobListener) getLogger() *log.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return log.Default()
}

type LoggingStepListener struct {
	Logger  *log.Logger
	Verbose bool
}

func NewLoggingStepListener() *LoggingStepListener {
	return &LoggingStepListener{
		Logger:  log.Default(),
		Verbose: false,
	}
}

func NewVerboseLoggingStepListener() *LoggingStepListener {
	return &LoggingStepListener{
		Logger:  log.Default(),
		Verbose: true,
	}
}

func (l *LoggingStepListener) BeforeStep(ctx context.Context, execution *models.StepExecution) error {
	logger := l.getLogger()

	if l.Verbose {
		logger.Printf("  [STEP STARTED] %s (ID: %d, Job: %d) at %s",
			execution.StepName,
			execution.ID,
			execution.JobExecutionID,
			execution.StartTime.Format(time.RFC3339))
	} else {
		logger.Printf("  [STEP STARTED] %s", execution.StepName)
	}

	return nil
}

func (l *LoggingStepListener) AfterStep(ctx context.Context, execution *models.StepExecution) error {
	logger := l.getLogger()

	duration := execution.Duration()
	metrics := execution.GetMetrics()
	status := execution.GetStatus()

	if l.Verbose {
		logger.Printf("  [STEP COMPLETED] %s (ID: %d)", execution.StepName, execution.ID)
		logger.Printf("    Status:      %s", status)
		logger.Printf("    Duration:    %s", duration.Round(time.Millisecond))
		logger.Printf("    Read:        %d items", metrics.ReadCount)
		logger.Printf("    Filtered:    %d items", metrics.FilterCount)
		logger.Printf("    Written:     %d items", metrics.WriteCount)
		logger.Printf("    Skipped:     %d items", metrics.SkipCount)
		logger.Printf("    Commits:     %d", metrics.CommitCount)
		logger.Printf("    Rollbacks:   %d", metrics.RollbackCount)

		if execution.ExitMessage != "" {
			logger.Printf("    Message:     %s", execution.ExitMessage)
		}
	} else {
		logger.Printf("  [STEP COMPLETED] %s - Read: %d, Written: %d, Skipped: %d (Duration: %s)",
			execution.StepName,
			metrics.ReadCount,
			metrics.WriteCount,
			metrics.SkipCount,
			duration.Round(time.Millisecond))
	}

	return nil
}

func (l *LoggingStepListener) getLogger() *log.Logger {
	if l.Logger != nil {
		return l.Logger
	}

	return log.Default()
}

type LoggingSkipListener[I, O any] struct {
	Logger        *log.Logger
	LogStackTrace bool
}

func NewLoggingSkipListener[I, O any]() *LoggingSkipListener[I, O] {
	return &LoggingSkipListener[I, O]{
		Logger:        log.Default(),
		LogStackTrace: false,
	}
}

func (l *LoggingSkipListener[I, O]) OnSkipInRead(ctx context.Context, err error) {
	logger := l.getLogger()
	logger.Printf("    [SKIP IN READ] Error: %v", err)
}

func (l *LoggingSkipListener[I, O]) OnSkipInProcess(ctx context.Context, item I, err error) {
	logger := l.getLogger()
	if l.LogStackTrace {
		logger.Printf("    [SKIP IN PROCESS] Item: %+v, Error: %v", item, err)
	} else {
		logger.Printf("    [SKIP IN PROCESS] Error: %v", err)
	}
}

func (l *LoggingSkipListener[I, O]) OnSkipInWrite(ctx context.Context, items []O, err error) {
	logger := l.getLogger()
	logger.Printf("    [SKIP IN WRITE] %d items skipped, Error: %v", len(items), err)
}

func (l *LoggingSkipListener[I, O]) getLogger() *log.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return log.Default()
}

type MetricsJobListener struct {
	OnJobStart    func(jobName string, startTime time.Time)
	OnJobComplete func(jobName string, status models.ExecutionStatus, duration time.Duration)
}

func (m *MetricsJobListener) BeforeJob(ctx context.Context, execution *models.JobExecution) error {
	if m.OnJobStart != nil {
		m.OnJobStart(execution.JobName, execution.StartTime)
	}
	return nil
}

func (m *MetricsJobListener) AfterJob(ctx context.Context, execution *models.JobExecution) error {
	if m.OnJobComplete != nil {
		duration := time.Since(execution.StartTime)
		m.OnJobComplete(execution.JobName, execution.GetStatus(), duration)
	}
	return nil
}

type MetricsStepListener struct {
	OnStepComplete func(stepName string, metrics models.StepMetrics, duration time.Duration)
}

func (m *MetricsStepListener) BeforeStep(ctx context.Context, execution *models.StepExecution) error {
	return nil
}

func (m *MetricsStepListener) AfterStep(ctx context.Context, execution *models.StepExecution) error {
	if m.OnStepComplete != nil {
		metrics := execution.GetMetrics()
		duration := execution.Duration()
		m.OnStepComplete(execution.StepName, metrics, duration)
	}
	return nil
}

type CompositeJobListener struct {
	Listeners []JobListener
}

func NewCompositeJobListener(listeners ...JobListener) *CompositeJobListener {
	return &CompositeJobListener{
		Listeners: listeners,
	}
}

func (c *CompositeJobListener) BeforeJob(ctx context.Context, execution *models.JobExecution) error {
	for _, listener := range c.Listeners {
		if err := listener.BeforeJob(ctx, execution); err != nil {
			return fmt.Errorf("composite listener error in BeforeJob: %w", err)
		}
	}
	return nil
}

func (c *CompositeJobListener) AfterJob(ctx context.Context, execution *models.JobExecution) error {
	for _, listener := range c.Listeners {
		if err := listener.AfterJob(ctx, execution); err != nil {
			return fmt.Errorf("composite listener error in AfterJob: %w", err)
		}
	}
	return nil
}

type CompositeStepListener struct {
	Listeners []StepListener
}

func NewCompositeStepListener(listeners ...StepListener) *CompositeStepListener {
	return &CompositeStepListener{
		Listeners: listeners,
	}
}

func (c *CompositeStepListener) BeforeStep(ctx context.Context, execution *models.StepExecution) error {
	for _, listener := range c.Listeners {
		if err := listener.BeforeStep(ctx, execution); err != nil {
			return fmt.Errorf("composite listener error in BeforeStep: %w", err)
		}
	}
	return nil
}

func (c *CompositeStepListener) AfterStep(ctx context.Context, execution *models.StepExecution) error {
	for _, listener := range c.Listeners {
		if err := listener.AfterStep(ctx, execution); err != nil {
			return fmt.Errorf("composite listener error in AfterStep: %w", err)
		}
	}
	return nil
}
