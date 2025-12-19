package models

import (
	"sync"
	"time"
)

type ExecutionStatus string

const (
	StatusStarted   ExecutionStatus = "STARTED"
	StatusCompleted ExecutionStatus = "COMPLETED"
	StatusFailed    ExecutionStatus = "FAILED"
	StatusStopped   ExecutionStatus = "STOPPED"
	StatusAbandoned ExecutionStatus = "ABANDONED"
)

type JobExecution struct {
	ID          int64
	JobName     string
	Parameters  map[string]any
	Status      ExecutionStatus
	StartTime   time.Time
	EndTime     *time.Time
	ExitCode    string
	ExitMessage string
	Version     int
	mu          sync.RWMutex
}

func (je *JobExecution) SetStatus(status ExecutionStatus) {
	je.mu.Lock()
	defer je.mu.Unlock()
	je.Status = status
}

func (je *JobExecution) GetStatus() ExecutionStatus {
	je.mu.RLock()
	defer je.mu.RUnlock()
	return je.Status
}

type StepExecution struct {
	ID              int64
	JobExecutionID  int64
	StepName        string
	Status          ExecutionStatus
	StartTime       time.Time
	EndTime         *time.Time
	ReadCount       int64
	FilterCount     int64
	WriteCount      int64
	SkipCount       int64
	CommitCount     int64
	RollbackCount   int64
	LastProcessedID any
	ExitCode        string
	ExitMessage     string
	Version         int
	mu              sync.RWMutex
}

func (se *StepExecution) IncrementReadCount() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.ReadCount++
}

func (se *StepExecution) IncrementFilterCount() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.FilterCount++
}

func (se *StepExecution) IncrementWriteCount(count int) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.WriteCount += int64(count)
}

func (se *StepExecution) IncrementSkipCount() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.SkipCount++
}

func (se *StepExecution) IncrementCommitCount() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.CommitCount++
}

func (se *StepExecution) IncrementRollbackCount() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.RollbackCount++
}

func (se *StepExecution) SetLastProcessedID(id any) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.LastProcessedID = id
}

func (se *StepExecution) GetLastProcessedID() any {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.LastProcessedID
}

func (se *StepExecution) SetStatus(status ExecutionStatus) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.Status = status
}

func (se *StepExecution) GetStatus() ExecutionStatus {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.Status
}

func (se *StepExecution) GetMetrics() StepMetrics {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return StepMetrics{
		ReadCount:     se.ReadCount,
		FilterCount:   se.FilterCount,
		WriteCount:    se.WriteCount,
		SkipCount:     se.SkipCount,
		CommitCount:   se.CommitCount,
		RollbackCount: se.RollbackCount,
	}
}

type StepMetrics struct {
	ReadCount     int64
	FilterCount   int64
	WriteCount    int64
	SkipCount     int64
	CommitCount   int64
	RollbackCount int64
}

func (se *StepExecution) Duration() time.Duration {
	if se.EndTime == nil {
		return time.Since(se.StartTime)
	}
	return se.EndTime.Sub(se.StartTime)
}

func (se *StepExecution) IsRunning() bool {
	status := se.GetStatus()
	return status == StatusStarted
}

func (se *StepExecution) IsComplete() bool {
	status := se.GetStatus()
	return status == StatusCompleted || status == StatusFailed || status == StatusStopped || status == StatusAbandoned
}
