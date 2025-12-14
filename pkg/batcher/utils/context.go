package utils

import (
	"context"
	"time"
)

type contextKey string

const (
	jobExecutionIDKey  contextKey = "batcher:job_execution_id"
	stepExecutionIDKey contextKey = "batcher:step_execution_id"
	jobNameKey         contextKey = "batcher:job_name"
	stepNameKey        contextKey = "batcher:step_name"
	chunkNumberKey     contextKey = "batcher:chunk_number"
	recordNumberKey    contextKey = "batcher:record_number"
	parametersKey      contextKey = "batcher:parameters"
)

func WithJobExecutionID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, jobExecutionIDKey, id)
}

func WithStepExecutionID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, stepExecutionIDKey, id)
}

func WithJobName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, jobNameKey, name)
}

func WithStepName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, stepNameKey, name)
}

func WithChunkNumber(ctx context.Context, chunkNum int) context.Context {
	return context.WithValue(ctx, chunkNumberKey, chunkNum)
}

func WithRecordNumber(ctx context.Context, recordNum int64) context.Context {
	return context.WithValue(ctx, recordNumberKey, recordNum)
}

func WithParameters(ctx context.Context, params map[string]any) context.Context {
	return context.WithValue(ctx, parametersKey, params)
}

func JobExecutionIDFromContext(ctx context.Context) int64 {
	if id, ok := ctx.Value(jobExecutionIDKey).(int64); ok {
		return id
	}
	return 0
}

func StepExecutionIDFromContext(ctx context.Context) int64 {
	if id, ok := ctx.Value(stepExecutionIDKey).(int64); ok {
		return id
	}
	return 0
}

func JobNameFromContext(ctx context.Context) string {
	if name, ok := ctx.Value(jobNameKey).(string); ok {
		return name
	}
	return ""
}

func StepNameFromContext(ctx context.Context) string {
	if name, ok := ctx.Value(stepNameKey).(string); ok {
		return name
	}
	return ""
}

func ChunkNumberFromContext(ctx context.Context) int {
	if num, ok := ctx.Value(chunkNumberKey).(int); ok {
		return num
	}
	return 0
}

func RecordNumberFromContext(ctx context.Context) int64 {
	if num, ok := ctx.Value(recordNumberKey).(int64); ok {
		return num
	}
	return 0
}

func ParametersFromContext(ctx context.Context) map[string]any {
	if params, ok := ctx.Value(parametersKey).(map[string]any); ok {
		return params
	}
	return nil
}

func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

func WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, deadline)
}

func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(parent)
}

func IsContextCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func ContextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func WaitForContext(ctx context.Context, duration time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(duration):
		return false
	}
}

type BatchContext struct {
	ctx             context.Context
	JobExecutionID  int64
	StepExecutionID int64
	JobName         string
	StepName        string
	ChunkNumber     int
	RecordNumber    int64
	Parameters      map[string]any
}

func NewBatchContext(ctx context.Context) *BatchContext {
	return &BatchContext{
		ctx:             ctx,
		JobExecutionID:  JobExecutionIDFromContext(ctx),
		StepExecutionID: StepExecutionIDFromContext(ctx),
		JobName:         JobNameFromContext(ctx),
		StepName:        StepNameFromContext(ctx),
		ChunkNumber:     ChunkNumberFromContext(ctx),
		RecordNumber:    RecordNumberFromContext(ctx),
		Parameters:      ParametersFromContext(ctx),
	}
}

func (bc *BatchContext) Context() context.Context {
	return bc.ctx
}

func (bc *BatchContext) IsCancelled() bool {
	return IsContextCancelled(bc.ctx)
}

func (bc *BatchContext) Err() error {
	return ContextError(bc.ctx)
}

func (bc *BatchContext) GetParameter(key string) (any, bool) {
	if bc.Parameters == nil {
		return nil, false
	}
	val, ok := bc.Parameters[key]
	return val, ok
}

func (bc *BatchContext) GetStringParameter(key string) (string, bool) {
	val, ok := bc.GetParameter(key)
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

func (bc *BatchContext) GetIntParameter(key string) (int, bool) {
	val, ok := bc.GetParameter(key)
	if !ok {
		return 0, false
	}
	i, ok := val.(int)
	return i, ok
}

func (bc *BatchContext) GetBoolParameter(key string) (bool, bool) {
	val, ok := bc.GetParameter(key)
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}

func PropagateContext(parent context.Context) context.Context {
	ctx := context.Background()

	if jobExecID := JobExecutionIDFromContext(parent); jobExecID > 0 {
		ctx = WithJobExecutionID(ctx, jobExecID)
	}
	if stepExecID := StepExecutionIDFromContext(parent); stepExecID > 0 {
		ctx = WithStepExecutionID(ctx, stepExecID)
	}
	if jobName := JobNameFromContext(parent); jobName != "" {
		ctx = WithJobName(ctx, jobName)
	}
	if stepName := StepNameFromContext(parent); stepName != "" {
		ctx = WithStepName(ctx, stepName)
	}
	if params := ParametersFromContext(parent); params != nil {
		ctx = WithParameters(ctx, params)
	}

	return ctx
}

func MergeContext(dest, source context.Context) context.Context {
	if jobExecID := JobExecutionIDFromContext(source); jobExecID > 0 {
		dest = WithJobExecutionID(dest, jobExecID)
	}
	if stepExecID := StepExecutionIDFromContext(source); stepExecID > 0 {
		dest = WithStepExecutionID(dest, stepExecID)
	}
	if jobName := JobNameFromContext(source); jobName != "" {
		dest = WithJobName(dest, jobName)
	}
	if stepName := StepNameFromContext(source); stepName != "" {
		dest = WithStepName(dest, stepName)
	}
	if params := ParametersFromContext(source); params != nil {
		dest = WithParameters(dest, params)
	}

	return dest
}
