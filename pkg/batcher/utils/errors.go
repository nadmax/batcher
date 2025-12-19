package utils

import (
	"errors"
	"fmt"
)

type BatchError struct {
	Op        string
	StepName  string
	Err       error
	Retryable bool
}

func (e *BatchError) Error() string {
	if e.StepName != "" {
		return fmt.Sprintf("batch error in step '%s' during %s: %v", e.StepName, e.Op, e.Err)
	}
	return fmt.Sprintf("batch error during %s: %v", e.Op, e.Err)
}

func (e *BatchError) Unwrap() error {
	return e.Err
}

func (e *BatchError) IsRetryable() bool {
	return e.Retryable
}

func NewBatchError(op, stepName string, err error, retryable bool) *BatchError {
	return &BatchError{
		Op:        op,
		StepName:  stepName,
		Err:       err,
		Retryable: retryable,
	}
}

type ReaderError struct {
	Source    string
	RecordNum int64
	Err       error
}

func (e *ReaderError) Error() string {
	if e.RecordNum > 0 {
		return fmt.Sprintf("reader error at record %d in '%s': %v", e.RecordNum, e.Source, e.Err)
	}
	return fmt.Sprintf("reader error in '%s': %v", e.Source, e.Err)
}

func (e *ReaderError) Unwrap() error {
	return e.Err
}

func NewReaderError(source string, recordNum int64, err error) *ReaderError {
	return &ReaderError{
		Source:    source,
		RecordNum: recordNum,
		Err:       err,
	}
}

type WriterError struct {
	Destination string
	ChunkNum    int
	ItemCount   int
	Err         error
}

func (e *WriterError) Error() string {
	return fmt.Sprintf("writer error at chunk %d (%d items) in '%s': %v",
		e.ChunkNum, e.ItemCount, e.Destination, e.Err)
}

func (e *WriterError) Unwrap() error {
	return e.Err
}

func NewWriterError(destination string, chunkNum, itemCount int, err error) *WriterError {
	return &WriterError{
		Destination: destination,
		ChunkNum:    chunkNum,
		ItemCount:   itemCount,
		Err:         err,
	}
}

type ProcessorError struct {
	ProcessorName string
	Item          string
	Err           error
}

func (e *ProcessorError) Error() string {
	if e.Item != "" {
		return fmt.Sprintf("processor '%s' error on item '%s': %v", e.ProcessorName, e.Item, e.Err)
	}
	return fmt.Sprintf("processor '%s' error: %v", e.ProcessorName, e.Err)
}

func (e *ProcessorError) Unwrap() error {
	return e.Err
}

func NewProcessorError(processorName, item string, err error) *ProcessorError {
	return &ProcessorError{
		ProcessorName: processorName,
		Item:          item,
		Err:           err,
	}
}

type ValidationError struct {
	Field      string
	Value      any
	Constraint string
	Err        error
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error on field '%s' (value: %v): %s",
			e.Field, e.Value, e.Constraint)
	}
	return fmt.Sprintf("validation error: %s", e.Constraint)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

func NewValidationError(field string, value any, constraint string) *ValidationError {
	return &ValidationError{
		Field:      field,
		Value:      value,
		Constraint: constraint,
		Err:        errors.New(constraint),
	}
}

type RepositoryError struct {
	Operation  string
	EntityType string
	EntityID   int64
	Err        error
}

func (e *RepositoryError) Error() string {
	if e.EntityID > 0 {
		return fmt.Sprintf("repository error during %s of %s (ID: %d): %v",
			e.Operation, e.EntityType, e.EntityID, e.Err)
	}
	return fmt.Sprintf("repository error during %s of %s: %v",
		e.Operation, e.EntityType, e.Err)
}

func (e *RepositoryError) Unwrap() error {
	return e.Err
}

func NewRepositoryError(operation, entityType string, entityID int64, err error) *RepositoryError {
	return &RepositoryError{
		Operation:  operation,
		EntityType: entityType,
		EntityID:   entityID,
		Err:        err,
	}
}

type ConfigurationError struct {
	Component string
	Parameter string
	Reason    string
}

func (e *ConfigurationError) Error() string {
	return fmt.Sprintf("configuration error in %s: parameter '%s' is invalid - %s",
		e.Component, e.Parameter, e.Reason)
}

func NewConfigurationError(component, parameter, reason string) *ConfigurationError {
	return &ConfigurationError{
		Component: component,
		Parameter: parameter,
		Reason:    reason,
	}
}

var (
	ErrEOF              = errors.New("EOF")
	ErrNoData           = errors.New("no data available")
	ErrInvalidData      = errors.New("invalid data format")
	ErrConnectionFailed = errors.New("connection failed")
	ErrTimeout          = errors.New("operation timeout")
	ErrDuplicateKey     = errors.New("duplicate key")
	ErrNotFound         = errors.New("not found")
	ErrPermissionDenied = errors.New("permission denied")
	ErrInvalidState     = errors.New("invalid state")
	ErrCancelled        = errors.New("operation cancelled")
)

func IsRetryable(err error) bool {
	var batchErr *BatchError
	if errors.As(err, &batchErr) {
		return batchErr.IsRetryable()
	}

	return errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrConnectionFailed)
}

func IsSkippable(err error) bool {
	return errors.Is(err, ErrInvalidData) ||
		errors.Is(err, ErrDuplicateKey) ||
		errors.Is(err, ErrNotFound)
}

func IsFatal(err error) bool {
	return errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrInvalidState) ||
		errors.Is(err, ErrCancelled)
}

func WrapError(err error, op, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s in %s: %w", op, context, err)
}

func ErrorChain(err error) []error {
	if err == nil {
		return nil
	}

	var chain []error
	for err != nil {
		chain = append(chain, err)
		err = errors.Unwrap(err)
	}
	return chain
}

func RootCause(err error) error {
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			return err
		}
		err = unwrapped
	}
}
