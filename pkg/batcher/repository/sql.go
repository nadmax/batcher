package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/nadmax/batcher/pkg/batcher/models"
)

type SQLExecutionRepository struct {
	db *sql.DB
}

func NewSQLExecutionRepository(db *sql.DB) (*SQLExecutionRepository, error) {
	repo := &SQLExecutionRepository{db: db}
	if err := repo.createTables(); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *SQLExecutionRepository) createTables() error {
	schema := `
	CREATE TABLE IF NOT EXISTS batch_job_execution (
		id BIGSERIAL PRIMARY KEY,
		job_name VARCHAR(255) NOT NULL,
		parameters TEXT,
		status VARCHAR(50) NOT NULL,
		start_time TIMESTAMP NOT NULL,
		end_time TIMESTAMP,
		exit_code VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS batch_step_execution (
		id BIGSERIAL PRIMARY KEY,
		job_execution_id BIGINT NOT NULL,
		step_name VARCHAR(255) NOT NULL,
		status VARCHAR(50) NOT NULL,
		start_time TIMESTAMP NOT NULL,
		end_time TIMESTAMP,
		read_count BIGINT DEFAULT 0,
		write_count BIGINT DEFAULT 0,
		skip_count BIGINT DEFAULT 0,
		commit_count BIGINT DEFAULT 0,
		rollback_count BIGINT DEFAULT 0,
		last_processed_id TEXT,
		FOREIGN KEY (job_execution_id) REFERENCES batch_job_execution(id)
	);

	CREATE INDEX IF NOT EXISTS idx_job_name ON batch_job_execution(job_name);
	CREATE INDEX IF NOT EXISTS idx_job_exec_step ON batch_step_execution(job_execution_id, step_name);
	`
	_, err := r.db.Exec(schema)
	return err
}

func (r *SQLExecutionRepository) SaveJobExecution(ctx context.Context, execution *models.JobExecution) error {
	params, _ := json.Marshal(execution.Parameters)
	return r.db.QueryRowContext(ctx, `
		INSERT INTO batch_job_execution (job_name, parameters, status, start_time, exit_code)
		VALUES ($1, $2, $3, $4, $5) RETURNING id
	`, execution.JobName, string(params), execution.Status, execution.StartTime, execution.ExitCode).Scan(&execution.ID)
}

func (r *SQLExecutionRepository) UpdateJobExecution(ctx context.Context, execution *models.JobExecution) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE batch_job_execution 
		SET status = $1, end_time = $2, exit_code = $3
		WHERE id = $4
	`, execution.Status, execution.EndTime, execution.ExitCode, execution.ID)
	return err
}

func (r *SQLExecutionRepository) GetLastJobExecution(ctx context.Context, jobName string) (*models.JobExecution, error) {
	execution := &models.JobExecution{}
	var params string
	var endTime sql.NullTime

	err := r.db.QueryRowContext(ctx, `
		SELECT id, job_name, parameters, status, start_time, end_time, exit_code
		FROM batch_job_execution
		WHERE job_name = $1
		ORDER BY id DESC LIMIT 1
	`, jobName).Scan(&execution.ID, &execution.JobName, &params, &execution.Status,
		&execution.StartTime, &endTime, &execution.ExitCode)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if endTime.Valid {
		execution.EndTime = &endTime.Time
	}
	json.Unmarshal([]byte(params), &execution.Parameters)
	return execution, nil
}

func (r *SQLExecutionRepository) SaveStepExecution(ctx context.Context, execution *models.StepExecution) error {
	var lastProcessedID *string
	if execution.LastProcessedID != nil {
		s := fmt.Sprintf("%v", execution.LastProcessedID)
		lastProcessedID = &s
	}

	return r.db.QueryRowContext(ctx, `
		INSERT INTO batch_step_execution 
		(job_execution_id, step_name, status, start_time, read_count, write_count, 
		 skip_count, commit_count, rollback_count, last_processed_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id
	`, execution.JobExecutionID, execution.StepName, execution.Status, execution.StartTime,
		execution.ReadCount, execution.WriteCount, execution.SkipCount, execution.CommitCount,
		execution.RollbackCount, lastProcessedID).Scan(&execution.ID)
}

func (r *SQLExecutionRepository) UpdateStepExecution(ctx context.Context, execution *models.StepExecution) error {
	var lastProcessedID *string
	if execution.LastProcessedID != nil {
		s := fmt.Sprintf("%v", execution.LastProcessedID)
		lastProcessedID = &s
	}

	_, err := r.db.ExecContext(ctx, `
		UPDATE batch_step_execution 
		SET status = $1, end_time = $2, read_count = $3, write_count = $4,
		    skip_count = $5, commit_count = $6, rollback_count = $7, last_processed_id = $8
		WHERE id = $9
	`, execution.Status, execution.EndTime, execution.ReadCount, execution.WriteCount,
		execution.SkipCount, execution.CommitCount, execution.RollbackCount,
		lastProcessedID, execution.ID)
	return err
}

func (r *SQLExecutionRepository) GetLastStepExecution(ctx context.Context, jobExecutionID int64, stepName string) (*models.StepExecution, error) {
	execution := &models.StepExecution{}
	var endTime sql.NullTime
	var lastProcessedID sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, job_execution_id, step_name, status, start_time, end_time,
		       read_count, write_count, skip_count, commit_count, rollback_count, last_processed_id
		FROM batch_step_execution
		WHERE job_execution_id = $1 AND step_name = $2
		ORDER BY id DESC LIMIT 1
	`, jobExecutionID, stepName).Scan(&execution.ID, &execution.JobExecutionID, &execution.StepName,
		&execution.Status, &execution.StartTime, &endTime, &execution.ReadCount, &execution.WriteCount,
		&execution.SkipCount, &execution.CommitCount, &execution.RollbackCount, &lastProcessedID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if endTime.Valid {
		execution.EndTime = &endTime.Time
	}
	if lastProcessedID.Valid {
		execution.LastProcessedID = lastProcessedID.String
	}
	return execution, nil
}
