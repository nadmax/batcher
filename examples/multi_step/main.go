package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/nadmax/batcher/pkg/batcher/core"
	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/processor"
	fileReader "github.com/nadmax/batcher/pkg/batcher/reader/file"
	"github.com/nadmax/batcher/pkg/batcher/repository"
	dbWriter "github.com/nadmax/batcher/pkg/batcher/writer/database"
)

type ArchiveFileTasklet struct {
	SourcePath  string
	ArchivePath string
}

func (t *ArchiveFileTasklet) Execute(ctx context.Context) error {
	fmt.Printf("  📦 Archiving %s to %s\n", t.SourcePath, t.ArchivePath)

	data, err := os.ReadFile(t.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source: %w", err)
	}

	err = os.WriteFile(t.ArchivePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write archive: %w", err)
	}

	return nil
}

type DataRecord struct {
	ID    string
	Name  string
	Value int
}

type NotificationTasklet struct {
	Message string
	Success bool
}

func (t *NotificationTasklet) Execute(ctx context.Context) error {
	if t.Success {
		fmt.Printf("  📧 SUCCESS: %s\n", t.Message)
	} else {
		fmt.Printf("  ⚠️  FAILURE: %s\n", t.Message)
	}
	return nil
}

func main() {
	fmt.Println("=== Multi-Step Job ===")

	db, err := sql.Open("postgres",
		"postgres://batcher:batcher@localhost:5432/batcher?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS processed_data (
			id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value INTEGER NOT NULL
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	repo, err := repository.NewSQLExecutionRepository(db)
	if err != nil {
		log.Fatal("Failed to create repository:", err)
	}

	archiveStep := core.NewTaskletStep("archiveFile", &ArchiveFileTasklet{
		SourcePath:  "data/input.csv",
		ArchivePath: "archive/input-backup.csv",
	})
	csvReader, err := fileReader.NewCSVReader("data/input.csv", true)
	if err != nil {
		log.Fatal("Failed to create CSV reader:", err)
	}

	proc := &processor.TransformProcessor[map[string]string, DataRecord]{
		Transform: func(row map[string]string) (DataRecord, error) {
			value, err := strconv.Atoi(row["value"])
			if err != nil {
				return DataRecord{}, fmt.Errorf("invalid value: %w", err)
			}

			return DataRecord{
				ID:    row["id"],
				Name:  row["name"],
				Value: value,
			}, nil
		},
	}
	writer := dbWriter.NewSQLWriter[DataRecord](
		db,
		"INSERT INTO processed_data (id, name, value) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value",
		func(d DataRecord) []any {
			return []any{d.ID, d.Name, d.Value}
		},
	)
	processStep := core.NewChunkStep[map[string]string, DataRecord]("processData").
		Reader(csvReader).
		Processor(proc).
		Writer(writer).
		ChunkSize(100).
		Repository(repo).
		Listener(listener.NewLoggingStepListener()).
		Build()
	notifyStep := core.NewTaskletStep("sendNotification", &NotificationTasklet{
		Message: "Batch job completed successfully!",
		Success: true,
	})
	job := core.NewJob("multiStepJob").
		Step(archiveStep).
		Step(processStep).
		Step(notifyStep).
		Repository(repo).
		Listener(listener.NewLoggingJobListener()).
		Build()
	ctx := context.Background()
	execution, err := job.Run(ctx, nil)
	if err != nil {
		log.Fatal("Job failed:", err)
	}

	fmt.Println("\n✅ All steps completed!")
	fmt.Printf("Status: %s\n", execution.Status)
	fmt.Printf("Duration: %v\n", execution.EndTime.Sub(execution.StartTime))
}
