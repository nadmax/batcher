package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/nadmax/batcher/pkg/batcher/core"
	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/processor"
	dbReader "github.com/nadmax/batcher/pkg/batcher/reader/database"
	"github.com/nadmax/batcher/pkg/batcher/repository"
	dbWriter "github.com/nadmax/batcher/pkg/batcher/writer/database"
)

type User struct {
	ID        int
	Email     string
	FirstName string
	LastName  string
	Active    bool
}

func main() {
	fmt.Println("=== Database Migration ===")

	sourceDB, err := sql.Open("postgres",
		"postgres://user:pass@source-host:5432/sourcedb?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer sourceDB.Close()

	targetDB, err := sql.Open("postgres",
		"postgres://batcher:batcher@localhost:5432/batcher?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to target database:", err)
	}
	defer targetDB.Close()

	_, err = targetDB.Exec(`
		CREATE TABLE IF NOT EXISTS users_migrated (
			id INTEGER PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			first_name VARCHAR(100),
			last_name VARCHAR(100),
			active BOOLEAN DEFAULT true
		)
	`)
	if err != nil {
		log.Fatal("Failed to create target table:", err)
	}

	repo, err := repository.NewSQLExecutionRepository(targetDB)
	if err != nil {
		log.Fatal("Failed to create repository:", err)
	}

	reader := dbReader.NewSQLReader[User](
		sourceDB,
		"SELECT id, email, first_name, last_name, active FROM users WHERE active = true",
		func(rows *sql.Rows) (*User, error) {
			var u User
			err := rows.Scan(&u.ID, &u.Email, &u.FirstName, &u.LastName, &u.Active)
			if err != nil {
				return nil, err
			}
			return &u, nil
		},
	)
	proc := &processor.TransformProcessor[User, User]{
		Transform: func(u User) (User, error) {
			u.Email = strings.ToLower(u.Email)
			return u, nil
		},
	}
	writer := dbWriter.NewSQLWriter[User](
		targetDB,
		`INSERT INTO users_migrated (id, email, first_name, last_name, active) 
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET
		 email = EXCLUDED.email,
		 first_name = EXCLUDED.first_name,
		 last_name = EXCLUDED.last_name,
		 active = EXCLUDED.active`,
		func(u User) []any {
			return []any{u.ID, u.Email, u.FirstName, u.LastName, u.Active}
		},
	)
	step := core.NewChunkStep[User, User]("migrateUsers").
		Reader(reader).
		Processor(proc).
		Writer(writer).
		ChunkSize(1000).
		Repository(repo).
		Listener(listener.NewVerboseLoggingStepListener()).
		Build()
	job := core.NewJob("userMigrationJob").
		Step(step).
		Repository(repo).
		Listener(listener.NewVerboseLoggingJobListener()).
		Build()
	ctx := context.Background()
	execution, err := job.Run(ctx, nil)
	if err != nil {
		log.Fatal("Job failed:", err)
	}

	fmt.Println("\n✅ Migration completed successfully!")
	fmt.Printf("Status: %s\n", execution.Status)
	fmt.Printf("Duration: %v\n", execution.EndTime.Sub(execution.StartTime))
}
