package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"github.com/nadmax/batcher/pkg/batcher/core"
	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/processor"
	fileReader "github.com/nadmax/batcher/pkg/batcher/reader/file"
	"github.com/nadmax/batcher/pkg/batcher/repository"
	dbWriter "github.com/nadmax/batcher/pkg/batcher/writer/database"
)

type Product struct {
	ID          string
	Name        string
	Description string
	Price       float64
	Stock       int
}

func main() {
	fmt.Println("=== CSV to Database ===")

	db, err := sql.Open("postgres",
		"postgres://batcher:batcher@localhost:5432/batcher?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS products (
			id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			price DECIMAL(10,2) NOT NULL,
			stock INTEGER NOT NULL
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	repo, err := repository.NewSQLExecutionRepository(db)
	if err != nil {
		log.Fatal("Failed to create repository:", err)
	}

	reader, err := fileReader.NewCSVReader("data/products.csv", true)
	if err != nil {
		log.Fatal("Failed to create reader:", err)
	}

	proc := &processor.TransformProcessor[map[string]string, Product]{
		Transform: func(row map[string]string) (Product, error) {
			price, err := strconv.ParseFloat(row["price"], 64)
			if err != nil {
				return Product{}, fmt.Errorf("invalid price: %w", err)
			}

			stock, err := strconv.Atoi(row["stock"])
			if err != nil {
				return Product{}, fmt.Errorf("invalid stock: %w", err)
			}

			return Product{
				ID:          row["id"],
				Name:        row["name"],
				Description: row["description"],
				Price:       price,
				Stock:       stock,
			}, nil
		},
	}
	writer := dbWriter.NewSQLWriter[Product](
		db,
		"INSERT INTO products (id, name, description, price, stock) VALUES ($1, $2, $3, $4, $5)",
		func(p Product) []any {
			return []any{p.ID, p.Name, p.Description, p.Price, p.Stock}
		},
	)
	step := core.NewChunkStep[map[string]string, Product]("importProducts").
		Reader(reader).
		Processor(proc).
		Writer(writer).
		ChunkSize(100).
		SkipLimit(10).
		Repository(repo).
		Listener(listener.NewLoggingStepListener()).
		Build()
	job := core.NewJob("productImportJob").
		Step(step).
		Repository(repo).
		Listener(listener.NewLoggingJobListener()).
		Build()
	ctx := context.Background()
	execution, err := job.Run(ctx, map[string]any{
		"inputFile": "data/products.csv",
	})
	if err != nil {
		log.Fatal("Job failed:", err)
	}

	fmt.Println("\n✅ Job completed successfully!")
	fmt.Printf("Status: %s\n", execution.Status)
	fmt.Printf("Duration: %v\n", execution.EndTime.Sub(execution.StartTime))
}
