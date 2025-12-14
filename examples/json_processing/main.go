package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nadmax/batcher/pkg/batcher/core"
	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/processor"
	fileReader "github.com/nadmax/batcher/pkg/batcher/reader/file"
	fileWriter "github.com/nadmax/batcher/pkg/batcher/writer/file"
)

type Order struct {
	ID         string  `json:"id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
}

type ProcessedOrder struct {
	ID         string  `json:"id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Fee        float64 `json:"fee"`
	Total      float64 `json:"total"`
	CreatedAt  string  `json:"created_at"`
}

func main() {
	fmt.Println("=== JSON Processing ===")

	reader, err := fileReader.NewJSONReader[Order]("data/orders.json")
	if err != nil {
		log.Fatal("Failed to create reader:", err)
	}

	proc := &processor.TransformProcessor[Order, ProcessedOrder]{
		Transform: func(order Order) (ProcessedOrder, error) {
			fee := order.Amount * 0.03 // 3% processing fee
			return ProcessedOrder{
				ID:         order.ID,
				CustomerID: order.CustomerID,
				Amount:     order.Amount,
				Status:     order.Status,
				Fee:        fee,
				Total:      order.Amount + fee,
				CreatedAt:  order.CreatedAt,
			}, nil
		},
	}
	writer, err := fileWriter.NewJSONWriter[ProcessedOrder]("output/processed-orders.json")
	if err != nil {
		log.Fatal("Failed to create writer:", err)
	}

	step := core.NewChunkStep[Order, ProcessedOrder]("processOrders").
		Reader(reader).
		Processor(proc).
		Writer(writer).
		ChunkSize(50).
		Listener(listener.NewLoggingStepListener()).
		Build()
	job := core.NewJob("orderProcessingJob").
		Step(step).
		Listener(listener.NewLoggingJobListener()).
		Build()
	ctx := context.Background()
	execution, err := job.Run(ctx, nil)
	if err != nil {
		log.Fatal("Job failed:", err)
	}

	fmt.Println("\n✅ Job completed successfully!")
	fmt.Printf("Status: %s\n", execution.Status)
}
