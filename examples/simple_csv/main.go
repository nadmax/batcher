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

func main() {
	fmt.Println("=== Simple CSV Processing ===")

	reader, err := fileReader.NewCSVReader("data/customers.csv", true)
	if err != nil {
		log.Fatal("Failed to create reader:", err)
	}

	proc := &processor.PassThroughProcessor[map[string]string]{}
	writer, err := fileWriter.NewCSVWriter("output/processed-customers.csv",
		[]string{"id", "name", "email", "age"})
	if err != nil {
		log.Fatal("Failed to create writer:", err)
	}

	step := core.NewChunkStep[map[string]string, map[string]string]("processCustomers").
		Reader(reader).
		Processor(proc).
		Writer(writer).
		ChunkSize(100).
		Listener(listener.NewLoggingStepListener()).
		Build()
	job := core.NewJob("simpleCSVJob").
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
	fmt.Printf("Duration: %v\n", execution.EndTime.Sub(execution.StartTime))
}
