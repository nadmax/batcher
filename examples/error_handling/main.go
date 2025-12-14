package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nadmax/batcher/pkg/batcher/core"
	"github.com/nadmax/batcher/pkg/batcher/listener"
	"github.com/nadmax/batcher/pkg/batcher/policy"
	"github.com/nadmax/batcher/pkg/batcher/processor"
	fileReader "github.com/nadmax/batcher/pkg/batcher/reader/file"
	fileWriter "github.com/nadmax/batcher/pkg/batcher/writer/file"
)

type Record struct {
	ID    string
	Value int
}

func main() {
	fmt.Println("=== Error Handling ===")

	reader, _ := fileReader.NewCSVReader("data/invalid-data.csv", true)
	proc := &processor.TransformProcessor[map[string]string, Record]{
		Transform: func(row map[string]string) (Record, error) {
			value, err := strconv.Atoi(row["value"])
			if err != nil {
				// This will trigger skip policy
				return Record{}, fmt.Errorf("invalid value: %w", err)
			}

			if value < 0 {
				return Record{}, fmt.Errorf("value cannot be negative")
			}

			return Record{
				ID:    row["id"],
				Value: value,
			}, nil
		},
	}
	writer, _ := fileWriter.NewJSONWriter[Record]("output/valid-records.json")
	skipPolicy := policy.NewLimitSkipPolicy(10)
	skipListener := listener.NewLoggingSkipListener[
		map[string]string,
		Record,
	]()
	step := core.NewChunkStep[map[string]string, Record]("processWithErrors").
		Reader(reader).
		Processor(proc).
		Writer(writer).
		ChunkSize(10).
		SkipPolicy(skipPolicy).
		SkipListener(skipListener).
		Listener(listener.NewLoggingStepListener()).
		Build()
	job := core.NewJob("errorHandlingJob").
		Step(step).
		Listener(listener.NewLoggingJobListener()).
		Build()
	ctx := context.Background()
	execution, err := job.Run(ctx, nil)
	if err != nil {
		fmt.Printf("\n❌ Job failed: %v\n", err)
	} else {
		fmt.Println("\n✅ Job completed with skipped errors!")
		fmt.Printf("Status: %s\n", execution.Status)
	}
}
