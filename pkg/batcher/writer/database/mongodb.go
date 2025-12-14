package writer

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBWriter[T any] struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func NewMongoDBWriter[T any](client *mongo.Client, database, collection string) *MongoDBWriter[T] {
	return &MongoDBWriter[T]{
		client:     client,
		collection: client.Database(database).Collection(collection),
	}
}

func (w *MongoDBWriter[T]) Write(ctx context.Context, items []T) error {
	docs := make([]any, len(items))
	for i, item := range items {
		docs[i] = item
	}

	_, err := w.collection.InsertMany(ctx, docs)
	return err
}

func (w *MongoDBWriter[T]) Close() error {
	return nil
}
