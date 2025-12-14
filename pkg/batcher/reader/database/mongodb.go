package reader

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBReader[T any] struct {
	client     *mongo.Client
	collection *mongo.Collection
	cursor     *mongo.Cursor
	filter     interface{}
	opts       *options.FindOptions
	started    bool
	closed     bool
}

func NewMongoDBReader[T any](client *mongo.Client, database, collection string, filter interface{}, opts *options.FindOptions) *MongoDBReader[T] {
	return &MongoDBReader[T]{
		client:     client,
		collection: client.Database(database).Collection(collection),
		filter:     filter,
		opts:       opts,
	}
}

func (r *MongoDBReader[T]) Read(ctx context.Context) (*T, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	if !r.started {
		cursor, err := r.collection.Find(ctx, r.filter, r.opts)
		if err != nil {
			return nil, err
		}
		r.cursor = cursor
		r.started = true
	}

	if !r.cursor.Next(ctx) {
		if err := r.cursor.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("EOF")
	}

	var item T
	if err := r.cursor.Decode(&item); err != nil {
		return nil, err
	}

	return &item, nil
}

func (r *MongoDBReader[T]) Close() error {
	r.closed = true
	if r.cursor != nil {
		return r.cursor.Close(context.Background())
	}
	return nil
}
