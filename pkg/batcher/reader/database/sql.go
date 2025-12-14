package reader

import (
	"context"
	"database/sql"
	"fmt"
)

type SQLReader[T any] struct {
	db      *sql.DB
	query   string
	args    []any
	rows    *sql.Rows
	mapFunc func(*sql.Rows) (*T, error)
	started bool
	closed  bool
}

func NewSQLReader[T any](db *sql.DB, query string, mapFunc func(*sql.Rows) (*T, error), args ...interface{}) *SQLReader[T] {
	return &SQLReader[T]{
		db:      db,
		query:   query,
		args:    args,
		mapFunc: mapFunc,
	}
}

func (r *SQLReader[T]) Read(ctx context.Context) (*T, error) {
	if r.closed {
		return nil, fmt.Errorf("EOF")
	}

	if !r.started {
		rows, err := r.db.QueryContext(ctx, r.query, r.args...)
		if err != nil {
			return nil, err
		}
		r.rows = rows
		r.started = true
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("EOF")
	}

	return r.mapFunc(r.rows)
}

func (r *SQLReader[T]) Close() error {
	r.closed = true
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}
