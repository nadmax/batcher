package writer

import (
	"context"
	"database/sql"
)

type SQLWriter[T any] struct {
	db          *sql.DB
	insertQuery string
	mapFunc     func(T) []any
}

func NewSQLWriter[T any](db *sql.DB, insertQuery string, mapFunc func(T) []any) *SQLWriter[T] {
	return &SQLWriter[T]{
		db:          db,
		insertQuery: insertQuery,
		mapFunc:     mapFunc,
	}
}

func (w *SQLWriter[T]) Write(ctx context.Context, items []T) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, w.insertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range items {
		args := w.mapFunc(item)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (w *SQLWriter[T]) Close() error {
	return nil
}
