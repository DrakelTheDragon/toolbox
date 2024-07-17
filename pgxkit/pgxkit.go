package pgxkit

import (
	"context"
	"errors"
	"io/fs"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type NamedArgs = pgx.NamedArgs

type Beginner interface {
	// Begin starts a new pgx.Tx. It may be a true transaction or a pseudo nested transaction implemented by savepoints.
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Copier interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type BatchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) (br pgx.BatchResults)
}

type Opener interface {
	Open(ctx context.Context) error
}

type Closer interface{ Close() }

type Acquirer interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
}

type Connector interface {
	Conn(ctx context.Context) (*pgx.Conn, error)
}

type Migrator interface {
	Migrate(ctx context.Context, fsys fs.FS, act MigrateAction) error
}

type DB interface {
	Beginner
	Copier
	Queryer
	Execer
	BatchSender
	Acquirer
	Closer
}

type Tx interface {
	Beginner
	Copier
	Queryer
	Execer
	BatchSender
	Commit(context.Context) error
	Rollback(context.Context) error
}

type Client interface {
	Opener
	Connector
	DB
	Migrator
}

func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

func Query[T any](ctx context.Context, q Queryer, sql string, args ...any) ([]T, error) {
	rows, _ := q.Query(ctx, sql, args...)
	rec, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	return rec, mapErr(err)
}

func QueryRow[T any](ctx context.Context, q Queryer, sql string, args ...any) (T, error) {
	rows, _ := q.Query(ctx, sql, args...)
	rec, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	return rec, mapErr(err)
}

func QueryValue[T any](ctx context.Context, q Queryer, sql string, args ...any) (T, error) {
	rows, _ := q.Query(ctx, sql, args...)
	val, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[T])
	return val, mapErr(err)
}

func Exec(ctx context.Context, e Execer, sql string, args ...any) error {
	_, err := e.Exec(ctx, sql, args...)
	return mapErr(err)
}

func mapErr(err error) error {
	var pgerr *pgconn.PgError

	switch {
	case err == nil:
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotFound
	case errors.As(err, &pgerr):
		return mapCode(pgerr)
	default:
		return err
	}
}

func mapCode(pgerr *pgconn.PgError) error {
	switch pgerr.Code {
	case pgerrcode.NoData, pgerrcode.NoDataFound:
		return ErrNotFound
	case pgerrcode.UniqueViolation:
		return ErrAlreadyExists
	default:
		return pgerr
	}
}
