package db

import (
	"context"
	"database/sql"

	"github.com/keegancsmith/sqlf"
	"github.com/sourcegraph/sourcegraph/internal/db/dbutil"
)

type DB interface {
	GetUploadByID(ctx context.Context, id int) (Upload, bool, error)
	GetUploadsByRepo(ctx context.Context, repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]Upload, int, error)
	Enqueue(ctx context.Context, commit, root, tracingContext string, repositoryID int, indexerName string) (int, TxCloser, error)
	GetStates(ctx context.Context, ids []int) (map[int]string, error)
	DeleteUploadByID(ctx context.Context, id int, getTipCommit func(repositoryID int) (string, error)) (bool, error)
	ResetStalled(ctx context.Context) ([]int, error)
	GetDumpByID(ctx context.Context, id int) (Dump, bool, error)
	FindClosestDumps(ctx context.Context, repositoryID int, commit, file string) ([]Dump, error)
	DeleteOldestDump(ctx context.Context) (int, bool, error)
	GetPackage(ctx context.Context, scheme, name, version string) (Dump, bool, error)
	SameRepoPager(ctx context.Context, repositoryID int, commit, scheme, name, version string, limit int) (int, *ReferencePager, error)
	PackageReferencePager(ctx context.Context, scheme, name, version string, repositoryID, limit int) (int, *ReferencePager, error)
}

type dbImpl struct {
	db *sql.DB
}

var _ DB = &dbImpl{}

// New creates a new instance of DB connected to the given Postgres DSN.
func New(postgresDSN string) (DB, error) {
	db, err := dbutil.NewDB(postgresDSN, "precise-code-intel-api-server")
	if err != nil {
		return nil, err
	}

	return &dbImpl{db: db}, nil
}

// query performs Query on the underlying connection.
func (db *dbImpl) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// queryRow performs QueryRow on the underlying connection.
func (db *dbImpl) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return db.db.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// beginTx performs BeginTx on the underlying connection and wraps the transaction.
func (db *dbImpl) beginTx(ctx context.Context) (*transactionWrapper, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &transactionWrapper{tx}, nil
}
