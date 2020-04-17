package db

import (
	"context"
	"database/sql"

	"github.com/keegancsmith/sqlf"
	"github.com/sourcegraph/sourcegraph/internal/db/dbutil"
)

type DB interface {
	GetUploadByID(id int) (Upload, bool, error)
	GetUploadsByRepo(repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]Upload, int, error)
	Enqueue(commit, root, tracingContext string, repositoryID int, indexerName string) (int, TxCloser, error)
	GetStates(ids []int) (map[int]string, error)
	DeleteUploadByID(id int, getTipCommit func(repositoryID int) (string, error)) (bool, error)
	ResetStalled() ([]int, error)

	GetDumpByID(id int) (Dump, bool, error)
	FindClosestDumps(repositoryID int, commit, file string) ([]Dump, error)
	DoPrune() (int, bool, error)

	GetPackage(scheme, name, version string) (Dump, bool, error)
	SameRepoPager(repositoryID int, commit, scheme, name, version string, limit int) (int, *ReferencePager, error)
	PackageReferencePager(scheme, name, version string, repositoryID, limit int) (int, *ReferencePager, error)
}

type dbImpl struct {
	db *sql.DB
}

var _ DB = &dbImpl{}

func New(postgresDSN string) (DB, error) {
	db, err := dbutil.NewDB(postgresDSN, "precise-code-intel-api-server")
	if err != nil {
		return nil, err
	}

	return &dbImpl{db: db}, nil
}

func (db *dbImpl) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

func (db *dbImpl) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return db.db.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

func (db *dbImpl) beginTx(ctx context.Context) (*transactionWrapper, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &transactionWrapper{tx}, nil
}
