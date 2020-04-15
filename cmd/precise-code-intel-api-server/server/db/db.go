package db

import (
	"database/sql"

	"github.com/sourcegraph/sourcegraph/internal/db/dbutil"
)

type DB interface {
	GetUploadByID(id int) (Upload, bool, error)
	GetUploadsByRepo(repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]Upload, int, error)
	Enqueue(commit, root, tracingContext string, repositoryID int, indexerName string, callback func(id int) error) (int, error)
	GetStates(ids []int) (map[int]string, error)
	DeleteUploadByID(id int) (found bool, err error)
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
