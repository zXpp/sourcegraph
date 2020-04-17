package server

import (
	"context"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

type mockDB struct {
	getUploadByID         func(ctx context.Context, id int) (db.Upload, bool, error)
	getUploadsByRepo      func(ctx context.Context, repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]db.Upload, int, error)
	enqueue               func(ctx context.Context, commit, root, tracingContext string, repositoryID int, indexerName string) (int, db.TxCloser, error)
	getStates             func(ctx context.Context, ids []int) (map[int]string, error)
	deleteUploadByID      func(ctx context.Context, id int, getTipCommit func(repositoryID int) (string, error)) (bool, error)
	resetStalled          func(ctx context.Context) ([]int, error)
	getDumpByID           func(ctx context.Context, id int) (db.Dump, bool, error)
	findClosestDumps      func(ctx context.Context, repositoryID int, commit, file string) ([]db.Dump, error)
	deleteOldestDump      func(ctx context.Context) (int, bool, error)
	getPackage            func(ctx context.Context, scheme, name, version string) (db.Dump, bool, error)
	sameRepoPager         func(ctx context.Context, repositoryID int, commit, scheme, name, version string, limit int) (int, *db.ReferencePager, error)
	packageReferencePager func(ctx context.Context, scheme, name, version string, repositoryID, limit int) (int, *db.ReferencePager, error)
}

var _ db.DB = &mockDB{}

func (db *mockDB) GetUploadByID(ctx context.Context, id int) (db.Upload, bool, error) {
	return db.getUploadByID(ctx, id)
}

func (db *mockDB) GetUploadsByRepo(ctx context.Context, repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]db.Upload, int, error) {
	return db.getUploadsByRepo(ctx, repositoryID, state, term, visibleAtTip, limit, offset)
}

func (db *mockDB) Enqueue(ctx context.Context, commit, root, tracingContext string, repositoryID int, indexerName string) (int, db.TxCloser, error) {
	return db.enqueue(ctx, commit, root, tracingContext, repositoryID, indexerName)
}

func (db *mockDB) GetStates(ctx context.Context, ids []int) (map[int]string, error) {
	return db.getStates(ctx, ids)
}

func (db *mockDB) DeleteUploadByID(ctx context.Context, id int, getTipCommit func(repositoryID int) (string, error)) (bool, error) {
	return db.deleteUploadByID(ctx, id, getTipCommit)
}

func (db *mockDB) ResetStalled(ctx context.Context) ([]int, error) {
	return db.resetStalled(ctx)
}

func (db *mockDB) GetDumpByID(ctx context.Context, id int) (db.Dump, bool, error) {
	return db.getDumpByID(ctx, id)
}

func (db *mockDB) FindClosestDumps(ctx context.Context, repositoryID int, commit, file string) ([]db.Dump, error) {
	return db.findClosestDumps(ctx, repositoryID, commit, file)
}

func (db *mockDB) DeleteOldestDump(ctx context.Context) (int, bool, error) {
	return db.deleteOldestDump(ctx)
}

func (db *mockDB) GetPackage(ctx context.Context, scheme, name, version string) (db.Dump, bool, error) {
	return db.getPackage(ctx, scheme, name, version)
}

func (db *mockDB) SameRepoPager(ctx context.Context, repositoryID int, commit, scheme, name, version string, limit int) (int, *db.ReferencePager, error) {
	return db.sameRepoPager(ctx, repositoryID, commit, scheme, name, version, limit)
}

func (db *mockDB) PackageReferencePager(ctx context.Context, scheme, name, version string, repositoryID, limit int) (int, *db.ReferencePager, error) {
	return db.packageReferencePager(ctx, scheme, name, version, repositoryID, limit)
}
