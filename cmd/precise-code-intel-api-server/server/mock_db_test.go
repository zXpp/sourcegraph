package server

import "github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"

type mockDB struct {
	getUploadByID         func(id int) (db.Upload, bool, error)
	getUploadsByRepo      func(repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]db.Upload, int, error)
	enqueue               func(commit, root, tracingContext string, repositoryID int, indexerName string) (int, db.TxCloser, error)
	getStates             func(ids []int) (map[int]string, error)
	deleteUploadByID      func(id int, getTipCommit func(repositoryID int) (string, error)) (bool, error)
	resetStalled          func() ([]int, error)
	getDumpByID           func(id int) (db.Dump, bool, error)
	findClosestDumps      func(repositoryID int, commit, file string) ([]db.Dump, error)
	doPrune               func() (int, bool, error)
	getPackage            func(scheme, name, version string) (db.Dump, bool, error)
	sameRepoPager         func(repositoryID int, commit, scheme, name, version string, limit int) (int, *db.ReferencePager, error)
	packageReferencePager func(scheme, name, version string, repositoryID, limit int) (int, *db.ReferencePager, error)
}

var _ db.DB = &mockDB{}

func (db *mockDB) GetUploadByID(id int) (db.Upload, bool, error) {
	return db.getUploadByID(id)
}

func (db *mockDB) GetUploadsByRepo(repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]db.Upload, int, error) {
	return db.getUploadsByRepo(repositoryID, state, term, visibleAtTip, limit, offset)
}

func (db *mockDB) Enqueue(commit, root, tracingContext string, repositoryID int, indexerName string) (int, db.TxCloser, error) {
	return db.enqueue(commit, root, tracingContext, repositoryID, indexerName)
}

func (db *mockDB) GetStates(ids []int) (map[int]string, error) {
	return db.getStates(ids)
}

func (db *mockDB) DeleteUploadByID(id int, getTipCommit func(repositoryID int) (string, error)) (bool, error) {
	return db.deleteUploadByID(id, getTipCommit)
}

func (db *mockDB) ResetStalled() ([]int, error) {
	return db.resetStalled()
}

func (db *mockDB) GetDumpByID(id int) (db.Dump, bool, error) {
	return db.getDumpByID(id)
}

func (db *mockDB) FindClosestDumps(repositoryID int, commit, file string) ([]db.Dump, error) {
	return db.findClosestDumps(repositoryID, commit, file)
}

func (db *mockDB) DoPrune() (int, bool, error) {
	return db.doPrune()
}

func (db *mockDB) GetPackage(scheme, name, version string) (db.Dump, bool, error) {
	return db.getPackage(scheme, name, version)
}

func (db *mockDB) SameRepoPager(repositoryID int, commit, scheme, name, version string, limit int) (int, *db.ReferencePager, error) {
	return db.sameRepoPager(repositoryID, commit, scheme, name, version, limit)
}

func (db *mockDB) PackageReferencePager(scheme, name, version string, repositoryID, limit int) (int, *db.ReferencePager, error) {
	return db.packageReferencePager(scheme, name, version, repositoryID, limit)
}
