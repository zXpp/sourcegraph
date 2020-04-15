package db

import (
	"reflect"
	"testing"
	"time"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

func TestGetPackage(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Package does not exist initially
	if _, exists, err := db.GetPackage("gomod", "leftpad", "0.1.0"); err != nil {
		t.Fatalf("unexpected error getting package: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}

	t1 := time.Now().UTC()
	t2 := t1.Add(time.Minute).UTC()
	t3 := t1.Add(time.Minute * 2).UTC()
	dumpQuery := `
		INSERT INTO lsif_uploads (
			id, commit, root, visible_at_tip, uploaded_at, state,
			failure_summary, failure_stacktrace, started_at, finished_at,
			tracing_context, repository_id, indexer
		) VALUES (
			1, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub/', true,
			$1, 'completed', NULL, NULL, $2, $3, '{"id": 42}', 50, 'lsif-go'
		)
	`
	if _, err := db.db.Exec(dumpQuery, t1, t2, t3); err != nil {
		t.Fatal(err)
	}

	packageQuery := `
		INSERT INTO lsif_packages (scheme, name, version, dump_id) VALUES
		('gomod', 'leftpad', '0.1.0', 1)
	`
	if _, err := db.db.Exec(packageQuery); err != nil {
		t.Fatal(err)
	}

	expected := Dump{
		ID:                1,
		Commit:            "deadbeef01deadbeef02deadbeef03deadbeef04",
		Root:              "sub/",
		VisibleAtTip:      true,
		UploadedAt:        t1,
		State:             "completed",
		FailureSummary:    nil,
		FailureStacktrace: nil,
		StartedAt:         &t2,
		FinishedAt:        &t3,
		TracingContext:    `{"id": 42}`,
		RepositoryID:      50,
		Indexer:           "lsif-go",
	}

	if dump, exists, err := db.GetPackage("gomod", "leftpad", "0.1.0"); err != nil {
		t.Fatalf("unexpected error getting package: %s", err)
	} else if !exists {
		t.Fatal("expected record to exist")
	} else if !reflect.DeepEqual(dump, expected) {
		t.Errorf("unexpected dump. want=%v have=%v", expected, dump)
	}
}

func TestSameRepoPager(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	dumpsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'sub1/', 'completed', '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'sub2/', 'completed', '', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'sub3/', 'completed', '', 50, 'lsif-go'),
		(4, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'sub4/', 'completed', '', 50, 'lsif-go'),
		(5, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'sub5/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(dumpsQuery); err != nil {
		t.Fatal(err)
	}

	referenceQuery := `
		INSERT INTO lsif_references (scheme, name, version, dump_id, filter) VALUES
		('gomod', 'leftpad', '0.1.0', 1, 'f1'),
		('gomod', 'leftpad', '0.1.0', 2, 'f2'),
		('gomod', 'leftpad', '0.1.0', 3, 'f3'),
		('gomod', 'leftpad', '0.1.0', 4, 'f4'),
		('gomod', 'leftpad', '0.1.0', 5, 'f5')
	`
	if _, err := db.db.Exec(referenceQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'deadbeef01deadbeef02deadbeef03deadbeef04', NULL),
		(50, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'deadbeef01deadbeef02deadbeef03deadbeef04'),
		(50, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'deadbeef11deadbeef12deadbeef13deadbeef14'),
		(50, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'deadbeef21deadbeef22deadbeef23deadbeef24')
	`
	if _, err := db.db.Exec(commitsQuery); err != nil {
		t.Fatal(err)
	}

	totalCount, pager, err := db.SameRepoPager(50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 5 {
		t.Errorf("unexpected dump. want=%v have=%v", 5, totalCount)
	}

	references, err := pager.PageFromOffset(0)
	if err != nil {
		t.Fatalf("unexpected error getting next page: %s", err)
	}

	expected := []Reference{
		{DumpID: 1, Filter: "f1"},
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
	}
	if !reflect.DeepEqual(references, expected) {
		t.Errorf("unexpected references. want=%v have=%v", expected, references)
	}
}

func TestSameRepoPagerEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	totalCount, pager, err := db.SameRepoPager(50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 0 {
		t.Errorf("unexpected dump. want=%v have=%v", 0, totalCount)
	}
}

func TestSameRepoPagerMultiplePages(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	dumpsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub1/', 'completed', '', 50, 'lsif-go'),
		(2, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub2/', 'completed', '', 50, 'lsif-go'),
		(3, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub3/', 'completed', '', 50, 'lsif-go'),
		(4, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub4/', 'completed', '', 50, 'lsif-go'),
		(5, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub5/', 'completed', '', 50, 'lsif-go'),
		(6, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub6/', 'completed', '', 50, 'lsif-go'),
		(7, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub7/', 'completed', '', 50, 'lsif-go'),
		(8, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub8/', 'completed', '', 50, 'lsif-go'),
		(9, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub9/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(dumpsQuery); err != nil {
		t.Fatal(err)
	}

	referenceQuery := `
		INSERT INTO lsif_references (scheme, name, version, dump_id, filter) VALUES
		('gomod', 'leftpad', '0.1.0', 1, 'f1'),
		('gomod', 'leftpad', '0.1.0', 2, 'f2'),
		('gomod', 'leftpad', '0.1.0', 3, 'f3'),
		('gomod', 'leftpad', '0.1.0', 4, 'f4'),
		('gomod', 'leftpad', '0.1.0', 5, 'f5'),
		('gomod', 'leftpad', '0.1.0', 6, 'f6'),
		('gomod', 'leftpad', '0.1.0', 7, 'f7'),
		('gomod', 'leftpad', '0.1.0', 8, 'f8'),
		('gomod', 'leftpad', '0.1.0', 9, 'f9')
	`
	if _, err := db.db.Exec(referenceQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'deadbeef01deadbeef02deadbeef03deadbeef04', NULL)
	`
	if _, err := db.db.Exec(commitsQuery); err != nil {
		t.Fatal(err)
	}

	totalCount, pager, err := db.SameRepoPager(50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 3)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 9 {
		t.Errorf("unexpected dump. want=%v have=%v", 9, totalCount)
	}

	expected := []Reference{
		{DumpID: 1, Filter: "f1"},
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
		{DumpID: 6, Filter: "f6"},
		{DumpID: 7, Filter: "f7"},
		{DumpID: 8, Filter: "f8"},
		{DumpID: 9, Filter: "f9"},
	}

	for lo := 0; lo < len(expected); lo++ {
		hi := lo + 3
		if hi > len(expected) {
			hi = len(expected)
		}

		references, err := pager.PageFromOffset(lo)
		if err != nil {
			t.Fatalf("unexpected error getting page at offset %d: %s", lo, err)
		}

		if !reflect.DeepEqual(references, expected[lo:hi]) {
			t.Errorf("unexpected references at offset %d. want=%v have=%v", lo, expected[lo:hi], references)
		}

	}
}

// TODO - test visibility

func TestPackageReferencePager(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	dumpsQuery := `
		INSERT INTO lsif_uploads (id, commit, visible_at_tip, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', true, 'completed', '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', true, 'completed', '', 51, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', true, 'completed', '', 52, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', true, 'completed', '', 53, 'lsif-go'),
		(5, 'deadbeef51deadbeef52deadbeef53deadbeef54', true, 'completed', '', 54, 'lsif-go'),
		(6, 'deadbeef61deadbeef62deadbeef63deadbeef64', true, 'completed', '', 55, 'lsif-go')
	`
	if _, err := db.db.Exec(dumpsQuery); err != nil {
		t.Fatal(err)
	}

	referenceQuery := `
		INSERT INTO lsif_references (scheme, name, version, dump_id, filter) VALUES
		('gomod', 'leftpad', '0.1.0', 1, 'f1'),
		('gomod', 'leftpad', '0.1.0', 2, 'f2'),
		('gomod', 'leftpad', '0.1.0', 3, 'f3'),
		('gomod', 'leftpad', '0.1.0', 4, 'f4'),
		('gomod', 'leftpad', '0.1.0', 5, 'f5'),
		('gomod', 'leftpad', '0.1.0', 6, 'f6')
	`
	if _, err := db.db.Exec(referenceQuery); err != nil {
		t.Fatal(err)
	}

	totalCount, pager, err := db.PackageReferencePager("gomod", "leftpad", "0.1.0", 50, 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 5 {
		t.Errorf("unexpected dump. want=%v have=%v", 5, totalCount)
	}

	references, err := pager.PageFromOffset(0)
	if err != nil {
		t.Fatalf("unexpected error getting next page: %s", err)
	}

	expected := []Reference{
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
		{DumpID: 6, Filter: "f6"},
	}
	if !reflect.DeepEqual(references, expected) {
		t.Errorf("unexpected references. want=%v have=%v", expected, references)
	}
}

func TestPackageReferencePagerEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	totalCount, pager, err := db.PackageReferencePager("gomod", "leftpad", "0.1.0", 50, 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 0 {
		t.Errorf("unexpected dump. want=%v have=%v", 0, totalCount)
	}
}

func TestPackageReferencePagerPages(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	dumpsQuery := `
		INSERT INTO lsif_uploads (id, commit, visible_at_tip, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', true, 'completed', '', 51, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', true, 'completed', '', 52, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', true, 'completed', '', 53, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', true, 'completed', '', 54, 'lsif-go'),
		(5, 'deadbeef51deadbeef52deadbeef53deadbeef54', true, 'completed', '', 55, 'lsif-go'),
		(6, 'deadbeef61deadbeef62deadbeef63deadbeef64', true, 'completed', '', 56, 'lsif-go'),
		(7, 'deadbeef71deadbeef72deadbeef73deadbeef74', true, 'completed', '', 57, 'lsif-go'),
		(8, 'deadbeef81deadbeef82deadbeef83deadbeef84', true, 'completed', '', 58, 'lsif-go'),
		(9, 'deadbeef91deadbeef92deadbeef93deadbeef94', true, 'completed', '', 59, 'lsif-go')
	`
	if _, err := db.db.Exec(dumpsQuery); err != nil {
		t.Fatal(err)
	}

	referenceQuery := `
		INSERT INTO lsif_references (scheme, name, version, dump_id, filter) VALUES
		('gomod', 'leftpad', '0.1.0', 1, 'f1'),
		('gomod', 'leftpad', '0.1.0', 2, 'f2'),
		('gomod', 'leftpad', '0.1.0', 3, 'f3'),
		('gomod', 'leftpad', '0.1.0', 4, 'f4'),
		('gomod', 'leftpad', '0.1.0', 5, 'f5'),
		('gomod', 'leftpad', '0.1.0', 6, 'f6'),
		('gomod', 'leftpad', '0.1.0', 7, 'f7'),
		('gomod', 'leftpad', '0.1.0', 8, 'f8'),
		('gomod', 'leftpad', '0.1.0', 9, 'f9')
	`
	if _, err := db.db.Exec(referenceQuery); err != nil {
		t.Fatal(err)
	}

	totalCount, pager, err := db.PackageReferencePager("gomod", "leftpad", "0.1.0", 50, 3)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer pager.CloseTx(nil)

	if totalCount != 9 {
		t.Errorf("unexpected dump. want=%v have=%v", 9, totalCount)
	}

	testCases := []struct {
		offset int
		lo     int
		hi     int
	}{
		{0, 0, 3},
		{1, 1, 4},
		{2, 2, 5},
		{3, 3, 6},
		{4, 4, 7},
		{5, 5, 8},
		{6, 6, 9},
		{7, 7, 9},
		{8, 8, 9},
	}

	expected := []Reference{
		{DumpID: 1, Filter: "f1"},
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
		{DumpID: 6, Filter: "f6"},
		{DumpID: 7, Filter: "f7"},
		{DumpID: 8, Filter: "f8"},
		{DumpID: 9, Filter: "f9"},
	}

	for _, testCase := range testCases {
		references, err := pager.PageFromOffset(testCase.offset)
		if err != nil {
			t.Fatalf("unexpected error getting page at offset %d: %s", testCase.offset, err)
		}

		if !reflect.DeepEqual(references, expected[testCase.lo:testCase.hi]) {
			t.Errorf("unexpected references at offset %d. want=%v have=%v", testCase.offset, expected[testCase.lo:testCase.hi], references)
		}
	}
}

// TODO - test visibility
