package db

import (
	"context"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

func TestSameRepoPager(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: "deadbeef11deadbeef12deadbeef13deadbeef14", Root: "sub1/"},
		Upload{ID: 2, Commit: "deadbeef21deadbeef22deadbeef23deadbeef24", Root: "sub2/"},
		Upload{ID: 3, Commit: "deadbeef31deadbeef32deadbeef33deadbeef34", Root: "sub3/"},
		Upload{ID: 4, Commit: "deadbeef21deadbeef22deadbeef23deadbeef24", Root: "sub4/"},
		Upload{ID: 5, Commit: "deadbeef11deadbeef12deadbeef13deadbeef14", Root: "sub5/"},
	)

	insertReferences(t, db.db,
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 1, Filter: "f1"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 2, Filter: "f2"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 3, Filter: "f3"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 4, Filter: "f4"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 5, Filter: "f5"},
	)

	insertCommits(t, db.db, map[string][]string{
		"deadbeef01deadbeef02deadbeef03deadbeef04": {},
		"deadbeef11deadbeef12deadbeef13deadbeef14": {"deadbeef01deadbeef02deadbeef03deadbeef04"},
		"deadbeef21deadbeef22deadbeef23deadbeef24": {"deadbeef11deadbeef12deadbeef13deadbeef14"},
		"deadbeef31deadbeef32deadbeef33deadbeef34": {"deadbeef21deadbeef22deadbeef23deadbeef24"},
	})

	totalCount, pager, err := db.SameRepoPager(context.Background(), 50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

	if totalCount != 5 {
		t.Errorf("unexpected dump. want=%v have=%v", 5, totalCount)
	}

	expected := []Reference{
		{DumpID: 1, Filter: "f1"},
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
	}

	if references, err := pager.PageFromOffset(0); err != nil {
		t.Fatalf("unexpected error getting next page: %s", err)
	} else if !reflect.DeepEqual(references, expected) {
		t.Errorf("unexpected references. want=%v have=%v", expected, references)
	}
}

func TestSameRepoPagerEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	totalCount, pager, err := db.SameRepoPager(context.Background(), 50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

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

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub1/"},
		Upload{ID: 2, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub2/"},
		Upload{ID: 3, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub3/"},
		Upload{ID: 4, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub4/"},
		Upload{ID: 5, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub5/"},
		Upload{ID: 6, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub6/"},
		Upload{ID: 7, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub7/"},
		Upload{ID: 8, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub8/"},
		Upload{ID: 9, Commit: "deadbeef01deadbeef02deadbeef03deadbeef04", Root: "sub9/"},
	)

	insertReferences(t, db.db,
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 1, Filter: "f1"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 2, Filter: "f2"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 3, Filter: "f3"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 4, Filter: "f4"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 5, Filter: "f5"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 6, Filter: "f6"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 7, Filter: "f7"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 8, Filter: "f8"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 9, Filter: "f9"},
	)

	insertCommits(t, db.db, map[string][]string{
		"deadbeef01deadbeef02deadbeef03deadbeef04": {},
	})

	totalCount, pager, err := db.SameRepoPager(context.Background(), 50, "deadbeef01deadbeef02deadbeef03deadbeef04", "gomod", "leftpad", "0.1.0", 3)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

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

		if references, err := pager.PageFromOffset(lo); err != nil {
			t.Fatalf("unexpected error getting page at offset %d: %s", lo, err)
		} else if !reflect.DeepEqual(references, expected[lo:hi]) {
			t.Errorf("unexpected references at offset %d. want=%v have=%v", lo, expected[lo:hi], references)
		}
	}
}

func TestSameRepoPagerVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1), Root: "sub1/"}, // not visible
		Upload{ID: 2, Commit: makeCommit(2), Root: "sub2/"}, // not visible
		Upload{ID: 3, Commit: makeCommit(3), Root: "sub1/"},
		Upload{ID: 4, Commit: makeCommit(4), Root: "sub2/"},
		Upload{ID: 5, Commit: makeCommit(5), Root: "sub5/"},
	)

	insertReferences(t, db.db,
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 1, Filter: "f1"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 2, Filter: "f2"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 3, Filter: "f3"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 4, Filter: "f4"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 5, Filter: "f5"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(3)},
		makeCommit(5): {makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
	})

	totalCount, pager, err := db.SameRepoPager(context.Background(), 50, makeCommit(6), "gomod", "leftpad", "0.1.0", 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

	if totalCount != 3 {
		t.Errorf("unexpected dump. want=%v have=%v", 5, totalCount)
	}

	expected := []Reference{
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
	}

	if references, err := pager.PageFromOffset(0); err != nil {
		t.Fatalf("unexpected error getting next page: %s", err)
	} else if !reflect.DeepEqual(references, expected) {
		t.Errorf("unexpected references. want=%v have=%v", expected, references)
	}
}

func TestPackageReferencePager(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: "deadbeef11deadbeef12deadbeef13deadbeef14", VisibleAtTip: true},
		Upload{ID: 2, Commit: "deadbeef21deadbeef22deadbeef23deadbeef24", VisibleAtTip: true, RepositoryID: 51},
		Upload{ID: 3, Commit: "deadbeef31deadbeef32deadbeef33deadbeef34", VisibleAtTip: true, RepositoryID: 52},
		Upload{ID: 4, Commit: "deadbeef41deadbeef42deadbeef43deadbeef44", VisibleAtTip: true, RepositoryID: 53},
		Upload{ID: 5, Commit: "deadbeef51deadbeef52deadbeef53deadbeef54", VisibleAtTip: true, RepositoryID: 54},
		Upload{ID: 6, Commit: "deadbeef61deadbeef62deadbeef63deadbeef64", VisibleAtTip: false, RepositoryID: 55},
		Upload{ID: 7, Commit: "deadbeef61deadbeef62deadbeef63deadbeef64", VisibleAtTip: true, RepositoryID: 56},
	)

	insertReferences(t, db.db,
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 1, Filter: "f1"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 2, Filter: "f2"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 3, Filter: "f3"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 4, Filter: "f4"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 5, Filter: "f5"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 6, Filter: "f6"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 7, Filter: "f7"},
	)

	totalCount, pager, err := db.PackageReferencePager(context.Background(), "gomod", "leftpad", "0.1.0", 50, 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

	if totalCount != 5 {
		t.Errorf("unexpected dump. want=%v have=%v", 5, totalCount)
	}

	expected := []Reference{
		{DumpID: 2, Filter: "f2"},
		{DumpID: 3, Filter: "f3"},
		{DumpID: 4, Filter: "f4"},
		{DumpID: 5, Filter: "f5"},
		{DumpID: 7, Filter: "f7"},
	}

	if references, err := pager.PageFromOffset(0); err != nil {
		t.Fatalf("unexpected error getting next page: %s", err)
	} else if !reflect.DeepEqual(references, expected) {
		t.Errorf("unexpected references. want=%v have=%v", expected, references)
	}
}

func TestPackageReferencePagerEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	totalCount, pager, err := db.PackageReferencePager(context.Background(), "gomod", "leftpad", "0.1.0", 50, 5)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

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

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: "deadbeef11deadbeef12deadbeef13deadbeef14", VisibleAtTip: true, RepositoryID: 51},
		Upload{ID: 2, Commit: "deadbeef21deadbeef22deadbeef23deadbeef24", VisibleAtTip: true, RepositoryID: 52},
		Upload{ID: 3, Commit: "deadbeef31deadbeef32deadbeef33deadbeef34", VisibleAtTip: true, RepositoryID: 53},
		Upload{ID: 4, Commit: "deadbeef41deadbeef42deadbeef43deadbeef44", VisibleAtTip: true, RepositoryID: 54},
		Upload{ID: 5, Commit: "deadbeef51deadbeef52deadbeef53deadbeef54", VisibleAtTip: true, RepositoryID: 55},
		Upload{ID: 6, Commit: "deadbeef61deadbeef62deadbeef63deadbeef64", VisibleAtTip: true, RepositoryID: 56},
		Upload{ID: 7, Commit: "deadbeef71deadbeef72deadbeef73deadbeef74", VisibleAtTip: true, RepositoryID: 57},
		Upload{ID: 8, Commit: "deadbeef81deadbeef82deadbeef83deadbeef84", VisibleAtTip: true, RepositoryID: 58},
		Upload{ID: 9, Commit: "deadbeef91deadbeef92deadbeef93deadbeef94", VisibleAtTip: true, RepositoryID: 59},
	)

	insertReferences(t, db.db,
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 1, Filter: "f1"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 2, Filter: "f2"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 3, Filter: "f3"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 4, Filter: "f4"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 5, Filter: "f5"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 6, Filter: "f6"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 7, Filter: "f7"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 8, Filter: "f8"},
		ReferenceModel{Scheme: "gomod", Name: "leftpad", Version: "0.1.0", DumpID: 9, Filter: "f9"},
	)

	totalCount, pager, err := db.PackageReferencePager(context.Background(), "gomod", "leftpad", "0.1.0", 50, 3)
	if err != nil {
		t.Fatalf("unexpected error getting pager: %s", err)
	}
	defer func() { _ = pager.CloseTx(nil) }()

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
		if references, err := pager.PageFromOffset(testCase.offset); err != nil {
			t.Fatalf("unexpected error getting page at offset %d: %s", testCase.offset, err)
		} else if !reflect.DeepEqual(references, expected[testCase.lo:testCase.hi]) {
			t.Errorf("unexpected references at offset %d. want=%v have=%v", testCase.offset, expected[testCase.lo:testCase.hi], references)
		}
	}
}
