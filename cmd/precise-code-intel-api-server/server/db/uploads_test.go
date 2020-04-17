package db

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

type printableRank struct{ value *int }

func (r printableRank) String() string {
	if r.value == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", r.value)
}

func TestGetUploadByID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Upload does not exist initially
	if _, exists, err := db.GetUploadByID(1); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}

	t1 := time.Now().UTC()
	t2 := t1.Add(time.Minute).UTC()
	expected := Upload{
		ID:                1,
		Commit:            makeCommit(1),
		Root:              "sub/",
		VisibleAtTip:      true,
		UploadedAt:        t1,
		State:             "processing",
		FailureSummary:    nil,
		FailureStacktrace: nil,
		StartedAt:         &t2,
		FinishedAt:        nil,
		TracingContext:    `{"id": 42}`,
		RepositoryID:      123,
		Indexer:           "lsif-go",
		Rank:              nil,
	}

	insertUploads(t, db.db, expected)

	if upload, exists, err := db.GetUploadByID(1); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if !exists {
		t.Fatal("expected record to exist")
	} else if !reflect.DeepEqual(upload, expected) {
		t.Errorf("unexpected upload. want=%v have=%v", expected, upload)
	}
}

func TestGetQueuedUploadRank(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, UploadedAt: time.Now().UTC(), State: "queued"},
		Upload{ID: 2, UploadedAt: time.Now().UTC().Add(+time.Minute * 5).UTC(), State: "queued"},
		Upload{ID: 3, UploadedAt: time.Now().UTC().Add(+time.Minute * 3).UTC(), State: "queued"},
		Upload{ID: 4, UploadedAt: time.Now().UTC().Add(+time.Minute * 1).UTC(), State: "queued"},
		Upload{ID: 5, UploadedAt: time.Now().UTC().Add(+time.Minute * 4).UTC(), State: "queued"},
		Upload{ID: 6, UploadedAt: time.Now().UTC().Add(+time.Minute * 2).UTC(), State: "processing"},
	)

	if upload, _, _ := db.GetUploadByID(1); upload.Rank == nil || *upload.Rank != 1 {
		t.Errorf("unexpected rank. want=%d have=%s", 1, printableRank{upload.Rank})
	}
	if upload, _, _ := db.GetUploadByID(2); upload.Rank == nil || *upload.Rank != 5 {
		t.Errorf("unexpected rank. want=%d have=%s", 5, printableRank{upload.Rank})
	}
	if upload, _, _ := db.GetUploadByID(3); upload.Rank == nil || *upload.Rank != 3 {
		t.Errorf("unexpected rank. want=%d have=%s", 3, printableRank{upload.Rank})
	}
	if upload, _, _ := db.GetUploadByID(4); upload.Rank == nil || *upload.Rank != 2 {
		t.Errorf("unexpected rank. want=%d have=%s", 2, printableRank{upload.Rank})
	}
	if upload, _, _ := db.GetUploadByID(5); upload.Rank == nil || *upload.Rank != 4 {
		t.Errorf("unexpected rank. want=%d have=%s", 4, printableRank{upload.Rank})
	}

	// Only considers queued uploads to determine rank
	if upload, _, _ := db.GetUploadByID(6); upload.Rank != nil {
		t.Errorf("unexpected rank. want=%s have=%s", "nil", printableRank{upload.Rank})
	}
}

func TestGetUploadsByRepo(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	t1 := time.Now().UTC()
	t2 := t1.Add(-time.Minute * 1).UTC()
	t3 := t1.Add(-time.Minute * 2).UTC()
	t4 := t1.Add(-time.Minute * 3).UTC()
	t5 := t1.Add(-time.Minute * 4).UTC()
	t6 := t1.Add(-time.Minute * 5).UTC()
	t7 := t1.Add(-time.Minute * 6).UTC()
	t8 := t1.Add(-time.Minute * 7).UTC()
	t9 := t1.Add(-time.Minute * 8).UTC()
	t10 := t1.Add(-time.Minute * 9).UTC()
	failureSummary := "unlucky 333"

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(3331), UploadedAt: t1, Root: "sub1/", State: "queued"},
		Upload{ID: 2, UploadedAt: t2, VisibleAtTip: true, State: "errored", FailureSummary: &failureSummary, Indexer: "lsif-tsc"},
		Upload{ID: 3, Commit: makeCommit(3333), UploadedAt: t3, Root: "sub2/", State: "queued"},
		Upload{ID: 4, UploadedAt: t4, State: "queued", RepositoryID: 51},
		Upload{ID: 5, Commit: makeCommit(3333), UploadedAt: t5, Root: "sub1/", VisibleAtTip: true, State: "processing", Indexer: "lsif-tsc"},
		Upload{ID: 6, UploadedAt: t6, Root: "sub2/", State: "processing"},
		Upload{ID: 7, UploadedAt: t7, Root: "sub1/", VisibleAtTip: true, Indexer: "lsif-tsc"},
		Upload{ID: 8, UploadedAt: t8, VisibleAtTip: true, Indexer: "lsif-tsc"},
		Upload{ID: 9, UploadedAt: t9, State: "queued"},
		Upload{ID: 10, UploadedAt: t10, Root: "sub1/", Indexer: "lsif-tsc"},
	)

	testCases := []struct {
		state        string
		term         string
		visibleAtTip bool
		expectedIDs  []int
	}{
		{expectedIDs: []int{1, 2, 3, 5, 6, 7, 8, 9, 10}},
		{state: "completed", expectedIDs: []int{7, 8, 10}},
		{term: "sub", expectedIDs: []int{1, 3, 5, 6, 7, 10}}, // searches root
		{term: "003", expectedIDs: []int{1, 3, 5}},           // searches commits
		{term: "333", expectedIDs: []int{1, 2, 3, 5}},        // searches commits and failure summary
		{term: "tsc", expectedIDs: []int{2, 5, 7, 8, 10}},    // searches indexer
		{visibleAtTip: true, expectedIDs: []int{2, 5, 7, 8}},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("state=%s term=%s visibleAtTip=%v", testCase.state, testCase.term, testCase.visibleAtTip)

		t.Run(name, func(t *testing.T) {
			for lo := 0; lo < len(testCase.expectedIDs); lo++ {
				hi := lo + 3
				if hi > len(testCase.expectedIDs) {
					hi = len(testCase.expectedIDs)
				}

				uploads, totalCount, err := db.GetUploadsByRepo(50, testCase.state, testCase.term, testCase.visibleAtTip, 3, lo)
				if err != nil {
					t.Fatalf("unexpected error getting uploads for repo: %s", err)
				}
				if totalCount != len(testCase.expectedIDs) {
					t.Errorf("unexpected total count. want=%d have=%d", len(testCase.expectedIDs), totalCount)
				}

				var ids []int
				for _, upload := range uploads {
					ids = append(ids, upload.ID)
				}

				if !reflect.DeepEqual(ids, testCase.expectedIDs[lo:hi]) {
					t.Errorf("unexpected upload ids at offset %d. want=%v have=%v", lo, testCase.expectedIDs[lo:hi], ids)
				}
			}
		})
	}
}

func TestEnqueue(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	id, closer, err := db.Enqueue(makeCommit(1), "sub/", `{"id": 42}`, 50, "lsif-go")
	if err != nil {
		t.Fatalf("unexpected error enqueueing upload: %s", err)
	}

	// Upload does not exist before transaction commit
	if _, exists, err := db.GetUploadByID(id); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}

	// Commit transaction
	_ = closer.CloseTx(nil)

	expected := Upload{
		ID:                id,
		Commit:            makeCommit(1),
		Root:              "sub/",
		VisibleAtTip:      false,
		UploadedAt:        time.Now().UTC(),
		State:             "queued",
		FailureSummary:    nil,
		FailureStacktrace: nil,
		StartedAt:         nil,
		FinishedAt:        nil,
		TracingContext:    `{"id": 42}`,
		RepositoryID:      50,
		Indexer:           "lsif-go",
	}

	if upload, exists, err := db.GetUploadByID(id); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if !exists {
		t.Fatal("expected record to exist")
	} else {
		// TODO - make these fields more testable
		rank := 1
		expected.Rank = &rank
		expected.UploadedAt = upload.UploadedAt

		if !reflect.DeepEqual(upload, expected) {
			t.Errorf("unexpected upload. want=%v have=%v", expected, upload)
		}
	}
}

func TestEnqueueRollback(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	id, closer, err := db.Enqueue(makeCommit(1), "sub/", `{"id": 42}`, 50, "lsif-go")
	if err != nil {
		t.Fatalf("unexpected error enqueueing upload: %s", err)
	}
	_ = closer.CloseTx(fmt.Errorf(""))

	// Upload does not exist after rollback
	if _, exists, err := db.GetUploadByID(id); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}
}

func TestGetStates(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, State: "queued"},
		Upload{ID: 2},
		Upload{ID: 3, State: "processing"},
		Upload{ID: 4, State: "errored"},
	)

	expected := map[int]string{
		1: "queued",
		2: "completed",
		4: "errored",
	}

	if states, err := db.GetStates([]int{1, 2, 4, 6}); err != nil {
		t.Fatalf("unexpected error getting states: %s", err)
	} else if !reflect.DeepEqual(states, expected) {
		t.Errorf("unexpected upload states. want=%v have=%v", expected, states)
	}
}

func TestDeleteUploadByID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1},
	)

	var called bool
	getTipCommit := func(repositoryID int) (string, error) {
		called = true
		return "", nil
	}

	if found, err := db.DeleteUploadByID(1, getTipCommit); err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	} else if !found {
		t.Fatalf("expected record to exist")
	} else if called {
		t.Fatalf("unexpected call to getTipCommit")
	}

	// Upload no longer exists
	if _, exists, err := db.GetUploadByID(1); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}
}

func TestDeleteUploadByIDMissingRow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	getTipCommit := func(repositoryID int) (string, error) {
		return "", nil
	}

	if found, err := db.DeleteUploadByID(1, getTipCommit); err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	} else if found {
		t.Fatalf("unexpected record")
	}
}

func TestDeleteUploadByIDUpdatesVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(4), Root: "sub1/", VisibleAtTip: true},
		Upload{ID: 2, Commit: makeCommit(3), Root: "sub2/", VisibleAtTip: true},
		Upload{ID: 3, Commit: makeCommit(2), Root: "sub1/", VisibleAtTip: false},
		Upload{ID: 4, Commit: makeCommit(1), Root: "sub2/", VisibleAtTip: false},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(3)},
	})

	var called bool
	getTipCommit := func(repositoryID int) (string, error) {
		called = true
		return makeCommit(4), nil
	}

	if found, err := db.DeleteUploadByID(1, getTipCommit); err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	} else if !found {
		t.Fatalf("expected record to exist")
	} else if !called {
		t.Fatalf("expected call to getTipCommit")
	}

	expected := map[int]bool{
		2: true,
		3: true,
		4: false,
	}

	if visibilities := getDumpVisibilities(t, db.db); !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}
}

func TestUpdateDumpsVisibleFromTipOverlappingRootsSameIndexer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// [1] -- [2] -- [3] -- [4] -- 5 -- [6] -- [7]

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1), Root: "r1/"},
		Upload{ID: 2, Commit: makeCommit(2), Root: "r2/"},
		Upload{ID: 3, Commit: makeCommit(3)},
		Upload{ID: 4, Commit: makeCommit(4), Root: "r3/"},
		Upload{ID: 5, Commit: makeCommit(6), Root: "r4/"},
		Upload{ID: 6, Commit: makeCommit(7), Root: "r5/"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(3)},
		makeCommit(5): {makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
		makeCommit(7): {makeCommit(6)},
	})

	err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(6))
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	expected := map[int]bool{
		1: false,
		2: false,
		3: false,
		4: true,
		5: true,
		6: false,
	}

	if visibilities := getDumpVisibilities(t, db.db); !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}
}

func TestUpdateDumpsVisibleFromTipOverlappingRoots(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// [1] -- 2 -- [3] -- [4] -- [5] -- [6] -- [7]

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1), Root: "r1/"},
		Upload{ID: 2, Commit: makeCommit(3), Root: "r2/"},
		Upload{ID: 3, Commit: makeCommit(4), Root: "r1/"},
		Upload{ID: 4, Commit: makeCommit(6), Root: "r3/"},
		Upload{ID: 5, Commit: makeCommit(7), Root: "r4/"},
		Upload{ID: 6, Commit: makeCommit(1), Root: "r1/", Indexer: "lsif-tsc"},
		Upload{ID: 7, Commit: makeCommit(3), Root: "r2/", Indexer: "lsif-tsc"},
		Upload{ID: 8, Commit: makeCommit(4), Indexer: "lsif-tsc"},
		Upload{ID: 9, Commit: makeCommit(5), Root: "r3/", Indexer: "lsif-tsc"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(3)},
		makeCommit(5): {makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
		makeCommit(7): {makeCommit(6)},
	})

	err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(6))
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	expected := map[int]bool{
		1: false,
		2: true,
		3: true,
		4: true,
		5: false,
		6: false,
		7: false,
		8: false,
		9: true,
	}

	if visibilities := getDumpVisibilities(t, db.db); !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}
}

func TestUpdateDumpsVisibleFromTipBranchingPaths(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// 1 --+-- [2] --- 3 ---+
	//     |                |
	//     +--- 4 --- [5] --+ -- [8] --+-- [9]
	//     |                           |
	//     +-- [6] --- 7 --------------+

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(2), Root: "r2/"},
		Upload{ID: 2, Commit: makeCommit(5), Root: "r2/a/"},
		Upload{ID: 3, Commit: makeCommit(5), Root: "r2/b/"},
		Upload{ID: 4, Commit: makeCommit(6), Root: "r1/a/"},
		Upload{ID: 5, Commit: makeCommit(6), Root: "r1/b/"},
		Upload{ID: 6, Commit: makeCommit(8), Root: "r1/"},
		Upload{ID: 7, Commit: makeCommit(9), Root: "r3/"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(1)},
		makeCommit(5): {makeCommit(4)},
		makeCommit(8): {makeCommit(5), makeCommit(3)},
		makeCommit(9): {makeCommit(7), makeCommit(8)},
		makeCommit(6): {makeCommit(1)},
		makeCommit(7): {makeCommit(6)},
	})

	err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(9))
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	expected := map[int]bool{
		1: false,
		2: true,
		3: true,
		4: false,
		5: false,
		6: true,
		7: true,
	}

	if visibilities := getDumpVisibilities(t, db.db); !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}
}

func TestUpdateDumpsVisibleFromTipMaxTraversalLimit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This repository has the following commit graph (ancestors to the left):
	//
	// (MAX_TRAVERSAL_LIMIT + 1) -- ... -- 2 -- 1 -- 0

	commits := map[string][]string{}
	for i := 0; i < MaxTraversalLimit+1; i++ {
		commits[makeCommit(i)] = []string{makeCommit(i + 1)}
	}

	insertCommits(t, db.db, commits)
	insertUploads(t, db.db, Upload{ID: 1, Commit: fmt.Sprintf("%040d", MaxTraversalLimit)})

	if err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(MaxTraversalLimit)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	} else if visibilities, expected := getDumpVisibilities(t, db.db), map[int]bool{1: true}; !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}

	if err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(1)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	} else if visibilities, expected := getDumpVisibilities(t, db.db), map[int]bool{1: true}; !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}

	if err := db.updateDumpsVisibleFromTip(nil, 50, makeCommit(0)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	} else if visibilities, expected := getDumpVisibilities(t, db.db), map[int]bool{1: false}; !reflect.DeepEqual(visibilities, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibilities)
	}
}

func TestResetStalled(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	t1 := time.Now().UTC().Add(-time.Second * 6) // old
	t2 := time.Now().UTC().Add(-time.Second * 2) // new enough
	t3 := time.Now().UTC().Add(-time.Second * 3) // new enough
	t4 := time.Now().UTC().Add(-time.Second * 8) // old
	t5 := time.Now().UTC().Add(-time.Second * 8) // old

	insertUploads(t, db.db,
		Upload{ID: 1, State: "processing", StartedAt: &t1},
		Upload{ID: 2, State: "processing", StartedAt: &t2},
		Upload{ID: 3, State: "processing", StartedAt: &t3},
		Upload{ID: 4, State: "processing", StartedAt: &t4},
		Upload{ID: 5, State: "processing", StartedAt: &t5},
	)

	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Row lock upload 5 in a transaction which should be skipped by ResetStalled
	if _, err := tx.Query(`SELECT * FROM lsif_uploads WHERE id = 5 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	expected := []int{1, 4}

	ids, err := db.ResetStalled()
	if err != nil {
		t.Fatalf("unexpected error resetting stalled uploads: %s", err)
	} else if !reflect.DeepEqual(ids, expected) {
		t.Errorf("unexpected ids. want=%v have=%v", expected, ids)
	}
}
