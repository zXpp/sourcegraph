package db

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

func TestGetDumpByID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Dump does not exist initially
	if _, exists, err := db.GetDumpByID(1); err != nil {
		t.Fatalf("unexpected error getting dump: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}

	t1 := time.Now().UTC()
	t2 := t1.Add(time.Minute).UTC()
	t3 := t1.Add(time.Minute * 2).UTC()
	expected := Dump{
		ID:                1,
		Commit:            makeCommit(1),
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

	insertUploads(t, db.db, Upload{
		ID:                expected.ID,
		Commit:            expected.Commit,
		Root:              expected.Root,
		VisibleAtTip:      expected.VisibleAtTip,
		UploadedAt:        expected.UploadedAt,
		State:             expected.State,
		FailureSummary:    expected.FailureSummary,
		FailureStacktrace: expected.FailureStacktrace,
		StartedAt:         expected.StartedAt,
		FinishedAt:        expected.FinishedAt,
		TracingContext:    expected.TracingContext,
		RepositoryID:      expected.RepositoryID,
		Indexer:           expected.Indexer,
	})

	if dump, exists, err := db.GetDumpByID(1); err != nil {
		t.Fatalf("unexpected error getting dump: %s", err)
	} else if !exists {
		t.Fatal("expected record to exist")
	} else if !reflect.DeepEqual(dump, expected) {
		t.Errorf("unexpected dump. want=%v have=%v", expected, dump)
	}
}

func TestFindClosestDumps(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// [1] --+--- 2 --------+--5 -- 6 --+-- [7]
	//       |              |           |
	//       +-- [3] -- 4 --+           +--- 8

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1)},
		Upload{ID: 2, Commit: makeCommit(3)},
		Upload{ID: 3, Commit: makeCommit(7)},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(1)},
		makeCommit(4): {makeCommit(3)},
		makeCommit(5): {makeCommit(2), makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
		makeCommit(7): {makeCommit(6)},
		makeCommit(8): {makeCommit(6)},
	})

	testCases := []struct {
		commit      string
		expectedIDs []int
	}{
		{commit: makeCommit(1), expectedIDs: []int{1}},
		{commit: makeCommit(2), expectedIDs: []int{1}},
		{commit: makeCommit(3), expectedIDs: []int{2}},
		{commit: makeCommit(4), expectedIDs: []int{2}},
		{commit: makeCommit(6), expectedIDs: []int{3}},
		{commit: makeCommit(7), expectedIDs: []int{3}},
		{commit: makeCommit(5), expectedIDs: []int{1, 2, 3}},
		{commit: makeCommit(8), expectedIDs: []int{1, 2}},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("commit=%s", testCase.commit), func(t *testing.T) {
			dumps, err := db.FindClosestDumps(50, testCase.commit, "file.ts")
			if err != nil {
				t.Fatalf("unexpected error finding closest dumps: %s", err)
			}
			if len(dumps) != 1 {
				t.Errorf("unexpected nearest dump length. want=%d have=%d", 1, len(dumps))
			}

			var found bool
			for _, id := range testCase.expectedIDs {
				if id == dumps[0].ID {
					found = true
				}
			}

			if !found {
				t.Errorf("unexpected nearest dump ids. want one of %v have=%v", testCase.expectedIDs, dumps[0].ID)
			}
		})
	}
}

func TestFindClosestDumpsAlternateCommitGraph(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// 1 --+-- [2] ---- 3
	//     |
	//     +--- 4 --+-- 5 -- 6
	//              |
	//              +-- 7 -- 8

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(2)},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(1)},
		makeCommit(5): {makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
		makeCommit(7): {makeCommit(4)},
		makeCommit(8): {makeCommit(7)},
	})

	testCases := []struct {
		commit      string
		expectedIDs []int
	}{
		{commit: makeCommit(1), expectedIDs: []int{1}},
		{commit: makeCommit(2), expectedIDs: []int{1}},
		{commit: makeCommit(3), expectedIDs: []int{1}},
		{commit: makeCommit(4), expectedIDs: []int{}},
		{commit: makeCommit(6), expectedIDs: []int{}},
		{commit: makeCommit(7), expectedIDs: []int{}},
		{commit: makeCommit(5), expectedIDs: []int{}},
		{commit: makeCommit(8), expectedIDs: []int{}},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("commit=%s", testCase.commit), func(t *testing.T) {
			dumps, err := db.FindClosestDumps(50, testCase.commit, "file.ts")
			if err != nil {
				t.Fatalf("unexpected error finding closest dumps: %s", err)
			}
			if len(testCase.expectedIDs) == 0 {
				if len(dumps) != 0 {
					t.Errorf("unexpected nearest dump length. want=%d have=%d", 0, len(dumps))
				}
				return
			}
			if len(dumps) != 1 {
				t.Errorf("unexpected nearest dump length. want=%d have=%d", 1, len(dumps))
			}

			var found bool
			for _, id := range testCase.expectedIDs {
				if id == dumps[0].ID {
					found = true
				}
			}

			if !found {
				t.Errorf("unexpected nearest dump ids. want one of %v have=%v", testCase.expectedIDs, dumps[0].ID)
			}
		})
	}
}

func TestFindClosestDumpsDistinctRoots(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// 1 --+-- [2]
	//
	// Where LSIF dumps exist at b at roots: root1/ and root2/.

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1), Root: "root1/"},
		Upload{ID: 2, Commit: makeCommit(2), Root: "root2/"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)}})

	testCases := []struct {
		commit      string
		file        string
		expectedIDs []int
	}{
		{commit: makeCommit(1), file: "blah", expectedIDs: []int{}},
		{commit: makeCommit(2), file: "root1/file.ts", expectedIDs: []int{1}},
		{commit: makeCommit(1), file: "root2/file.ts", expectedIDs: []int{2}},
		{commit: makeCommit(2), file: "root2/file.ts", expectedIDs: []int{2}},
		{commit: makeCommit(1), file: "root3/file.ts", expectedIDs: []int{}},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("commit=%s file=%s", testCase.commit, testCase.file)

		t.Run(name, func(t *testing.T) {
			dumps, err := db.FindClosestDumps(50, testCase.commit, testCase.file)
			if err != nil {
				t.Fatalf("unexpected error finding closest dumps: %s", err)
			}
			if len(testCase.expectedIDs) == 0 {
				if len(dumps) != 0 {
					t.Errorf("unexpected nearest dump length. want=%d have=%d", 0, len(dumps))
				}
				return
			}
			if len(dumps) != 1 {
				t.Errorf("unexpected nearest dump length. want=%d have=%d", 1, len(dumps))
			}

			var found bool
			for _, id := range testCase.expectedIDs {
				if id == dumps[0].ID {
					found = true
				}
			}

			if !found {
				t.Errorf("unexpected nearest dump ids. want one of %v have=%v", testCase.expectedIDs, dumps[0].ID)
			}
		})
	}
}

func TestFindClosestDumpsOverlappingRoots(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This database has the following commit graph:
	//
	// 1 -- 2 --+-- 3 --+-- 5 -- 6
	//          |       |
	//          +-- 4 --+
	//
	// With the following LSIF dumps:
	//
	// | Commit | Root    | Indexer |
	// | ------ + ------- + ------- |
	// | 1      | root3/  | lsif-go |
	// | 1      | root4/  | lsif-py |
	// | 2      | root1/  | lsif-go |
	// | 2      | root2/  | lsif-go |
	// | 2      |         | lsif-py | (overwrites root4/ at commit 1)
	// | 3      | root1/  | lsif-go | (overwrites root1/ at commit 2)
	// | 4      |         | lsif-py | (overwrites (root) at commit 2)
	// | 5      | root2/  | lsif-go | (overwrites root2/ at commit 2)
	// | 6      | root1/  | lsif-go | (overwrites root1/ at commit 2)

	insertUploads(t, db.db,
		Upload{ID: 1, Commit: makeCommit(1), Root: "root3/"},
		Upload{ID: 2, Commit: makeCommit(1), Root: "root4/", Indexer: "lsif-py"},
		Upload{ID: 3, Commit: makeCommit(2), Root: "root1/"},
		Upload{ID: 4, Commit: makeCommit(2), Root: "root2/"},
		Upload{ID: 5, Commit: makeCommit(2), Root: "", Indexer: "lsif-py"},
		Upload{ID: 6, Commit: makeCommit(3), Root: "root1/"},
		Upload{ID: 7, Commit: makeCommit(4), Root: "", Indexer: "lsif-py"},
		Upload{ID: 8, Commit: makeCommit(5), Root: "root2/"},
		Upload{ID: 9, Commit: makeCommit(6), Root: "root1/"},
	)

	insertCommits(t, db.db, map[string][]string{
		makeCommit(1): {},
		makeCommit(2): {makeCommit(1)},
		makeCommit(3): {makeCommit(2)},
		makeCommit(4): {makeCommit(2)},
		makeCommit(5): {makeCommit(3), makeCommit(4)},
		makeCommit(6): {makeCommit(5)},
	})

	testCases := []struct {
		commit      string
		file        string
		expectedIDs []int
	}{
		{commit: makeCommit(4), file: "root1/file.ts", expectedIDs: []int{7, 3}},
		{commit: makeCommit(5), file: "root2/file.ts", expectedIDs: []int{8, 7}},
		{commit: makeCommit(3), file: "root3/file.ts", expectedIDs: []int{5, 1}},
		{commit: makeCommit(1), file: "root4/file.ts", expectedIDs: []int{2}},
		{commit: makeCommit(2), file: "root4/file.ts", expectedIDs: []int{5}},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("commit=%s file=%s", testCase.commit, testCase.file)

		t.Run(name, func(t *testing.T) {
			dumps, err := db.FindClosestDumps(50, testCase.commit, testCase.file)
			if err != nil {
				t.Fatalf("unexpected error finding closest dumps: %s", err)
			}
			if len(testCase.expectedIDs) == 0 {
				if len(dumps) != 0 {
					t.Errorf("unexpected nearest dump length. want=%d have=%d", 0, len(dumps))
				}
				return
			}
			if len(dumps) != len(testCase.expectedIDs) {
				t.Errorf("unexpected nearest dump length. want=%d have=%d", 1, len(dumps))
			}

			allPresent := true
			for _, id := range testCase.expectedIDs {
				var found bool
				for _, dump := range dumps {
					if id == dump.ID {
						found = true
					}
				}

				allPresent = allPresent && found
			}

			if !allPresent {
				t.Errorf("unexpected nearest dump ids. want one of %v have=%v", testCase.expectedIDs, dumps[0].ID)
			}
		})
	}
}

func TestFindClosestDumpsMaxTraversalLimit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// This repository has the following commit graph (ancestors to the left):
	//
	// MAX_TRAVERSAL_LIMIT -- ... -- 2 -- 1 -- 0

	commits := map[string][]string{}
	for i := 0; i < MaxTraversalLimit; i++ {
		commits[makeCommit(i)] = []string{makeCommit(i + 1)}
	}

	insertCommits(t, db.db, commits)
	insertUploads(t, db.db, Upload{ID: 1, Commit: makeCommit(0)})

	// (Assuming MAX_TRAVERSAL_LIMIT = 100)
	// At commit `50`, the traversal limit will be reached before visiting commit `0`
	// because commits are visited in this order:
	//
	// | depth | commit |
	// | ----- | ------ |
	// | 1     | 50     | (with direction 'A')
	// | 2     | 50     | (with direction 'D')
	// | 3     | 51     |
	// | 4     | 49     |
	// | 5     | 52     |
	// | 6     | 48     |
	// | ...   |        |
	// | 99    | 99     |
	// | 100   | 1      | (limit reached)

	testCases := []struct {
		commit      string
		file        string
		expectedIDs []int
	}{
		{commit: makeCommit(0), file: "file.ts", expectedIDs: []int{1}},
		{commit: makeCommit(1), file: "file.ts", expectedIDs: []int{1}},
		{commit: makeCommit(MaxTraversalLimit/2 - 1), file: "file.ts", expectedIDs: []int{1}},
		{commit: makeCommit(MaxTraversalLimit / 2), file: "file.ts", expectedIDs: []int{}},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("commit=%s file=%s", testCase.commit, testCase.file)

		t.Run(name, func(t *testing.T) {
			dumps, err := db.FindClosestDumps(50, testCase.commit, testCase.file)
			if err != nil {
				t.Fatalf("unexpected error finding closest dumps: %s", err)
			}
			if len(testCase.expectedIDs) == 0 {
				if len(dumps) != 0 {
					t.Errorf("unexpected nearest dump length. want=%d have=%d", 0, len(dumps))
				}
				return
			}
			if len(dumps) != len(testCase.expectedIDs) {
				t.Errorf("unexpected nearest dump length. want=%d have=%d", 1, len(dumps))
			}

			allPresent := true
			for _, id := range testCase.expectedIDs {
				var found bool
				for _, dump := range dumps {
					if id == dump.ID {
						found = true
					}
				}

				allPresent = allPresent && found
			}

			if !allPresent {
				t.Errorf("unexpected nearest dump ids. want one of %v have=%v", testCase.expectedIDs, dumps)
			}
		})
	}
}

func TestDoPrune(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Cannot prune empty dump set
	if _, prunable, err := db.DoPrune(); err != nil {
		t.Fatalf("unexpected error pruning dumps: %s", err)
	} else if prunable {
		t.Fatal("unexpectedly prunable")
	}

	t1 := time.Now().UTC()
	t2 := t1.Add(time.Minute).UTC()
	t3 := t1.Add(time.Minute * 2).UTC()
	t4 := t1.Add(time.Minute * 3).UTC()

	insertUploads(t, db.db,
		Upload{ID: 1, UploadedAt: t1},
		Upload{ID: 2, UploadedAt: t2, VisibleAtTip: true},
		Upload{ID: 3, UploadedAt: t3},
		Upload{ID: 4, UploadedAt: t4},
	)

	// Prune oldest
	if id, prunable, err := db.DoPrune(); err != nil {
		t.Fatalf("unexpected error pruning dumps: %s", err)
	} else if !prunable {
		t.Fatal("unexpectedly non-prunable")
	} else if id != 1 {
		t.Errorf("unexpected pruned identifier. want=%v have=%v", 1, id)
	}

	// Prune next oldest (skips visible at tip)
	if id, prunable, err := db.DoPrune(); err != nil {
		t.Fatalf("unexpected error pruning dumps: %s", err)
	} else if !prunable {
		t.Fatal("unexpectedly non-prunable")
	} else if id != 3 {
		t.Errorf("unexpected pruned identifier. want=%v have=%v", 3, id)
	}
}
