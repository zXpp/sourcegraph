package db

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/keegancsmith/sqlf"
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
	query := `
		INSERT INTO lsif_uploads (
			id, commit, root, visible_at_tip, uploaded_at, state,
			failure_summary, failure_stacktrace, started_at, finished_at,
			tracing_context, repository_id, indexer
		) VALUES (
			1, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub/', true,
			$1, 'completed', NULL, NULL, $2, $3, '{"id": 42}', 50, 'lsif-go'
		)
	`
	if _, err := db.db.Exec(query, t1, t2, t3); err != nil {
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
	// [a] --+--- b --------+--e -- f --+-- [g]
	//       |              |           |
	//       +-- [c] -- d --+           +--- h

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, state, tracing_context, repository_id, indexer) VALUES
		(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'completed', '', 50, 'lsif-go'),
		(2, 'cccccccccccccccccccccccccccccccccccccccc', 'completed', '', 50, 'lsif-go'),
		(3, 'gggggggggggggggggggggggggggggggggggggggg', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'cccccccccccccccccccccccccccccccccccccccc'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'),
		(50, 'gggggggggggggggggggggggggggggggggggggggg', 'ffffffffffffffffffffffffffffffffffffffff'),
		(50, 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh', 'ffffffffffffffffffffffffffffffffffffffff')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

	testCases := []struct {
		commit      string
		expectedIDs []int
	}{
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", expectedIDs: []int{1}},
		{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", expectedIDs: []int{1}},
		{commit: "cccccccccccccccccccccccccccccccccccccccc", expectedIDs: []int{2}},
		{commit: "dddddddddddddddddddddddddddddddddddddddd", expectedIDs: []int{2}},
		{commit: "ffffffffffffffffffffffffffffffffffffffff", expectedIDs: []int{3}},
		{commit: "gggggggggggggggggggggggggggggggggggggggg", expectedIDs: []int{3}},
		{commit: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", expectedIDs: []int{1, 2, 3}},
		{commit: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh", expectedIDs: []int{1, 2}},
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
	// a --+-- [b] ---- c
	//     |
	//     +--- d --+-- e -- f
	//              |
	//              +-- g -- h

	uploadsQuery := `
		INSERT INTO lsif_uploads (
			id, commit, state, tracing_context, repository_id, indexer
		) VALUES (
			1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'completed', '', 50, 'lsif-go'
		)
	`
	if _, err := db.db.Exec(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'),
		(50, 'gggggggggggggggggggggggggggggggggggggggg', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh', 'gggggggggggggggggggggggggggggggggggggggg')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

	testCases := []struct {
		commit      string
		expectedIDs []int
	}{
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", expectedIDs: []int{1}},
		{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", expectedIDs: []int{1}},
		{commit: "cccccccccccccccccccccccccccccccccccccccc", expectedIDs: []int{1}},
		{commit: "dddddddddddddddddddddddddddddddddddddddd", expectedIDs: []int{}},
		{commit: "ffffffffffffffffffffffffffffffffffffffff", expectedIDs: []int{}},
		{commit: "gggggggggggggggggggggggggggggggggggggggg", expectedIDs: []int{}},
		{commit: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", expectedIDs: []int{}},
		{commit: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh", expectedIDs: []int{}},
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
	// a --+-- [b]
	//
	// Where LSIF dumps exist at b at roots: root1/ and root2/.

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'root1/', 'completed', '', 50, 'lsif-go'),
		(2, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'root2/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

	testCases := []struct {
		commit      string
		file        string
		expectedIDs []int
	}{
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", file: "blah", expectedIDs: []int{}},
		{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", file: "root1/file.ts", expectedIDs: []int{1}},
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", file: "root2/file.ts", expectedIDs: []int{2}},
		{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", file: "root2/file.ts", expectedIDs: []int{2}},
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", file: "root3/file.ts", expectedIDs: []int{}},
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
	// a -- b --+-- c --+-- e -- f
	//          |       |
	//          +-- d --+
	//
	// With the following LSIF dumps:
	//
	// | Commit | Root    | Indexer |
	// | ------ + ------- + ------- |
	// | a      | root3/  | A       |
	// | a      | root4/  | B       |
	// | b      | root1/  | A       |
	// | b      | root2/  | A       |
	// | b      |         | B       | (overwrites root4/ at commit a)
	// | c      | root1/  | A       | (overwrites root1/ at commit b)
	// | d      |         | B       | (overwrites (root) at commit b)
	// | e      | root2/  | A       | (overwrites root2/ at commit b)
	// | f      | root1/  | A       | (overwrites root1/ at commit b)

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'root3/', 'completed', '', 50, 'lsif-go'),
		(2, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'root4/', 'completed', '', 50, 'lsif-py'),
		(3, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'root1/', 'completed', '', 50, 'lsif-go'),
		(4, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'root2/', 'completed', '', 50, 'lsif-go'),
		(5, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '', 'completed', '', 50, 'lsif-py'),
		(6, 'cccccccccccccccccccccccccccccccccccccccc', 'root1/', 'completed', '', 50, 'lsif-go'),
		(7, 'dddddddddddddddddddddddddddddddddddddddd', '', 'completed', '', 50, 'lsif-py'),
		(8, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'root2/', 'completed', '', 50, 'lsif-go'),
		(9, 'ffffffffffffffffffffffffffffffffffffffff', 'root1/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'cccccccccccccccccccccccccccccccccccccccc'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

	testCases := []struct {
		commit      string
		file        string
		expectedIDs []int
	}{
		{commit: "dddddddddddddddddddddddddddddddddddddddd", file: "root1/file.ts", expectedIDs: []int{7, 3}},
		{commit: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", file: "root2/file.ts", expectedIDs: []int{8, 7}},
		{commit: "cccccccccccccccccccccccccccccccccccccccc", file: "root3/file.ts", expectedIDs: []int{5, 1}},
		{commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", file: "root4/file.ts", expectedIDs: []int{2}},
		{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", file: "root4/file.ts", expectedIDs: []int{5}},
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

	var values []*sqlf.Query
	for i := 0; i < MaxTraversalLimit; i++ {
		v := sqlf.Sprintf(
			"(50, %s, %s)",
			fmt.Sprintf("%040d", i),
			fmt.Sprintf("%040d", i+1),
		)
		values = append(values, v)
	}

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, state, tracing_context, repository_id, indexer) VALUES
		(1, '0000000000000000000000000000000000000000', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := sqlf.Sprintf(`INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES %s`, sqlf.Join(values, ", "))
	if _, err := db.db.Query(commitsQuery.Query(sqlf.PostgresBindVar), commitsQuery.Args()...); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

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
		{commit: "0000000000000000000000000000000000000000", file: "file.ts", expectedIDs: []int{1}},
		{commit: "0000000000000000000000000000000000000001", file: "file.ts", expectedIDs: []int{1}},
		{commit: fmt.Sprintf("%040d", MaxTraversalLimit/2-1), file: "file.ts", expectedIDs: []int{1}},
		{commit: fmt.Sprintf("%040d", MaxTraversalLimit/2), file: "file.ts", expectedIDs: []int{}},
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
	query := `
		INSERT INTO lsif_uploads (id, commit, visible_at_tip, state, uploaded_at, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', false, 'completed', $1, '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', true,  'completed', $2, '', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', false, 'completed', $3, '', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', false, 'completed', $4, '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(query, t1, t2, t3, t4); err != nil {
		t.Fatal(err)
	}

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
