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
	query := `
		INSERT INTO lsif_uploads (
			id, commit, root, visible_at_tip, uploaded_at, state,
			failure_summary, failure_stacktrace, started_at, finished_at,
			tracing_context, repository_id, indexer
		) VALUES (
			1, 'deadbeef01deadbeef02deadbeef03deadbeef04', 'sub/', true,
			$1, 'processing', NULL, NULL, $2, NULL, '{"id": 42}', 50, 'lsif-go'
		)
	`
	if _, err := db.db.Exec(query, t1, t2); err != nil {
		t.Fatal(err)
	}

	expected := Upload{
		ID:                1,
		Commit:            "deadbeef01deadbeef02deadbeef03deadbeef04",
		Root:              "sub/",
		VisibleAtTip:      true,
		UploadedAt:        t1,
		State:             "processing",
		FailureSummary:    nil,
		FailureStacktrace: nil,
		StartedAt:         &t2,
		FinishedAt:        nil,
		TracingContext:    `{"id": 42}`,
		RepositoryID:      50,
		Indexer:           "lsif-go",
		Rank:              nil,
	}

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

	// Upload does not exist initially
	if _, exists, err := db.GetUploadByID(1); err != nil {
		t.Fatalf("unexpected error getting upload: %s", err)
	} else if exists {
		t.Fatal("unexpected record")
	}

	t1 := time.Now().UTC()
	t2 := t1.Add(+time.Minute * 5).UTC()
	t3 := t1.Add(+time.Minute * 3).UTC()
	t4 := t1.Add(+time.Minute * 1).UTC()
	t5 := t1.Add(+time.Minute * 4).UTC()
	t6 := t1.Add(+time.Minute * 2).UTC()
	query := `
		INSERT INTO lsif_uploads (id, commit, uploaded_at, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', $1, 'queued', '{"id": 42}', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', $2, 'queued', '{"id": 42}', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', $3, 'queued', '{"id": 42}', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', $4, 'queued', '{"id": 42}', 50, 'lsif-go'),
		(5, 'deadbeef51deadbeef52deadbeef53deadbeef54', $5, 'queued', '{"id": 42}', 50, 'lsif-go'),
		(6, 'deadbeef51deadbeef52deadbeef53deadbeef54', $6, 'processing', '{"id": 42}', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(query, t1, t2, t3, t4, t5, t6); err != nil {
		t.Fatal(err)
	}

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

type printableRank struct{ value *int }

func (r printableRank) String() string {
	if r.value == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", r.value)
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
	query := `
		INSERT INTO lsif_uploads (id, commit, root, visible_at_tip, uploaded_at, state, failure_summary, tracing_context, repository_id, indexer) VALUES
		(1, 'badbabe11badbabe12badbabe13badbabe14badb', 'sub1/', false, $1, 'queued', '', '{"id": 42}', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', '', true, $2, 'errored', 'not a babe', '{"id": 42}', 50, 'lsif-tsc'),
		(3, 'badbabe31badbabe32badbabe33badbabe34badb', 'sub2/', false, $3, 'queued', '', '{"id": 42}', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', '', false, $4, 'queued', '', '{"id": 42}', 51, 'lsif-go'),
		(5, 'badbabe51badbabe52badbabe53badbabe54badb', 'sub1/', true, $5, 'processing', '', '{"id": 42}', 50, 'lsif-tsc'),
		(6, 'deadbeef51deadbeef52deadbeef53deadbeef54', 'sub2/', false, $6, 'processing', '', '{"id": 42}', 50, 'lsif-go'),
		(7, 'deadbeef71deadbeef72deadbeef73deadbeef74', 'sub1/', true, $7, 'completed', '', '{"id": 42}', 50, 'lsif-tsc'),
		(8, 'deadbeef81deadbeef82deadbeef83deadbeef84', '', true, $8, 'completed', '', '{"id": 42}', 50, 'lsif-tsc'),
		(9, 'deadbeef91deadbeef92deadbeef93deadbeef94', '', false, $9, 'queued', '', '{"id": 42}', 50, 'lsif-go'),
		(10, 'deadbeef91deadbeef02deadbeef03deadbeef04', 'sub1/', false, $10, 'completed', '', '{"id": 42}', 50, 'lsif-tsc')
	`
	if _, err := db.db.Exec(query, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		state        string
		term         string
		visibleAtTip bool
		expectedIDs  []int
	}{
		{expectedIDs: []int{1, 2, 3, 5, 6, 7, 8, 9, 10}},
		{state: "completed", expectedIDs: []int{7, 8, 10}},
		{term: "sub", expectedIDs: []int{1, 3, 5, 6, 7, 10}}, // searches root
		{term: "badbabe", expectedIDs: []int{1, 3, 5}},       // searches commits
		{term: "babe", expectedIDs: []int{1, 2, 3, 5}},       // searches commits and failure summary
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

	id, closer, err := db.Enqueue("deadbeef01deadbeef02deadbeef03deadbeef04", "sub/", `{"id": 42}`, 50, "lsif-go")
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
		Commit:            "deadbeef01deadbeef02deadbeef03deadbeef04",
		Root:              "sub/",
		VisibleAtTip:      false,
		UploadedAt:        time.Now(),
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

	id, closer, err := db.Enqueue("deadbeef01deadbeef02deadbeef03deadbeef04", "sub/", `{"id": 42}`, 50, "lsif-go")
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

	query := `
		INSERT INTO lsif_uploads (id, commit, state, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'queued', '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'completed', '', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'processing', '', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', 'errored', '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(query); err != nil {
		t.Fatal(err)
	}

	states, err := db.GetStates([]int{1, 2, 4, 6})
	if err != nil {
		t.Fatalf("unexpected error getting states: %s", err)
	}

	expected := map[int]string{
		1: "queued",
		2: "completed",
		4: "errored",
	}
	if !reflect.DeepEqual(states, expected) {
		t.Errorf("unexpected upload states. want=%v have=%v", expected, states)
	}
}

func TestDeleteUploadByID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	query := `
		INSERT INTO lsif_uploads (
			id, commit, state, visible_at_tip, tracing_context, repository_id, indexer
		) VALUES (
			1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'completed', false, '', 50, 'lsif-go'
		)
	`
	if _, err := db.db.Query(query); err != nil {
		t.Fatal(err)
	}

	var called bool
	getTipCommit := func(repositoryID int) (string, error) {
		called = true
		return "", nil
	}

	found, err := db.DeleteUploadByID(1, getTipCommit)
	if err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	}
	if !found {
		t.Fatalf("expected record to exist")
	}
	if called {
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

	found, err := db.DeleteUploadByID(1, getTipCommit)
	if err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	}
	if found {
		t.Fatalf("unexpected record")
	}
}

func TestDeleteUploadByIDUpdatesVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, visible_at_tip, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'sub1/', 'completed', true, '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'sub2/', 'completed', true, '', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'sub1/', 'completed', false, '', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', 'sub2/', 'completed', false, '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'deadbeef41deadbeef42deadbeef43deadbeef44', NULL),
		(50, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'deadbeef41deadbeef42deadbeef43deadbeef44'),
		(50, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'deadbeef31deadbeef32deadbeef33deadbeef34'),
		(50, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'deadbeef21deadbeef22deadbeef23deadbeef24')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		t.Fatal(err)
	}

	var called bool
	getTipCommit := func(repositoryID int) (string, error) {
		called = true
		return "deadbeef11deadbeef12deadbeef13deadbeef14", nil
	}

	found, err := db.DeleteUploadByID(1, getTipCommit)
	if err != nil {
		t.Fatalf("unexpected error deleting upload: %s", err)
	}
	if !found {
		t.Fatalf("expected record to exist")
	}
	if !called {
		t.Fatalf("expected call to getTipCommit")
	}

	rows, err := db.db.Query("SELECT id, visible_at_tip FROM lsif_uploads")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	visibility := map[int]bool{}
	for rows.Next() {
		var id int
		var visibleAtTip bool
		if err := rows.Scan(&id, &visibleAtTip); err != nil {
			t.Fatal(err)
		}

		visibility[id] = visibleAtTip
	}

	expected := map[int]bool{2: true, 3: true, 4: false}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
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
	// a -- b -- c -- d -- e -- f -- g

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'r1/', 'completed', '', 50, 'lsif-go'),
		(2, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'r2/', 'completed', '', 50, 'lsif-go'),
		(3, 'cccccccccccccccccccccccccccccccccccccccc', '', 'completed', '', 50, 'lsif-go'),
		(4, 'dddddddddddddddddddddddddddddddddddddddd', 'r3/', 'completed', '', 50, 'lsif-go'),
		(5, 'ffffffffffffffffffffffffffffffffffffffff', 'r4/', 'completed', '', 50, 'lsif-go'),
		(6, 'gggggggggggggggggggggggggggggggggggggggg', 'r5/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'cccccccccccccccccccccccccccccccccccccccc'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'),
		(50, 'gggggggggggggggggggggggggggggggggggggggg', 'ffffffffffffffffffffffffffffffffffffffff')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		t.Fatal(err)
	}

	err := db.updateDumpsVisibleFromTip(nil, 50, "ffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	rows, err := db.db.Query("SELECT id, visible_at_tip FROM lsif_uploads")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	visibility := map[int]bool{}
	for rows.Next() {
		var id int
		var visibleAtTip bool
		if err := rows.Scan(&id, &visibleAtTip); err != nil {
			t.Fatal(err)
		}

		visibility[id] = visibleAtTip
	}

	expected := map[int]bool{1: false, 2: false, 3: false, 4: true, 5: true, 6: false}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
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
	// a -- b --

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'r1/', 'completed', '', 50, 'lsif-go'),
		(2, 'cccccccccccccccccccccccccccccccccccccccc', 'r2/', 'completed', '', 50, 'lsif-go'),
		(3, 'dddddddddddddddddddddddddddddddddddddddd', 'r1/', 'completed', '', 50, 'lsif-go'),
		(4, 'ffffffffffffffffffffffffffffffffffffffff', 'r3/', 'completed', '', 50, 'lsif-go'),
		(5, 'gggggggggggggggggggggggggggggggggggggggg', 'r4/', 'completed', '', 50, 'lsif-go'),
		(6, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'r1/', 'completed', '', 50, 'lsif-tsc'),
		(7, 'cccccccccccccccccccccccccccccccccccccccc', 'r2/', 'completed', '', 50, 'lsif-tsc'),
		(8, 'dddddddddddddddddddddddddddddddddddddddd', '', 'completed', '', 50, 'lsif-tsc'),
		(9, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'r3/', 'completed', '', 50, 'lsif-tsc')
	`
	if _, err := db.db.Query(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'cccccccccccccccccccccccccccccccccccccccc'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'),
		(50, 'gggggggggggggggggggggggggggggggggggggggg', 'ffffffffffffffffffffffffffffffffffffffff')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		t.Fatal(err)
	}

	err := db.updateDumpsVisibleFromTip(nil, 50, "ffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	rows, err := db.db.Query("SELECT id, visible_at_tip FROM lsif_uploads")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	visibility := map[int]bool{}
	for rows.Next() {
		var id int
		var visibleAtTip bool
		if err := rows.Scan(&id, &visibleAtTip); err != nil {
			t.Fatal(err)
		}

		visibility[id] = visibleAtTip
	}

	expected := map[int]bool{1: false, 2: true, 3: true, 4: true, 5: false, 6: false, 7: false, 8: false, 9: true}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
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
	// a --+-- [b] --- c ---+
	//     |                |
	//     +--- d --- [e] --+ -- [h] --+-- [i]
	//     |                           |
	//     +-- [f] --- g --------------+

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, root, state, tracing_context, repository_id, indexer) VALUES
		(1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'r2/', 'completed', '', 50, 'lsif-go'),
		(2, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'r2/a/', 'completed', '', 50, 'lsif-go'),
		(3, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'r2/b/', 'completed', '', 50, 'lsif-go'),
		(4, 'ffffffffffffffffffffffffffffffffffffffff', 'r1/a/', 'completed', '', 50, 'lsif-go'),
		(5, 'ffffffffffffffffffffffffffffffffffffffff', 'r1/b/', 'completed', '', 50, 'lsif-go'),
		(6, 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh', 'r1/', 'completed', '', 50, 'lsif-go'),
		(7, 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii', 'r3/', 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(uploadsQuery); err != nil {
		t.Fatal(err)
	}

	commitsQuery := `
		INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES
		(50, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL),
		(50, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'cccccccccccccccccccccccccccccccccccccccc', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
		(50, 'dddddddddddddddddddddddddddddddddddddddd', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'dddddddddddddddddddddddddddddddddddddddd'),
		(50, 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'),
		(50, 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh', 'cccccccccccccccccccccccccccccccccccccccc'),
		(50, 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii', 'gggggggggggggggggggggggggggggggggggggggg'),
		(50, 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii', 'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh'),
		(50, 'ffffffffffffffffffffffffffffffffffffffff', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
		(50, 'gggggggggggggggggggggggggggggggggggggggg', 'ffffffffffffffffffffffffffffffffffffffff')
	`
	if _, err := db.db.Query(commitsQuery); err != nil {
		t.Fatal(err)
	}

	err := db.updateDumpsVisibleFromTip(nil, 50, "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
	if err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	rows, err := db.db.Query("SELECT id, visible_at_tip FROM lsif_uploads")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	visibility := map[int]bool{}
	for rows.Next() {
		var id int
		var visibleAtTip bool
		if err := rows.Scan(&id, &visibleAtTip); err != nil {
			t.Fatal(err)
		}

		visibility[id] = visibleAtTip
	}

	expected := map[int]bool{1: false, 2: true, 3: true, 4: false, 5: false, 6: true, 7: true}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
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

	var values []*sqlf.Query
	for i := 0; i < MaxTraversalLimit+1; i++ {
		v := sqlf.Sprintf(
			"(50, %s, %s)",
			fmt.Sprintf("%040d", i),
			fmt.Sprintf("%040d", i+1),
		)
		values = append(values, v)
	}

	uploadsQuery := `
		INSERT INTO lsif_uploads (id, commit, state, tracing_context, repository_id, indexer) VALUES
		(1, $1, 'completed', '', 50, 'lsif-go')
	`
	if _, err := db.db.Exec(uploadsQuery, fmt.Sprintf("%040d", MaxTraversalLimit)); err != nil {
		t.Fatal(err)
	}

	commitsQuery := sqlf.Sprintf(`INSERT INTO lsif_commits (repository_id, commit, parent_commit) VALUES %s`, sqlf.Join(values, ", "))
	if _, err := db.db.Query(commitsQuery.Query(sqlf.PostgresBindVar), commitsQuery.Args()...); err != nil {
		// TODO - exec, never query
		t.Fatal(err)
	}

	getVisible := func() map[int]bool {
		rows, err := db.db.Query("SELECT id, visible_at_tip FROM lsif_uploads")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		visibility := map[int]bool{}
		for rows.Next() {
			var id int
			var visibleAtTip bool
			if err := rows.Scan(&id, &visibleAtTip); err != nil {
				t.Fatal(err)
			}

			visibility[id] = visibleAtTip
		}

		return visibility
	}

	if err := db.updateDumpsVisibleFromTip(nil, 50, fmt.Sprintf("%040d", MaxTraversalLimit)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	visibility := getVisible()
	expected := map[int]bool{1: true}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
	}

	if err := db.updateDumpsVisibleFromTip(nil, 50, fmt.Sprintf("%040d", 1)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	visibility = getVisible()
	expected = map[int]bool{1: true}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
	}

	if err := db.updateDumpsVisibleFromTip(nil, 50, fmt.Sprintf("%040d", 0)); err != nil {
		t.Fatalf("unexpected error updating dumps visible from tip: %s", err)
	}

	visibility = getVisible()
	expected = map[int]bool{1: false}
	if !reflect.DeepEqual(visibility, expected) {
		t.Errorf("unexpected visibility. want=%v have=%v", expected, visibility)
	}
}

func TestResetStalled(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	t1 := time.Now().Add(-time.Second * 6) // old
	t2 := time.Now().Add(-time.Second * 2) // new enough
	t3 := time.Now().Add(-time.Second * 3) // new enough
	t4 := time.Now().Add(-time.Second * 8) // old
	t5 := time.Now().Add(-time.Second * 8) // old
	query := `
		INSERT INTO lsif_uploads (id, commit, state, started_at, tracing_context, repository_id, indexer) VALUES
		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'processing', $1, '', 50, 'lsif-go'),
		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'processing', $2, '', 50, 'lsif-go'),
		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'processing', $3, '', 50, 'lsif-go'),
		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', 'processing', $4, '', 50, 'lsif-go'),
		(5, 'deadbeef41deadbeef52deadbeef53deadbeef54', 'processing', $5, '', 50, 'lsif-go')
	`
	if _, err := db.db.Query(query, t1, t2, t3, t4, t5); err != nil {
		t.Fatal(err)
	}

	//
	// Lock upload 5 in a transaction that is skipped by reset stalled

	if tx, err := db.db.Begin(); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Query(`SELECT * FROM lsif_uploads WHERE id = 5 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	ids, err := db.ResetStalled()
	if err != nil {
		t.Fatalf("unexpected error resetting stalled uploads: %s", err)
	}

	expected := []int{1, 4}
	if !reflect.DeepEqual(ids, expected) {
		t.Errorf("unexpected ids. want=%v have=%v", expected, ids)
	}
}
