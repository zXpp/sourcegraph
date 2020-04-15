package db

import (
	"fmt"
	"reflect"
	"testing"
	"time"

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

// TODO - specifically test for rank

func TestGetUploadsByRepo(t *testing.T) {
	// TODO
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
	// TODO
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
