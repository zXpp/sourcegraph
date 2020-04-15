package db

import (
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
	db := &DB{db: dbconn.Global}

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
	// TODO - need to test LOTS of conditions here
}

// func TestGetDumps(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip()
// 	}
// 	dbtesting.SetupGlobalTestDB(t)
// 	db := &DB{db: dbconn.Global}

// 	query := `
// 		INSERT INTO lsif_uploads (id, commit, state, tracing_context, repository_id, indexer) VALUES
// 		(1, 'deadbeef11deadbeef12deadbeef13deadbeef14', 'completed', '', 50, 'lsif-go'),
// 		(2, 'deadbeef21deadbeef22deadbeef23deadbeef24', 'completed', '', 50, 'lsif-go'),
// 		(3, 'deadbeef31deadbeef32deadbeef33deadbeef34', 'completed', '', 50, 'lsif-go'),
// 		(4, 'deadbeef41deadbeef42deadbeef43deadbeef44', 'completed', '', 50, 'lsif-go')
// 	`
// 	if _, err := db.db.Query(query); err != nil {
// 		t.Fatal(err)
// 	}

// 	dumps, err := db.GetDumps([]int{1, 2, 4, 6})
// 	if err != nil {
// 		t.Fatalf("unexpected error getting dump: %s", err)
// 	}

// 	commits := map[int]string{}
// 	for id, dump := range dumps {
// 		if id != dump.ID {
// 			t.Errorf("unexpected dump id. want=%v have=%v", dump.ID, id)
// 		}
// 		commits[dump.ID] = dump.Commit
// 	}

// 	expected := map[int]string{
// 		1: "deadbeef11deadbeef12deadbeef13deadbeef14",
// 		2: "deadbeef21deadbeef22deadbeef23deadbeef24",
// 		4: "deadbeef41deadbeef42deadbeef43deadbeef44",
// 	}
// 	if !reflect.DeepEqual(commits, expected) {
// 		t.Errorf("unexpected dump commits. want=%v have=%v", expected, commits)
// 	}
// }

func TestDoPrune(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &DB{db: dbconn.Global}

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

	// Purne next oldest (skips visible at tip)
	if id, prunable, err := db.DoPrune(); err != nil {
		t.Fatalf("unexpected error pruning dumps: %s", err)
	} else if !prunable {
		t.Fatal("unexpectedly non-prunable")
	} else if id != 3 {
		t.Errorf("unexpected pruned identifier. want=%v have=%v", 3, id)
	}
}
