package db

import (
	"context"
	"time"

	"github.com/keegancsmith/sqlf"
)

type Dump struct {
	ID                int        `json:"id"`
	Commit            string     `json:"commit"`
	Root              string     `json:"root"`
	VisibleAtTip      bool       `json:"visibleAtTip"`
	UploadedAt        time.Time  `json:"uploadedAt"`
	State             string     `json:"state"`
	FailureSummary    *string    `json:"failureSummary"`
	FailureStacktrace *string    `json:"failureStacktrace"`
	StartedAt         *time.Time `json:"startedAt"`
	FinishedAt        *time.Time `json:"finishedAt"`
	TracingContext    string     `json:"tracingContext"`
	RepositoryID      int        `json:"repositoryId"`
	Indexer           string     `json:"indexer"`
	// TODO - add field?
	// ProcessedAt       time.Time  `json:"processedAt"`
}

func (db *dbImpl) GetDumpByID(id int) (Dump, bool, error) {
	query := `
		SELECT
			u.id,
			u.commit,
			u.root,
			u.visible_at_tip,
			u.uploaded_at,
			u.state,
			u.failure_summary,
			u.failure_stacktrace,
			u.started_at,
			u.finished_at,
			u.tracing_context,
			u.repository_id,
			u.indexer
		FROM lsif_uploads u WHERE id = %d
	`

	// TODO - should ensure state as well?
	dump, err := scanDump(db.queryRow(context.Background(), sqlf.Sprintf(query, id)))
	if err != nil {
		return Dump{}, false, ignoreNoRows(err)
	}

	return dump, true, nil
}

func (db *dbImpl) FindClosestDumps(repositoryID int, commit, file string) ([]Dump, error) {
	visibleIDsQuery := `
		SELECT d.dump_id FROM lineage_with_dumps d
		WHERE %s LIKE (d.root || '%%%%') AND d.dump_id IN (SELECT * FROM visible_ids)
		ORDER BY d.n
	`

	ids, err := scanInts(db.query(context.Background(), withBidirectionalLineage(visibleIDsQuery, repositoryID, commit, file)))
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// TODO - completed condition
	query := `
		SELECT
			u.id,
			u.commit,
			u.root,
			u.visible_at_tip,
			u.uploaded_at,
			u.state,
			u.failure_summary,
			u.failure_stacktrace,
			u.started_at,
			u.finished_at,
			u.tracing_context,
			u.repository_id,
			u.indexer
		FROM lsif_uploads u WHERE id IN (%s)
	`

	// TODO - transaction
	dumps, err := scanDumps(db.query(context.Background(), sqlf.Sprintf(query, sqlf.Join(intsToQueries(ids), ", "))))
	if err != nil {
		return nil, err
	}

	return dumps, nil
}

// TODO - rename
func (db *dbImpl) DoPrune() (int, bool, error) {
	// TODO - should only be completed (all methods in this file)
	query := `
		DELETE FROM lsif_uploads
		WHERE ctid IN (
			SELECT ctid FROM lsif_uploads
			WHERE visible_at_tip = false
			ORDER BY uploaded_at
			LIMIT 1
		) RETURNING id
	`

	id, err := scanInt(db.queryRow(context.Background(), sqlf.Sprintf(query)))
	if err != nil {
		return 0, false, ignoreNoRows(err)
	}

	return id, true, nil
}
