package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/keegancsmith/sqlf"
)

const StalledUploadMaxAge = time.Second * 5

type Upload struct {
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
	Rank              *int       `json:"placeInQueue"`
	// TODO - add this as an optional field
	// ProcessedAt       time.Time  `json:"processedAt"`
}

func (db *dbImpl) GetUploadByID(id int) (Upload, bool, error) {
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
			u.indexer,
			s.rank
		FROM lsif_uploads u
		LEFT JOIN (
			SELECT r.id, RANK() OVER (ORDER BY r.uploaded_at) as rank
			FROM lsif_uploads r
			WHERE r.state = 'queued'
		) s
		ON u.id = s.id
		WHERE u.id = $1
	`

	row := db.db.QueryRowContext(context.Background(), query, id)

	upload := Upload{}
	if err := row.Scan(
		&upload.ID,
		&upload.Commit,
		&upload.Root,
		&upload.VisibleAtTip,
		&upload.UploadedAt,
		&upload.State,
		&upload.FailureSummary,
		&upload.FailureStacktrace,
		&upload.StartedAt,
		&upload.FinishedAt,
		&upload.TracingContext,
		&upload.RepositoryID,
		&upload.Indexer,
		&upload.Rank,
	); err != nil {
		if err == sql.ErrNoRows {
			return Upload{}, false, nil
		}

		return Upload{}, false, err
	}

	return upload, true, nil
}

func (db *dbImpl) GetUploadsByRepo(repositoryID int, state, term string, visibleAtTip bool, limit, offset int) ([]Upload, int, error) {
	conds := []*sqlf.Query{
		sqlf.Sprintf("u.repository_id = %s", repositoryID),
	}
	if state != "" {
		conds = append(conds, sqlf.Sprintf("state = %s", state))
	}
	if term != "" {
		var termConds []*sqlf.Query
		for _, column := range []string{"commit", "root", "indexer", "failure_summary", "failure_stacktrace"} {
			termConds = append(termConds, sqlf.Sprintf(column+" LIKE %s", "%"+term+"%"))
		}

		conds = append(conds, sqlf.Sprintf("(%s)", sqlf.Join(termConds, " OR ")))
	}
	if visibleAtTip {
		conds = append(conds, sqlf.Sprintf("visible_at_tip = true"))
	}

	query := sqlf.Sprintf(`
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
			u.indexer,
			s.rank
		FROM lsif_uploads u
		LEFT JOIN (
			SELECT r.id, RANK() OVER (ORDER BY r.uploaded_at) as rank
			FROM lsif_uploads r
			WHERE r.state = 'queued'
		) s
		ON u.id = s.id
		WHERE %s
		ORDER BY uploaded_at DESC LIMIT %d OFFSET %d
	`, sqlf.Join(conds, " AND "), limit, offset)

	rows, err := db.db.QueryContext(context.Background(), query.Query(sqlf.PostgresBindVar), query.Args()...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var uploads []Upload
	for rows.Next() {
		upload := Upload{}
		if err := rows.Scan(
			&upload.ID,
			&upload.Commit,
			&upload.Root,
			&upload.VisibleAtTip,
			&upload.UploadedAt,
			&upload.State,
			&upload.FailureSummary,
			&upload.FailureStacktrace,
			&upload.StartedAt,
			&upload.FinishedAt,
			&upload.TracingContext,
			&upload.RepositoryID,
			&upload.Indexer,
			&upload.Rank,
		); err != nil {
			return nil, 0, err
		}

		uploads = append(uploads, upload)
	}

	// TODO - do this transactionally
	var count int
	countQuery := sqlf.Sprintf("SELECT COUNT(1) FROM lsif_uploads u WHERE %s", sqlf.Join(conds, " AND "))
	if err := db.db.QueryRowContext(context.Background(), countQuery.Query(sqlf.PostgresBindVar), countQuery.Args()...).Scan(&count); err != nil {
		return nil, 0, err
	}

	return uploads, count, nil
}

func (db *dbImpl) Enqueue(commit, root, tracingContext string, repositoryID int, indexerName string) (int, TxCloser, error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}

	var id int
	if err := tx.QueryRowContext(
		context.Background(),
		`INSERT INTO lsif_uploads (commit, root, tracing_context, repository_id, indexer) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		commit, root, tracingContext, repositoryID, indexerName,
	).Scan(&id); err != nil {
		return 0, nil, closeTx(tx, err)
	}

	return id, &txCloser{tx}, nil
}

func (db *dbImpl) GetStates(ids []int) (map[int]string, error) {
	var qs []*sqlf.Query
	for _, id := range ids {
		qs = append(qs, sqlf.Sprintf("%d", id))
	}

	query := sqlf.Sprintf("SELECT id, state FROM lsif_uploads WHERE id IN (%s)", sqlf.Join(qs, ", "))

	rows, err := db.db.QueryContext(context.Background(), query.Query(sqlf.PostgresBindVar), query.Args()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := map[int]string{}
	for rows.Next() {
		var id int
		var state string
		if err := rows.Scan(&id, &state); err != nil {
			return nil, err
		}

		states[id] = state
	}

	return states, nil
}

func (db *dbImpl) DeleteUploadByID(id int, getTipCommit func(repositoryID int) (string, error)) (found bool, err error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return false, err
	}
	defer func() {
		err = closeTx(tx, err)
	}()

	query := "DELETE FROM lsif_uploads WHERE id = $1 RETURNING repository_id, visible_at_tip"

	var repositoryID int
	var visibleAtTip bool
	if err = tx.QueryRowContext(context.Background(), query, id).Scan(&repositoryID, &visibleAtTip); err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}

		return
	}

	found = true
	if !visibleAtTip {
		return
	}

	var tipCommit string
	tipCommit, err = getTipCommit(repositoryID)
	if err != nil {
		return
	}

	// TODO - do we need to discover commits here? The old
	// implementation does it but my gut says no now that
	// I think about it a bit more.

	query2 := "WITH " + ancestorLineage + ", " + visibleDumps + `
			-- Update dump records by:
			--   (1) unsetting the visibility flag of all previously visible dumps, and
			--   (2) setting the visibility flag of all currently visible dumps
			UPDATE lsif_dumps d
			SET visible_at_tip = id IN (SELECT * from visible_ids)
			WHERE d.repository_id = $1 AND (d.id IN (SELECT * from visible_ids) OR d.visible_at_tip)
		`

	if _, err = tx.ExecContext(context.Background(), query2, repositoryID, tipCommit); err != nil {
		return
	}

	return
}

func (db *dbImpl) ResetStalled() ([]int, error) {
	query := `
		UPDATE lsif_uploads u SET state = 'queued', started_at = null WHERE id = ANY(
			SELECT id FROM lsif_uploads
			WHERE state = 'processing' AND started_at < now() - ($1 * interval '1 second')
			FOR UPDATE SKIP LOCKED
		)
		RETURNING u.id
	`

	rows, err := db.db.QueryContext(context.Background(), query, StalledUploadMaxAge/time.Second)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	return ids, nil
}
