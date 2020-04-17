package db

import (
	"context"
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
		WHERE u.id = %s
	`

	upload, err := scanUpload(db.queryRow(context.Background(), sqlf.Sprintf(query, id)))
	if err != nil {
		return Upload{}, false, ignoreNoRows(err)
	}

	return upload, true, nil
}

// TODO - do this transactionally
// TODO - rewrite this logic to be nicer
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
		WHERE %s
		ORDER BY uploaded_at DESC LIMIT %d OFFSET %d
	`

	uploads, err := scanUploads(db.query(context.Background(), sqlf.Sprintf(query, sqlf.Join(conds, " AND "), limit, offset)))
	if err != nil {
		return nil, 0, err
	}

	countQuery := `
		SELECT COUNT(1) FROM lsif_uploads u
		WHERE %s
	`

	count, err := scanInt(db.queryRow(context.Background(), sqlf.Sprintf(countQuery, sqlf.Join(conds, " AND "))))
	if err != nil {
		return nil, 0, err
	}

	return uploads, count, nil
}

func (db *dbImpl) Enqueue(commit, root, tracingContext string, repositoryID int, indexerName string) (_ int, _ TxCloser, err error) {
	tw, err := db.beginTx(context.Background())
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil {
			err = closeTx(tw.tx, err)
		}
	}()

	query := `
		INSERT INTO lsif_uploads (commit, root, tracing_context, repository_id, indexer)
		VALUES (%s, %s, %s, %s, %s)
		RETURNING id
	`

	id, err := scanInt(tw.queryRow(context.Background(), sqlf.Sprintf(query, commit, root, tracingContext, repositoryID, indexerName)))
	if err != nil {
		return 0, nil, err
	}

	return id, &txCloser{tw.tx}, nil
}

func (db *dbImpl) GetStates(ids []int) (map[int]string, error) {
	query := `
		SELECT id, state FROM lsif_uploads
		WHERE id IN (%s)
	`

	return scanStates(db.query(context.Background(), sqlf.Sprintf(query, sqlf.Join(intsToQueries(ids), ", "))))
}

func (db *dbImpl) DeleteUploadByID(id int, getTipCommit func(repositoryID int) (string, error)) (_ bool, err error) {
	tw, err := db.beginTx(context.Background())
	if err != nil {
		return false, err
	}
	defer func() {
		err = closeTx(tw.tx, err)
	}()

	query := `
		DELETE FROM lsif_uploads
		WHERE id = %s
		RETURNING repository_id, visible_at_tip
	`

	repositoryID, visibleAtTip, err := scanVisibility(tw.queryRow(context.Background(), sqlf.Sprintf(query, id)))
	if err != nil {
		return false, ignoreNoRows(err)
	}

	if !visibleAtTip {
		return true, nil
	}

	tipCommit, err := getTipCommit(repositoryID)
	if err != nil {
		return false, err
	}

	if err := db.updateDumpsVisibleFromTip(tw, repositoryID, tipCommit); err != nil {
		return false, err
	}

	return true, nil
}

// TODO  should be in dumps.go?
func (db *dbImpl) updateDumpsVisibleFromTip(tw *transactionWrapper, repositoryID int, tipCommit string) (err error) {
	if tw == nil {
		tw, err = db.beginTx(context.Background())
		if err != nil {
			return
		}
		defer func() {
			err = closeTx(tw.tx, err)
		}()
	}

	// Update dump records by:
	//   (1) unsetting the visibility flag of all previously visible dumps, and
	//   (2) setting the visibility flag of all currently visible dumps
	query := `
		UPDATE lsif_dumps d
		SET visible_at_tip = id IN (SELECT * from visible_ids)
		WHERE d.repository_id = %s AND (d.id IN (SELECT * from visible_ids) OR d.visible_at_tip)
	`

	_, err = tw.exec(context.Background(), withAncestorLineage(query, repositoryID, tipCommit, repositoryID))
	return
}

func (db *dbImpl) ResetStalled() ([]int, error) {
	query := `
		UPDATE lsif_uploads u SET state = 'queued', started_at = null WHERE id = ANY(
			SELECT id FROM lsif_uploads
			WHERE state = 'processing' AND started_at < now() - (%s * interval '1 second')
			FOR UPDATE SKIP LOCKED
		)
		RETURNING u.id
	`

	ids, err := scanInts(db.query(context.Background(), sqlf.Sprintf(query, StalledUploadMaxAge/time.Second)))
	if err != nil {
		return nil, err
	}

	return ids, nil
}
