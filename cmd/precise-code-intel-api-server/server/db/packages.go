package db

import (
	"context"
	"database/sql"

	"github.com/keegancsmith/sqlf"
)

type Reference struct {
	DumpID int
	Filter string
}

type ReferencePager struct {
	tx   *sql.Tx
	next func(offset int) ([]Reference, error)
}

func (p *ReferencePager) Close() error {
	return p.tx.Commit() // TODO - always commit?
}

// TODO - rename
func (p *ReferencePager) Next(offset int) ([]Reference, error) {
	return p.next(offset)
}

func (db *DB) GetPackage(scheme, name, version string) (Dump, bool, error) {
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
		FROM lsif_packages p
		JOIN lsif_uploads u ON p.dump_id = u.id
		WHERE p.scheme = $1 AND p.name = $2 AND p.version = $3
		LIMIT 1
	`

	dump := Dump{}
	if err := db.db.QueryRowContext(context.Background(), query, scheme, name, version).Scan(
		&dump.ID,
		&dump.Commit,
		&dump.Root,
		&dump.VisibleAtTip,
		&dump.UploadedAt,
		&dump.State,
		&dump.FailureSummary,
		&dump.FailureStacktrace,
		&dump.StartedAt,
		&dump.FinishedAt,
		&dump.TracingContext,
		&dump.RepositoryID,
		&dump.Indexer,
	); err != nil {
		if err == sql.ErrNoRows {
			return Dump{}, false, nil
		}

		return Dump{}, false, err
	}

	return dump, true, nil
}

func (db *DB) SameRepoPager(repositoryID int, commit, scheme, name, version string, limit int) (int, *ReferencePager, error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}

	rows, err := tx.QueryContext(context.Background(), "WITH "+bidirectionalLineage+", "+visibleDumps+"SELECT id FROM visible_ids", repositoryID, commit)
	if err != nil {
		_ = tx.Rollback()
		return 0, nil, err
	}
	defer rows.Close()

	var visibleIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			_ = tx.Rollback()
			return 0, nil, err
		}

		visibleIDs = append(visibleIDs, id)
	}

	if len(visibleIDs) == 0 {
		return 0, &ReferencePager{tx: tx, next: func(offset int) ([]Reference, error) { return nil, nil }}, nil
	}

	var qs []*sqlf.Query
	for _, id := range visibleIDs {
		qs = append(qs, sqlf.Sprintf("%d", id))
	}

	cq := sqlf.Sprintf(`
		SELECT COUNT(1) FROM lsif_references r
		WHERE r.scheme = %s AND r.name = %s AND r.version = %s AND r.dump_id IN (%s)
	`, scheme, name, version, sqlf.Join(qs, ", "))

	var totalCount int
	if err := tx.QueryRowContext(context.Background(), cq.Query(sqlf.PostgresBindVar), cq.Args()...).Scan(&totalCount); err != nil {
		_ = tx.Rollback()
		return 0, nil, err
	}

	next := func(offset int) ([]Reference, error) {
		queryx := sqlf.Sprintf(`
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d on r.dump_id = d.id
			WHERE r.scheme = %s AND r.name = %s AND r.version = %s AND r.dump_id IN (%s)
			ORDER BY d.root OFFSET %s LIMIT %s
		`, scheme, name, version, sqlf.Join(qs, ", "), offset, limit)

		rows, err := tx.QueryContext(context.Background(), queryx.Query(sqlf.PostgresBindVar), queryx.Args()...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var refs []Reference
		for rows.Next() {
			var dumpID int
			var filter string

			if err := rows.Scan(&dumpID, &filter); err != nil {
				return nil, err
			}

			refs = append(refs, Reference{dumpID, filter})
		}

		return refs, nil

	}

	return totalCount, &ReferencePager{tx, next}, nil
}

func (db *DB) PackageReferencePager(scheme, name, version string, repositoryID, limit int) (int, *ReferencePager, error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}

	query := `
		SELECT COUNT(1) FROM lsif_references r
		LEFT JOIN lsif_dumps d ON r.dump_id = d.id
		WHERE scheme = $1 AND name = $2 AND version = $3 AND d.repository_id != $4 AND d.visible_at_tip = true
	`

	var totalCount int
	if err := tx.QueryRowContext(context.Background(), query, scheme, name, version, repositoryID).Scan(&totalCount); err != nil {
		_ = tx.Rollback()
		return 0, nil, err
	}

	next := func(offset int) ([]Reference, error) {
		queryx := `
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d ON r.dump_id = d.id
			WHERE scheme = $1 AND name = $2 AND version = $3 AND d.repository_id != $4 AND d.visible_at_tip = true
			ORDER BY d.repository_id, d.root LIMIT $5 OFFSET $6
		`

		rows, err := tx.QueryContext(context.Background(), queryx, scheme, name, version, repositoryID, limit, offset)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var refs []Reference
		for rows.Next() {
			var dumpID int
			var filter string

			if err := rows.Scan(&dumpID, &filter); err != nil {
				return nil, err
			}

			refs = append(refs, Reference{dumpID, filter})
		}

		return refs, nil
	}

	return totalCount, &ReferencePager{tx, next}, nil
}
