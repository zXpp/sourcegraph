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

func scanReference(scanner Scanner) (reference Reference, err error) {
	err = scanner.Scan(&reference.DumpID, &reference.Filter)
	return
}

func scanReferences(rows *sql.Rows) (references []Reference, err error) {
	for rows.Next() {
		var reference Reference
		reference, err = scanReference(rows)
		if err != nil {
			return
		}

		references = append(references, reference)
	}
	return
}

func (db *dbImpl) GetPackage(scheme, name, version string) (Dump, bool, error) {
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
			u.indexer
		FROM lsif_packages p
		JOIN lsif_uploads u ON p.dump_id = u.id
		WHERE p.scheme = %s AND p.name = %s AND p.version = %s
		LIMIT 1
	`, scheme, name, version)

	dump, err := scanDump(db.db.QueryRowContext(context.Background(), query.Query(sqlf.PostgresBindVar), query.Args()...))
	if err != nil {
		if err == sql.ErrNoRows {
			return Dump{}, false, nil
		}

		return Dump{}, false, err
	}

	return dump, true, nil
}

func (db *dbImpl) SameRepoPager(repositoryID int, commit, scheme, name, version string, limit int) (int, *ReferencePager, error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}

	rows, err := tx.QueryContext(context.Background(), "WITH "+bidirectionalLineage+", "+visibleDumps+"SELECT id FROM visible_ids", repositoryID, commit)
	if err != nil {
		return 0, nil, closeTx(tx, err)
	}
	defer rows.Close()

	var visibleIDs []int
	for rows.Next() {
		id, err := scanInt(rows)
		if err != nil {
			return 0, nil, closeTx(tx, err)
		}

		visibleIDs = append(visibleIDs, id)
	}

	if len(visibleIDs) == 0 {
		return 0, newEmptyReferencePager(tx), nil
	}

	var qs []*sqlf.Query
	for _, id := range visibleIDs {
		qs = append(qs, sqlf.Sprintf("%d", id))
	}

	cq := sqlf.Sprintf(`
		SELECT COUNT(1) FROM lsif_references r
		WHERE r.scheme = %s AND r.name = %s AND r.version = %s AND r.dump_id IN (%s)
	`, scheme, name, version, sqlf.Join(qs, ", "))

	totalCount, err := scanInt(tx.QueryRowContext(context.Background(), cq.Query(sqlf.PostgresBindVar), cq.Args()...))
	if err != nil {
		return 0, nil, closeTx(tx, err)
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
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

		references, err := scanReferences(rows)
		if err != nil {
			return nil, err
		}

		return references, nil
	}

	return totalCount, newReferencePager(tx, pageFromOffset), nil
}

func (db *dbImpl) PackageReferencePager(scheme, name, version string, repositoryID, limit int) (int, *ReferencePager, error) {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}

	query := sqlf.Sprintf(`
		SELECT COUNT(1) FROM lsif_references r
		LEFT JOIN lsif_dumps d ON r.dump_id = d.id
		WHERE scheme = %s AND name = %s AND version = %s AND d.repository_id != %s AND d.visible_at_tip = true
	`, scheme, name, version, repositoryID)

	totalCount, err := scanInt(tx.QueryRowContext(context.Background(), query.Query(sqlf.PostgresBindVar), query.Args()...))
	if err != nil {
		return 0, nil, closeTx(tx, err)
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
		queryx := sqlf.Sprintf(`
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d ON r.dump_id = d.id
			WHERE scheme = %s AND name = %s AND version = %s AND d.repository_id != %s AND d.visible_at_tip = true
			ORDER BY d.repository_id, d.root LIMIT %s OFFSET %s
		`, scheme, name, version, repositoryID, limit, offset)

		rows, err := tx.QueryContext(context.Background(), queryx.Query(sqlf.PostgresBindVar), queryx.Args()...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		references, err := scanReferences(rows)
		if err != nil {
			return nil, err
		}

		return references, nil
	}

	return totalCount, newReferencePager(tx, pageFromOffset), nil
}
