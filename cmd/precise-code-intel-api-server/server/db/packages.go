package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

type Reference struct {
	DumpID int
	Filter string
}

func (db *dbImpl) GetPackage(scheme, name, version string) (Dump, bool, error) {
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
		WHERE p.scheme = %s AND p.name = %s AND p.version = %s
		LIMIT 1
	`

	dump, err := scanDump(db.queryRow(context.Background(), sqlf.Sprintf(query, scheme, name, version)))
	if err != nil {
		return Dump{}, false, ignoreNoRows(err)
	}

	return dump, true, nil
}

func (db *dbImpl) SameRepoPager(repositoryID int, commit, scheme, name, version string, limit int) (_ int, _ *ReferencePager, err error) {
	tw, err := db.beginTx(context.Background())
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil {
			err = closeTx(tw.tx, err)
		}
	}()

	visibleIDsQuery := `SELECT id FROM visible_ids`
	visibleIDs, err := scanInts(tw.query(context.Background(), withBidirectionalLineage(visibleIDsQuery, repositoryID, commit)))
	if err != nil {
		return 0, nil, err
	}

	if len(visibleIDs) == 0 {
		return 0, newEmptyReferencePager(tw.tx), nil
	}

	countQuery := `
		SELECT COUNT(1) FROM lsif_references r
		WHERE r.scheme = %s AND r.name = %s AND r.version = %s AND r.dump_id IN (%s)
	`

	totalCount, err := scanInt(tw.queryRow(context.Background(), sqlf.Sprintf(countQuery, scheme, name, version, sqlf.Join(intsToQueries(visibleIDs), ", "))))
	if err != nil {
		return 0, nil, err
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
		query := `
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d on r.dump_id = d.id
			WHERE r.scheme = %s AND r.name = %s AND r.version = %s AND r.dump_id IN (%s)
			ORDER BY d.root OFFSET %s LIMIT %s
		`

		return scanReferences(tw.query(context.Background(), sqlf.Sprintf(query, scheme, name, version, sqlf.Join(intsToQueries(visibleIDs), ", "), offset, limit)))
	}

	return totalCount, newReferencePager(tw.tx, pageFromOffset), nil
}

func (db *dbImpl) PackageReferencePager(scheme, name, version string, repositoryID, limit int) (_ int, _ *ReferencePager, err error) {
	tw, err := db.beginTx(context.Background())
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil {
			err = closeTx(tw.tx, err)
		}
	}()

	countQuery := `
		SELECT COUNT(1) FROM lsif_references r
		LEFT JOIN lsif_dumps d ON r.dump_id = d.id
		WHERE scheme = %s AND name = %s AND version = %s AND d.repository_id != %s AND d.visible_at_tip = true
	`

	totalCount, err := scanInt(tw.queryRow(context.Background(), sqlf.Sprintf(countQuery, scheme, name, version, repositoryID)))
	if err != nil {
		return 0, nil, err
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
		query := `
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d ON r.dump_id = d.id
			WHERE scheme = %s AND name = %s AND version = %s AND d.repository_id != %s AND d.visible_at_tip = true
			ORDER BY d.repository_id, d.root LIMIT %s OFFSET %s
		`

		return scanReferences(tw.query(context.Background(), sqlf.Sprintf(query, scheme, name, version, repositoryID, limit, offset)))
	}

	return totalCount, newReferencePager(tw.tx, pageFromOffset), nil
}
