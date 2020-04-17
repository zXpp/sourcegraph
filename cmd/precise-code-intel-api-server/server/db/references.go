package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

type Reference struct {
	DumpID int
	Filter string
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

	conds := []*sqlf.Query{
		sqlf.Sprintf("r.scheme = %s", scheme),
		sqlf.Sprintf("r.name = %s", name),
		sqlf.Sprintf("r.version = %s", version),
		sqlf.Sprintf("r.dump_id IN (%s)", sqlf.Join(intsToQueries(visibleIDs), ", ")),
	}

	countQuery := `SELECT COUNT(1) FROM lsif_references r WHERE %s`
	totalCount, err := scanInt(tw.queryRow(context.Background(), sqlf.Sprintf(countQuery, sqlf.Join(conds, " AND "))))
	if err != nil {
		return 0, nil, err
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
		query := `
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d on r.dump_id = d.id
			WHERE %s ORDER BY d.root LIMIT %d OFFSET %d
		`

		return scanReferences(tw.query(context.Background(), sqlf.Sprintf(query, sqlf.Join(conds, " AND "), limit, offset)))
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

	conds := []*sqlf.Query{
		sqlf.Sprintf("r.scheme = %s", scheme),
		sqlf.Sprintf("r.name = %s", name),
		sqlf.Sprintf("r.version = %s", version),
		sqlf.Sprintf("d.repository_id != %s", repositoryID),
		sqlf.Sprintf("d.visible_at_tip = true"),
	}

	countQuery := `
		SELECT COUNT(1) FROM lsif_references r
		LEFT JOIN lsif_dumps d ON r.dump_id = d.id
		WHERE %s
	`

	totalCount, err := scanInt(tw.queryRow(context.Background(), sqlf.Sprintf(countQuery, sqlf.Join(conds, " AND "))))
	if err != nil {
		return 0, nil, err
	}

	pageFromOffset := func(offset int) ([]Reference, error) {
		query := `
			SELECT d.id, r.filter FROM lsif_references r
			LEFT JOIN lsif_dumps d ON r.dump_id = d.id
			WHERE %s ORDER BY d.repository_id, d.root LIMIT %d OFFSET %d
		`

		return scanReferences(tw.query(context.Background(), sqlf.Sprintf(query, sqlf.Join(conds, " AND "), limit, offset)))
	}

	return totalCount, newReferencePager(tw.tx, pageFromOffset), nil
}
