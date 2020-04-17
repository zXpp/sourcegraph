package db

import (
	"fmt"

	"github.com/keegancsmith/sqlf"
)

const MaxTraversalLimit = 100

var visibleIDsCTE = `
	-- Limit the visibility to the maximum traversal depth and approximate
	-- each commit's depth by its row number.
	limited_lineage AS (
		SELECT a.*, row_number() OVER() as n from lineage a LIMIT ` + fmt.Sprintf("%d", MaxTraversalLimit) + `
	),
	-- Correlate commits to dumps and filter out commits without LSIF data
	lineage_with_dumps AS (
		SELECT a.*, d.root, d.indexer, d.id as dump_id FROM limited_lineage a
		JOIN lsif_dumps d ON d.repository_id = a.repository_id AND d."commit" = a."commit"
	),
	visible_ids AS (
		-- Remove dumps where there exists another visible dump of smaller depth with an
		-- overlapping root from the same indexer. Such dumps would not be returned with
		-- a closest commit query so we don't want to return results for them in global
		-- find-reference queries either.
		SELECT DISTINCT t1.dump_id as id FROM lineage_with_dumps t1 WHERE NOT EXISTS (
			SELECT 1 FROM lineage_with_dumps t2
			WHERE t2.n < t1.n AND t1.indexer = t2.indexer AND (
				t2.root LIKE (t1.root || '%%%%') OR
				t1.root LIKE (t2.root || '%%%%')
			)
		)
	)
`

func withAncestorLineage(query string, repositoryID int, commit string, args ...interface{}) *sqlf.Query {
	queryWithCTEs := `
		WITH
		RECURSIVE lineage(id, "commit", parent, repository_id) AS (
			SELECT c.* FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
			UNION
			SELECT c.* FROM lineage a JOIN lsif_commits c ON a.repository_id = c.repository_id AND a.parent = c."commit"
		), ` + visibleIDsCTE + " " + query

	return sqlf.Sprintf(queryWithCTEs, append([]interface{}{repositoryID, commit}, args...)...)
}

func withBidirectionalLineage(query string, repositoryID int, commit string, args ...interface{}) *sqlf.Query {
	queryWithCTEs := `
		WITH
		RECURSIVE lineage(id, "commit", parent_commit, repository_id, direction) AS (
			SELECT l.* FROM (
				-- seed recursive set with commit looking in ancestor direction
				SELECT c.*, 'A' FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
				UNION
				-- seed recursive set with commit looking in descendant direction
				SELECT c.*, 'D' FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
			) l

			UNION

			SELECT * FROM (
				WITH l_inner AS (SELECT * FROM lineage)
				-- get next ancestors (multiple parents for merge commits)
				SELECT c.*, 'A' FROM l_inner l JOIN lsif_commits c ON l.direction = 'A' AND c.repository_id = l.repository_id AND c."commit" = l.parent_commit
				UNION
				-- get next descendants
				SELECT c.*, 'D' FROM l_inner l JOIN lsif_commits c ON l.direction = 'D' and c.repository_id = l.repository_id AND c.parent_commit = l."commit"
			) subquery
		), ` + visibleIDsCTE + " " + query

	return sqlf.Sprintf(queryWithCTEs, append([]interface{}{repositoryID, commit, repositoryID, commit}, args...)...)
}
