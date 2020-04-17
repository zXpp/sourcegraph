package db

import (
	"database/sql"

	"github.com/keegancsmith/sqlf"
)

func ignoreErrNoRows(err error) error {
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

func intsToQueries(values []int) []*sqlf.Query {
	var queries []*sqlf.Query
	for _, value := range values {
		queries = append(queries, sqlf.Sprintf("%d", value))
	}

	return queries
}
