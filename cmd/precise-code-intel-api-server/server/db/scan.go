package db

import (
	"database/sql"
)

type Scanner interface {
	Scan(targets ...interface{}) error
}

func scanDump(scanner Scanner) (dump Dump, err error) {
	err = scanner.Scan(
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
	)
	return
}

func scanDumps(rows *sql.Rows, err error) ([]Dump, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dumps []Dump
	for rows.Next() {
		dump, err := scanDump(rows)
		if err != nil {
			return nil, err
		}

		dumps = append(dumps, dump)
	}

	return dumps, nil
}

func scanUpload(scanner Scanner) (upload Upload, err error) {
	err = scanner.Scan(
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
	)
	return
}

func scanUploads(rows *sql.Rows, err error) ([]Upload, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []Upload
	for rows.Next() {
		upload, err := scanUpload(rows)
		if err != nil {
			return nil, err
		}

		uploads = append(uploads, upload)
	}

	return uploads, nil
}

func scanReference(scanner Scanner) (reference Reference, err error) {
	err = scanner.Scan(&reference.DumpID, &reference.Filter)
	return
}

func scanReferences(rows *sql.Rows, err error) ([]Reference, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var references []Reference
	for rows.Next() {
		reference, err := scanReference(rows)
		if err != nil {
			return nil, err
		}

		references = append(references, reference)
	}

	return references, nil
}

func scanInt(scanner Scanner) (value int, err error) {
	err = scanner.Scan(&value)
	return
}

func scanInts(rows *sql.Rows, err error) ([]int, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []int
	for rows.Next() {
		value, err := scanInt(rows)
		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return values, nil
}

func scanState(scanner Scanner) (repositoryID int, state string, err error) {
	err = scanner.Scan(&repositoryID, &state)
	return
}

func scanStates(rows *sql.Rows, err error) (map[int]string, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := map[int]string{}
	for rows.Next() {
		repositoryID, state, err := scanState(rows)
		if err != nil {
			return nil, err
		}

		states[repositoryID] = state
	}

	return states, nil
}

func scanVisibility(scanner Scanner) (repositoryID int, visibleAtTip bool, err error) {
	err = scanner.Scan(&repositoryID, &visibleAtTip)
	return
}

func scanVisibilities(rows *sql.Rows, err error) (map[int]bool, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	visibilities := map[int]bool{}
	for rows.Next() {
		repositoryID, visibleAtTip, err := scanVisibility(rows)
		if err != nil {
			return nil, err
		}

		visibilities[repositoryID] = visibleAtTip
	}

	return visibilities, nil
}
