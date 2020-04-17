package db

import "database/sql"

// ReferencePager holds state for a reference result in a SQL transaction. Each page
// requested should have a consistent view into the database.
type ReferencePager struct {
	*txCloser
	pageFromOffset func(offset int) ([]Reference, error)
}

// PageFromOffset returns the page of references that starts at the given offset.
func (p *ReferencePager) PageFromOffset(offset int) ([]Reference, error) {
	return p.pageFromOffset(offset)
}

func newReferencePager(tx *sql.Tx, pageFromOffset func(offset int) ([]Reference, error)) *ReferencePager {
	return &ReferencePager{
		txCloser:       &txCloser{tx},
		pageFromOffset: pageFromOffset,
	}
}

func newEmptyReferencePager(tx *sql.Tx) *ReferencePager {
	return newReferencePager(tx, func(offset int) ([]Reference, error) {
		return nil, nil
	})
}
