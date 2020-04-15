package db

import "database/sql"

type ReferencePager struct {
	*txCloser
	pageFromOffset func(offset int) ([]Reference, error)
}

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
