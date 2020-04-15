package db

import (
	"database/sql"

	"github.com/hashicorp/go-multierror"
)

type TxCloser interface {
	CloseTx(err error) error
}

type txCloser struct {
	tx *sql.Tx
}

func (txc *txCloser) CloseTx(err error) error {
	return closeTx(txc.tx, err)
}

func closeTx(tx *sql.Tx, err error) error {
	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			err = multierror.Append(err, rollErr)
		}
		return err
	}

	return tx.Commit()
}
