package db

import (
	"database/sql"

	"github.com/hashicorp/go-multierror"
)

type Scanner interface {
	Scan(targets ...interface{}) error
}

func scanInt(scanner Scanner) (value int, err error) {
	err = scanner.Scan(&value)
	return
}

func scanInts(rows *sql.Rows) (values []int, err error) {
	for rows.Next() {
		var value int
		value, err = scanInt(rows)
		if err != nil {
			return
		}

		values = append(values, value)
	}
	return
}

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
