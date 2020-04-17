package db

import (
	"context"
	"database/sql"

	"github.com/hashicorp/go-multierror"
	"github.com/keegancsmith/sqlf"
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

type transactionWrapper struct {
	tx *sql.Tx
}

func (tw *transactionWrapper) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return tw.tx.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

func (tw *transactionWrapper) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return tw.tx.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

func (tw *transactionWrapper) exec(ctx context.Context, query *sqlf.Query) (sql.Result, error) {
	return tw.tx.ExecContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}
