package dbutil

import (
	"database/sql"
	"errors"
)

var ErrNotTx = errors.New("not a Tx")

type MayTx struct {
	Tx *sql.Tx
	DB *sql.DB
}

func NewMayTx(db *sql.DB, tx *sql.Tx) *MayTx {
	return &MayTx{Tx: tx, DB: db}
}

func (tx *MayTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if tx != nil {
		return tx.Tx.Query(query, args...)
	}
	return tx.DB.Query(query, args...)
}

func (tx *MayTx) QueryRow(query string, args ...interface{}) *sql.Row {
	if tx != nil {
		return tx.Tx.QueryRow(query, args...)
	}
	return tx.DB.QueryRow(query, args...)
}

func (tx *MayTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if tx != nil {
		return tx.Tx.Exec(query, args...)
	}
	return tx.DB.Exec(query, args...)
}

func (tx *MayTx) Stmt(stmt *sql.Stmt) *sql.Stmt {
	if tx != nil {
		return tx.Tx.Stmt(stmt)
	}
	return stmt
}

func (tx *MayTx) Prepare(query string) (*sql.Stmt, error) {
	if tx != nil {
		return tx.Tx.Prepare(query)
	}
	return tx.DB.Prepare(query)
}

func (tx *MayTx) Commit() error {
	if tx != nil {
		return tx.Tx.Commit()
	}
	return ErrNotTx
}

func (tx *MayTx) Rollback() error {
	if tx != nil {
		return tx.Tx.Rollback()
	}
	return ErrNotTx
}
