package dbutil

import (
	"database/sql"
)

func Insert(db *sql.DB, q string, v ...interface{}) (int64, error) {
	if rst, err := db.Exec(q, v...); err == nil {
		if effected, err := rst.RowsAffected(); err == nil {
			if effected == 0 {
				return 0, ZeroEffected
			}
			return rst.LastInsertId()
		} else {
			return 0, err
		}
	} else {
		return 0, err
	}
}

func (tx *MayTx) Insert(q string, v ...interface{}) (int64, error) {
	if tx != nil {
		if rst, err := tx.Exec(q, v...); err == nil {
			if effected, err := rst.RowsAffected(); err == nil {
				if effected == 0 {
					return 0, ZeroEffected
				}
				return rst.LastInsertId()
			} else {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	return Insert(tx.DB, q, v...)
}

func Update(db *sql.DB, q string, v ...interface{}) (effected int64, err error) {
	var rst sql.Result
	if rst, err = db.Exec(q, v...); err == nil {
		if effected, err = rst.RowsAffected(); err == nil {
			if effected == 0 {
				return 0, ZeroEffected
			}
		}
	}
	return
}

func (tx *MayTx) Update(q string, v ...interface{}) (effected int64, err error) {
	if tx != nil {
		var rst sql.Result
		if rst, err = tx.Exec(q, v...); err == nil {
			if effected, err = rst.RowsAffected(); err == nil {
				if effected == 0 {
					return 0, ZeroEffected
				}
			}
		}
	}

	return Update(tx.DB, q, v...)
}
