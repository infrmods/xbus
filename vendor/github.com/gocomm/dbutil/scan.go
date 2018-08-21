package dbutil

import (
	"database/sql"
	"fmt"
	"reflect"
)

func scanStruct(val reflect.Value, rows *sql.Rows) error {
	meta := GetTableMeta(val.Type())
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if v, err := meta.GetScanFields(columns, val.Addr().Interface()); err == nil {
		return rows.Scan(v...)
	} else {
		return err
	}
}

func scanSlice(val reflect.Value, rows *sql.Rows) (interface{}, error) {
	eleType := val.Type().Elem()
	switch eleType.Kind() {
	case reflect.Struct:
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		meta := GetTableMeta(eleType)
		p, err := meta.PrepareScan(columns)
		for rows.Next() {
			v := reflect.New(eleType)
			fs, err := p.GetScanFields(v.Interface())
			if err != nil {
				return nil, err
			}
			if err := rows.Scan(fs...); err != nil {
				return nil, err
			}
			val = reflect.Append(val, v.Elem())
		}
	default:
		for rows.Next() {
			v := reflect.New(eleType)
			if err := rows.Scan(v.Interface()); err != nil {
				return nil, err
			}
			val = reflect.Append(val, v.Elem())
		}
	}
	return val.Interface(), nil
}

func scanRows(obj interface{}, rows *sql.Rows) error {
	val := reflect.ValueOf(obj)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("invalid scan type: %v", val.Type())
	}
	if val.IsNil() {
		return fmt.Errorf("nil scan ptr")
	}
	objType := val.Elem().Type()
	switch objType.Kind() {
	case reflect.Struct:
		if rows.Next() {
			return scanStruct(val.Elem(), rows)
		} else {
			return sql.ErrNoRows
		}
	case reflect.Slice:
		if s, err := scanSlice(val.Elem(), rows); err == nil {
			val.Elem().Set(reflect.ValueOf(s))
			return nil
		} else {
			return err
		}
	default:
		if rows.Next() {
			return rows.Scan(obj)
		} else {
			return sql.ErrNoRows
		}
	}
}

func Query(db *sql.DB, obj interface{}, q string, v ...interface{}) error {
	if rows, err := db.Query(q, v...); err == nil {
		defer rows.Close()
		return scanRows(obj, rows)
	} else {
		return err
	}
}
