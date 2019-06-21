package dbutil

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type NullString struct {
	String string
	Valid  bool
}

func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		ns.String, ns.Valid = "", false
		return nil
	}
	switch v := value.(type) {
	case string:
		ns.String = v
	case []byte:
		ns.String = string(v)
	default:
		return fmt.Errorf("invalid string value: %#v", value)
	}
	ns.Valid = true
	return nil
}

func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

func (ns NullString) MarshalJSON() ([]byte, error) {
	if ns.Valid {
		return json.Marshal(ns.String)
	} else {
		return []byte("null"), nil
	}
}

func (ns *NullString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		ns.Valid = false
		return nil
	} else {
		ns.Valid = true
		return json.Unmarshal(data, &ns.String)
	}
}

type NullTime struct {
	Time  time.Time
	Valid bool
}

const timeFormat = "2006-01-02 15:04:05"

func parseDateTime(str string, loc *time.Location) (t time.Time, err error) {
	switch len(str) {
	case 10: // YYYY-MM-DD
		if str == "0000-00-00" {
			return
		}
		t, err = time.Parse(timeFormat[:10], str)
	case 19: // YYYY-MM-DD HH:MM:SS
		if str == "0000-00-00 00:00:00" {
			return
		}
		t, err = time.Parse(timeFormat, str)
	default:
		err = fmt.Errorf("Invalid Time-String: %s", str)
		return
	}

	if err == nil && loc != time.UTC {
		y, mo, d := t.Date()
		h, mi, s := t.Clock()
		t, err = time.Date(y, mo, d, h, mi, s, t.Nanosecond(), loc), nil
	}

	return
}

func (nt *NullTime) Scan(value interface{}) (err error) {
	if value == nil {
		nt.Time, nt.Valid = time.Time{}, false
		return
	}

	switch v := value.(type) {
	case time.Time:
		nt.Time, nt.Valid = v, true
		return
	case []byte:
		nt.Time, err = parseDateTime(string(v), time.UTC)
		nt.Valid = (err == nil)
		return
	case string:
		nt.Time, err = parseDateTime(v, time.UTC)
		nt.Valid = (err == nil)
		return
	}

	nt.Valid = false
	return fmt.Errorf("Can't convert %T to time.Time", value)
}

func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func (nt NullTime) MarshalJSON() ([]byte, error) {
	if nt.Valid {
		return json.Marshal(nt.Time)
	} else {
		return []byte("null"), nil
	}
}

func (nt *NullTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		nt.Valid = false
	} else {
		if err := json.Unmarshal(data, &nt.Time); err != nil {
			return err
		}
		nt.Valid = true
	}
	return nil
}

type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

func (n *NullUint64) Scan(value interface{}) error {
	if value == nil {
		n.Uint64, n.Valid = 0, false
		return nil
	}
	switch v := value.(type) {
	case string:
		if num, err := strconv.ParseUint(v, 10, 64); err == nil {
			n.Uint64 = num
		} else {
			return err
		}
	case []byte:
		if num, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			n.Uint64 = num
		} else {
			return err
		}
	case uint64:
		n.Uint64 = v
	case int64:
		n.Uint64 = uint64(v)
	default:
		return fmt.Errorf("invalid value: %#v", value)
	}
	n.Valid = true
	return nil
}

func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Uint64, nil
}

func (n NullUint64) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Uint64)
	} else {
		return []byte("null"), nil
	}
}

func (n *NullUint64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
	} else {
		if err := json.Unmarshal(data, &n.Uint64); err != nil {
			return err
		}
		n.Valid = true
	}
	return nil
}

type NullInt64 struct {
	Int64 int64
	Valid bool
}

func (n *NullInt64) Scan(value interface{}) error {
	if value == nil {
		n.Int64, n.Valid = 0, false
		return nil
	}
	switch v := value.(type) {
	case string:
		if num, err := strconv.ParseInt(v, 10, 64); err == nil {
			n.Int64 = num
		} else {
			return err
		}
	case []byte:
		if num, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			n.Int64 = num
		} else {
			return err
		}
	case int64:
		n.Int64 = v
	case uint64:
		n.Int64 = int64(v)
	default:
		return fmt.Errorf("invalid value: %#v", value)
	}
	n.Valid = true
	return nil
}

func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}

func (n NullInt64) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Int64)
	} else {
		return []byte("null"), nil
	}
}

func (n *NullInt64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
	} else {
		if err := json.Unmarshal(data, &n.Int64); err != nil {
			return err
		}
		n.Valid = true
	}
	return nil
}

type NullFloat64 struct {
	Float64 float64
	Valid   bool
}

func (n *NullFloat64) Scan(value interface{}) error {
	if value == nil {
		n.Float64, n.Valid = 0, false
		return nil
	}
	switch v := value.(type) {
	case string:
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			n.Float64 = num
		} else {
			return err
		}
	case []byte:
		if num, err := strconv.ParseFloat(string(v), 64); err == nil {
			n.Float64 = num
		} else {
			return err
		}
	case float64:
		n.Float64 = v
	case float32:
		n.Float64 = float64(v)
	default:
		return fmt.Errorf("invalid value: %#v", value)
	}
	n.Valid = true
	return nil
}

func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}

func (n NullFloat64) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Float64)
	} else {
		return []byte("null"), nil
	}
}

func (n *NullFloat64) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
	} else {
		if err := json.Unmarshal(data, &n.Float64); err != nil {
			return err
		}
		n.Valid = true
	}
	return nil
}
