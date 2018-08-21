package dbutil

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type JsonStrList []string

func (list *JsonStrList) Scan(value interface{}) error {
	if value == nil {
		*list = nil
	}
	switch v := value.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), list); err != nil {
			return fmt.Errorf("invalid JsonStrList: %v", err)
		}
	case []byte:
		if err := json.Unmarshal(v, list); err != nil {
			return fmt.Errorf("invalid JsonStrList: %v", err)
		}
	default:
		return fmt.Errorf("invalid JsonStrList: %#v", value)
	}
	return nil
}

func (list JsonStrList) Value() (driver.Value, error) {
	if list == nil {
		return nil, nil
	}
	if data, err := json.Marshal(list); err == nil {
		return string(data), nil
	} else {
		return nil, err
	}
}

type NameList []string

func (sl *NameList) Scan(value interface{}) error {
	if value == nil {
		*sl = nil
		return nil
	}

	switch v := value.(type) {
	case string:
		*sl = strings.Split(v, ",")
	case []byte:
		*sl = strings.Split(string(v), ",")
	default:
		return fmt.Errorf("invalid string list value: %#v", value)
	}

	return nil
}

func (sl NameList) Value() (driver.Value, error) {
	if sl == nil {
		return nil, nil
	}
	return strings.Join(sl, ","), nil
}

type JsonNumList []int64

func (list *JsonNumList) Scan(value interface{}) error {
	if value == nil {
		*list = nil
	}
	switch v := value.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), list); err != nil {
			return fmt.Errorf("invalid JsonNumList: %v", err)
		}
	case []byte:
		if err := json.Unmarshal(v, list); err != nil {
			return fmt.Errorf("invalid JsonNumList: %v", err)
		}
	default:
		return fmt.Errorf("invalid JsonNumList: %#v", value)
	}
	return nil
}

func (list JsonNumList) Value() (driver.Value, error) {
	if list == nil {
		return nil, nil
	}
	if data, err := json.Marshal(list); err == nil {
		return string(data), nil
	} else {
		return nil, err
	}
}

type NumList []int64

func (sl *NumList) Scan(value interface{}) error {
	if value == nil {
		*sl = nil
		return nil
	}

	var ns []string
	switch v := value.(type) {
	case string:
		ns = strings.Split(v, ",")
	case []byte:
		ns = strings.Split(string(v), ",")
	default:
		return fmt.Errorf("invalid string list value: %#v", value)
	}

	nums := make([]int64, 0, len(ns))
	for _, numstr := range ns {
		numstr = strings.TrimSpace(numstr)
		if numstr == "" {
			continue
		}
		if n, err := strconv.ParseInt(numstr, 10, 64); err == nil {
			nums = append(nums, n)
		} else {
			return fmt.Errorf("invalid number: %s", numstr)
		}
	}
	*sl = nums

	return nil
}

func (sl NumList) Value() (driver.Value, error) {
	if sl == nil {
		return nil, nil
	}
	ns := make([]string, len(sl))
	for i, n := range sl {
		ns[i] = strconv.FormatInt(n, 10)
	}
	return strings.Join(ns, ","), nil
}
