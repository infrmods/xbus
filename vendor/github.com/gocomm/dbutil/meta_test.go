package dbutil

import (
	"reflect"
	"testing"
	"time"
)

type Times struct {
	CreateTime time.Time
	ModifyTime time.Time
}

type User struct {
	Id   int64
	Name string
	Age  int64
	D    string `db:"detail"`
	Times
}

func comparePtr(t *testing.T, name string, right, real interface{}) {
	if !reflect.DeepEqual(right, real) {
		t.Errorf("ptr mismatch: %s", name)
	}
}

func TestMetaFields(t *testing.T) {
	var u User
	meta := GetTableMeta(reflect.TypeOf(u))
	if vs, err := meta.GetScanFields([]string{"id", "name", "age", "detail", "create_time", "modify_time"}, &u); err == nil {
		if len(vs) != 6 {
			t.Errorf("invalid fields: %v", vs)
		} else {
			comparePtr(t, "id", &u.Id, vs[0])
			comparePtr(t, "name", &u.Name, vs[1])
			comparePtr(t, "age", &u.Age, vs[2])
			comparePtr(t, "detail", &u.D, vs[3])
			comparePtr(t, "create_time", &u.CreateTime, vs[4])
			comparePtr(t, "modify_time", &u.ModifyTime, vs[5])
		}
	} else {
		t.Errorf("get fields fail: %v", err)
	}
}

func TestMetaPrepareFields(t *testing.T) {
	var u User
	meta := GetTableMeta(reflect.TypeOf(u))
	p, err := meta.PrepareScan([]string{"id", "name", "age", "detail", "create_time", "modify_time"})
	if err != nil {
		t.Errorf("prepare fail: %v", err)
		return
	}

	if vs, err := p.GetScanFields(&u); err == nil {
		if len(vs) != 6 {
			t.Errorf("invalid fields: %v", vs)
		} else {
			comparePtr(t, "id", &u.Id, vs[0])
			comparePtr(t, "name", &u.Name, vs[1])
			comparePtr(t, "age", &u.Age, vs[2])
			comparePtr(t, "detail", &u.D, vs[3])
			comparePtr(t, "create_time", &u.CreateTime, vs[4])
			comparePtr(t, "modify_time", &u.ModifyTime, vs[5])
		}
	} else {
		t.Errorf("get fields fail: %v", err)
	}
}
