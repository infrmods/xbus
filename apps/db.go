package apps

import (
	"database/sql"
	"github.com/gocomm/dbutil"
	"strconv"
	"time"
)

type App struct {
	Id          int64     `json:"-"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Cert        string    `json:"cert"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

type Group struct {
	Id          int64     `json:"-"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

type GroupMember struct {
	Id      int64
	AppId   int64
	GroupId int64
}

type Perm struct {
	Id         int64
	PermType   int
	TargetType int
	TargetId   int64
	Content    string
}

type ConfigItem struct {
	Id         int64
	Name       string
	Value      string
	Ver        int64
	CreateTime time.Time
	ModifyTime time.Time
}

func (m *ConfigItem) GetIntValue() (int64, error) {
	return strconv.ParseInt(m.Value, 10, 64)
}

func (m *ConfigItem) SetIntValue(n int64) {
	m.Value = strconv.FormatInt(n, 10)
}

func GetConfigItem(db *sql.DB, name string) (*ConfigItem, error) {
	row := db.QueryRow(`select id, name, value, ver, create_time, modify_time
                        from config_items where name=?`, name)
	var item ConfigItem
	if err := row.Scan(&item.Id, &item.Name, &item.Value,
		&item.Ver, &item.CreateTime, &item.ModifyTime); err == nil {
		return &item, nil
	} else {
		return nil, err
	}
}

func (item *ConfigItem) Refresh(db *sql.DB) error {
	row := db.QueryRow(`select name, value, ver, create_time, modify_time
                        from config_items where id=?`, item.Id)
	return row.Scan(&item.Name, &item.Value,
		&item.Ver, &item.CreateTime, &item.ModifyTime)
}

func (item *ConfigItem) UpdateValue(db *sql.DB) error {
	_, err := dbutil.Update(db, `update config_items set value=?, ver=ver+1
                                 where id=? and ver=?`,
		item.Value, item.Id, item.Ver)
	return err
}
