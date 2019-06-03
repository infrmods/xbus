package apps

import (
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"strconv"
	"time"

	"github.com/gocomm/dbutil"
)

type App struct {
	Id          int64     `json:"-"`
	Status      int       `json:"status"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	PrivateKey  string    `json:"-"`
	Cert        string    `json:"cert"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`

	certificate *x509.Certificate
}

func (app *App) Certificate() (*x509.Certificate, error) {
	if app.certificate == nil {
		block, _ := pem.Decode([]byte(app.Cert))
		if block == nil || block.Type != "Certificate" {
			return nil, fmt.Errorf("invalid pem cert")
		}
		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			app.certificate = cert
		} else {
			return nil, err
		}
	}
	return app.certificate, nil
}

func ListApp(db *sql.DB, skip, limit int) ([]App, error) {
	var apps []App
	if err := dbutil.Query(db, &apps, `select * from apps order by id limit ?,?`, skip, limit); err == nil {
		return apps, nil
	} else {
		return nil, err
	}
}

func InsertApp(db *sql.DB, app *App) error {
	if id, err := dbutil.Insert(db,
		`insert ignore into apps(status, name, description, private_key, cert)
         values(?, ?, ?, ?, ?)`, app.Status, app.Name, app.Description, app.PrivateKey, app.Cert); err == nil {
		app.Id = id
		return nil
	} else {
		return err
	}
}

func GetAppList(db *sql.DB) (apps []App, err error) {
	err = dbutil.Query(db, &apps, `select * from apps`)
	return
}

func GetAppByName(db *sql.DB, name string) (*App, error) {
	var app App
	if err := dbutil.Query(db, &app,
		`select * from apps where name=?`, name); err == nil {
		return &app, nil
	} else if err == sql.ErrNoRows {
		return nil, nil
	} else {
		return nil, err
	}
}

func GetAppGroupByName(db *sql.DB, name string) (*App, []int64, error) {
	row := db.QueryRow(`select apps.id, apps.status, apps.name,
                               apps.description, apps.cert, apps.create_time, apps.modify_time,
                               group_concat(groups.id, ",")
                        from apps
                        left join group_members on group_members.app_id=apps.id
                        left join `+"`groups`"+` on group_members.group_id=groups.id
						where apps.name=?
                        group by apps.id,groups.id`, name)
	var app App
	var groupIds dbutil.NumList
	if err := row.Scan(&app.Id, &app.Status, &app.Name, &app.Description,
		&app.Cert, &app.CreateTime, &app.ModifyTime, &groupIds); err == nil {
		return &app, groupIds, nil
	} else if err == sql.ErrNoRows {
		return nil, nil, nil
	} else {
		return nil, nil, err
	}
}

type Group struct {
	Id          int64     `json:"-"`
	Status      int       `json:"status"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

func InsertGroup(db *sql.DB, group *Group) error {
	if id, err := dbutil.Insert(db,
		`insert into groups(status, name, description)
         values(?, ?, ?)`, group.Status, group.Name, group.Description); err == nil {
		group.Id = id
		return nil
	} else {
		return err
	}
}

func GetGroupList(db *sql.DB) (groups []Group, err error) {
	err = dbutil.Query(db, &groups, `select * from groups`)
	return
}

func GetGroupByName(db *sql.DB, name string) (*Group, error) {
	var group Group
	if err := dbutil.Query(db, &group,
		`select * from groups where name=?`, name); err == nil {
		return &group, nil
	} else if err == sql.ErrNoRows {
		return nil, nil
	} else {
		return nil, err
	}
}

type GroupMember struct {
	Id         int64
	AppId      int64
	GroupId    int64
	CreateTime time.Time
}

func NewGroupMember(db *sql.DB, groupId, appId int64) error {
	_, err := dbutil.Insert(db,
		`insert into group_members(app_id, group_id)
         values(?, ?)`, appId, groupId)
	return err
}

func GetGroupMembers(db *sql.DB, groupId int64) (apps []App, err error) {
	if err := dbutil.Query(db, &apps,
		`select * from apps
         where apps.id in
             (select app_id from group_members
              where group_id=?)`, groupId); err != nil {
		return nil, err
	}
	return
}

const (
	PermTypeConfig  = 0
	PermTypeService = 1
	PermTypeApp     = 2

	PermTargetApp   = 0
	PermTargetGroup = 1

	PermPublicTargetId = 0
)

type Perm struct {
	Id         int64
	PermType   int
	TargetType int
	TargetId   int64
	CanWrite   bool
	Content    string
	CreateTime time.Time
}

func GetPerms(db *sql.DB, typ int, target_type *int, target_id *int64,
	can_write *bool, prefix *string) ([]Perm, error) {
	args := make([]interface{}, 0, 5)
	args = append(args, typ)

	q := `select * from perms where perm_type=?`
	if target_type != nil {
		q += ` and target_type=?`
		args = append(args, *target_type)
	}
	if target_id != nil {
		q += ` and target_id=?`
		args = append(args, *target_id)
	}
	if can_write != nil {
		q += ` and can_write=?`
		args = append(args, *can_write)
	}
	if prefix != nil {
		q += ` and content like ?`
		args = append(args, *prefix+"%")
	}

	var perms []Perm
	if err := dbutil.Query(db, &perms, q, args...); err == nil {
		return perms, nil
	} else {
		return nil, err
	}
}

func InsertPerm(db *sql.DB, perm *Perm) error {
	var query string
	if perm.CanWrite {
		query = `insert into perms(perm_type, target_type, target_id, can_write, content)
                 values(?, ?, ?, ?, ?)
                 on duplicate key update can_write=true`
	} else {
		query = `insert ignore into perms(perm_type, target_type, target_id, can_write, content)
                 values(?, ?, ?, ?, ?)`
	}
	if id, err := dbutil.Insert(db, query, perm.PermType, perm.TargetType, perm.TargetId,
		perm.CanWrite, perm.Content); err == nil {
		perm.Id = id
		return nil
	} else if err == dbutil.ZeroEffected {
		return nil
	} else {
		return err
	}
}

func HasAnyPrefixPerm(db *sql.DB, permType int, appId int64, groupIds []int64, needWrite bool, content string) (bool, error) {
	var extra string
	if needWrite {
		extra = ` and can_write=true`
	}
	var count int64
	var err error
	if appId == PermPublicTargetId {
		err = dbutil.Query(db, &count,
			`select count(*) from perms
             where target_type=? and target_id=? and
                   perm_type=? and ? like CONCAT(content, "%")`+extra,
			PermTargetApp, PermPublicTargetId,
			permType, content)
	} else {
		err = dbutil.Query(db, &count,
			`select count(*) from perms
             where ((target_type=? and target_id in (?)) or
                    (target_type=? and target_id=?) or
                    target_id=?) and
                   perm_type=? and ? like CONCAT(content, "%")`+extra,
			PermTargetGroup, dbutil.NumList(groupIds),
			PermTargetApp, appId,
			PermPublicTargetId,
			permType, content)
	}
	if err == nil {
		return count > 0, nil
	} else {
		return false, err
	}
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

func InsertConfigItem(db *sql.DB, item *ConfigItem) (bool, error) {
	if id, err := dbutil.Insert(db,
		`insert ignore into config_items(name, value, ver)
         values(?,?,?)`, item.Name, item.Value, item.Ver); err == nil {
		item.Id = id
		return true, nil
	} else if err == dbutil.ZeroEffected {
		return false, nil
	} else {
		return false, err
	}
}

func GetConfigItem(db *sql.DB, name string) (*ConfigItem, error) {
	var item ConfigItem
	if err := dbutil.Query(db, &item,
		`select id, name, value, ver, create_time, modify_time
                        from config_items where name=?`, name); err == nil {
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
