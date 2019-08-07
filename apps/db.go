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

// App app table
type App struct {
	ID          int64     `json:"-"`
	Status      int       `json:"status"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	PrivateKey  string    `json:"-"`
	Cert        string    `json:"cert"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`

	certificate *x509.Certificate
}

// Certificate cert
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

// ListApp list app
func ListApp(db *sql.DB, skip, limit int) ([]App, error) {
	var apps []App
	err := dbutil.Query(db, &apps, `select * from apps order by id limit ?,?`, skip, limit)
	if err != nil {
		return nil, err
	}
	return apps, nil
}

// InsertApp insert app
func InsertApp(db *sql.DB, app *App) error {
	id, err := dbutil.Insert(db,
		`insert ignore into apps(status, name, description, private_key, cert)
         values(?, ?, ?, ?, ?)`, app.Status, app.Name, app.Description, app.PrivateKey, app.Cert)
	if err != nil {
		return err
	}
	app.ID = id
	return nil
}

// GetAppList get app list
func GetAppList(db *sql.DB) (apps []App, err error) {
	err = dbutil.Query(db, &apps, `select * from apps`)
	return
}

// GetAppByName get app by name
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

// GetAppGroupByName get app group by name
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
	var groupIDs dbutil.NumList
	if err := row.Scan(&app.ID, &app.Status, &app.Name, &app.Description,
		&app.Cert, &app.CreateTime, &app.ModifyTime, &groupIDs); err == nil {
		return &app, groupIDs, nil
	} else if err == sql.ErrNoRows {
		return nil, nil, nil
	} else {
		return nil, nil, err
	}
}

// Group group table
type Group struct {
	ID          int64     `json:"-"`
	Status      int       `json:"status"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

// InsertGroup insert group
func InsertGroup(db *sql.DB, group *Group) error {
	id, err := dbutil.Insert(db,
		`insert into groups(status, name, description)
         values(?, ?, ?)`, group.Status, group.Name, group.Description)
	if err != nil {
		return err
	}
	group.ID = id
	return nil
}

// GetGroupList get group list
func GetGroupList(db *sql.DB) (groups []Group, err error) {
	err = dbutil.Query(db, &groups, `select * from groups`)
	return
}

// GetGroupByName get group by name
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

// GroupMember group member
type GroupMember struct {
	ID         int64
	AppID      int64
	GroupID    int64
	CreateTime time.Time
}

// NewGroupMember new group member
func NewGroupMember(db *sql.DB, groupID, appID int64) error {
	_, err := dbutil.Insert(db,
		`insert into group_members(app_id, group_id)
         values(?, ?)`, appID, groupID)
	return err
}

// GetGroupMembers get group members
func GetGroupMembers(db *sql.DB, groupID int64) (apps []App, err error) {
	if err := dbutil.Query(db, &apps,
		`select * from apps
         where apps.id in
             (select app_id from group_members
              where group_id=?)`, groupID); err != nil {
		return nil, err
	}
	return
}

const (
	// PermTypeConfig perm type config
	PermTypeConfig = 0
	// PermTypeService perm type service
	PermTypeService = 1
	// PermTypeApp perm type app
	PermTypeApp = 2

	// PermTargetApp perm target app
	PermTargetApp = 0
	// PermTargetGroup perm target group
	PermTargetGroup = 1

	// PermPublicTargetID perm public target id
	PermPublicTargetID = 0
)

// Perm perm table
type Perm struct {
	ID         int64
	PermType   int
	TargetType int
	TargetID   int64
	CanWrite   bool
	Content    string
	CreateTime time.Time
}

// GetPerms get perms
func GetPerms(db *sql.DB, typ int, targetType *int, targetID *int64,
	canWrite *bool, prefix *string) ([]Perm, error) {
	args := make([]interface{}, 0, 5)
	args = append(args, typ)

	q := `select * from perms where perm_type=?`
	if targetType != nil {
		q += ` and targetType=?`
		args = append(args, *targetType)
	}
	if targetID != nil {
		q += ` and targetID=?`
		args = append(args, *targetID)
	}
	if canWrite != nil {
		q += ` and canWrite=?`
		args = append(args, *canWrite)
	}
	if prefix != nil {
		q += ` and content like ?`
		args = append(args, *prefix+"%")
	}

	var perms []Perm
	err := dbutil.Query(db, &perms, q, args...)
	if err != nil {
		return nil, err
	}
	return perms, nil
}

// InsertPerm insert perm
func InsertPerm(db *sql.DB, perm *Perm) error {
	var query string
	if perm.CanWrite {
		query = `insert into perms(perm_type, targetType, targetID, canWrite, content)
                 values(?, ?, ?, ?, ?)
                 on duplicate key update canWrite=true`
	} else {
		query = `insert ignore into perms(perm_type, targetType, targetID, canWrite, content)
                 values(?, ?, ?, ?, ?)`
	}
	if id, err := dbutil.Insert(db, query, perm.PermType, perm.TargetType, perm.TargetID,
		perm.CanWrite, perm.Content); err == nil {
		perm.ID = id
		return nil
	} else if err == dbutil.ZeroEffected {
		return nil
	} else {
		return err
	}
}

// HasAnyPrefixPerm has any prefix perm
func HasAnyPrefixPerm(db *sql.DB, permType int, appID int64, groupIDs []int64, needWrite bool, content string) (bool, error) {
	var extra string
	if needWrite {
		extra = ` and canWrite=true`
	}
	var count int64
	var err error
	if appID == PermPublicTargetID {
		err = dbutil.Query(db, &count,
			`select count(*) from perms
             where targetType=? and targetID=? and
                   perm_type=? and ? like CONCAT(content, "%")`+extra,
			PermTargetApp, PermPublicTargetID,
			permType, content)
	} else {
		err = dbutil.Query(db, &count,
			`select count(*) from perms
             where ((targetType=? and targetID in (?)) or
                    (targetType=? and targetID=?) or
                    targetID=?) and
                   perm_type=? and ? like CONCAT(content, "%")`+extra,
			PermTargetGroup, dbutil.NumList(groupIDs),
			PermTargetApp, appID,
			PermPublicTargetID,
			permType, content)
	}
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ConfigItem config item table
type ConfigItem struct {
	ID         int64
	Name       string
	Value      string
	Ver        int64
	CreateTime time.Time
	ModifyTime time.Time
}

// GetIntValue get int value
func (m *ConfigItem) GetIntValue() (int64, error) {
	return strconv.ParseInt(m.Value, 10, 64)
}

// SetIntValue set int value
func (m *ConfigItem) SetIntValue(n int64) {
	m.Value = strconv.FormatInt(n, 10)
}

// InsertConfigItem insert config item
func InsertConfigItem(db *sql.DB, item *ConfigItem) (bool, error) {
	if id, err := dbutil.Insert(db,
		`insert ignore into config_items(name, value, ver)
         values(?,?,?)`, item.Name, item.Value, item.Ver); err == nil {
		item.ID = id
		return true, nil
	} else if err == dbutil.ZeroEffected {
		return false, nil
	} else {
		return false, err
	}
}

// GetConfigItem get config item
func GetConfigItem(db *sql.DB, name string) (*ConfigItem, error) {
	var item ConfigItem
	err := dbutil.Query(db, &item,
		`select id, name, value, ver, create_time, modify_time
                        from config_items where name=?`, name)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// Refresh refresh
func (m *ConfigItem) Refresh(db *sql.DB) error {
	row := db.QueryRow(`select name, value, ver, create_time, modify_time
                        from config_items where id=?`, m.ID)
	return row.Scan(&m.Name, &m.Value,
		&m.Ver, &m.CreateTime, &m.ModifyTime)
}

// UpdateValue update value
func (m *ConfigItem) UpdateValue(db *sql.DB) error {
	_, err := dbutil.Update(db, `update config_items set value=?, ver=ver+1
                                 where id=? and ver=?`,
		m.Value, m.ID, m.Ver)
	return err
}
