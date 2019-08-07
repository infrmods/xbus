package configs

import (
	"database/sql"
	"time"

	"github.com/gocomm/dbutil"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

const (
	// ConfigStatusOk config status ok
	ConfigStatusOk = 0
	// ConfigStatusDeleted config status deleted
	ConfigStatusDeleted = -1
)

// DBConfigItem db config table
type DBConfigItem struct {
	ID         int64     `json:"id"`
	Status     int       `json:"-"`
	Tag        string    `json:"tag"`
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

// GetDBConfig get db config
func GetDBConfig(db *sql.DB, name string) (*DBConfigItem, error) {
	var item DBConfigItem
	if err := dbutil.Query(db, &item, `select * from configs where
			status=? and name=?`, ConfigStatusOk, name); err == nil {
		return &item, nil
	} else if err == sql.ErrNoRows {
		return nil, nil
	} else {
		return nil, err
	}
}

// GetDBConfigCount get db config count
func GetDBConfigCount(db *sql.DB, tag, prefix string) (int64, error) {
	args := make([]interface{}, 0, 3)
	q := `select count(*) from configs where status=?`
	args = append(args, ConfigStatusOk)

	if tag != "" {
		q += ` and tag = ?`
		args = append(args, tag)
	}
	if prefix != "" {
		q += ` and name like ?`
		args = append(args, prefix+"%")
	}

	var count int64
	err := dbutil.Query(db, &count, q, args...)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// ConfigInfo config info
type ConfigInfo struct {
	Tag        *string   `json:"tag"`
	Name       string    `json:"name"`
	ModifyTime time.Time `json:"modify_time"`
}

// ListDBConfigs list db configs
func ListDBConfigs(db *sql.DB, tag, prefix string, skip, limit int) ([]ConfigInfo, error) {
	args := make([]interface{}, 0, 3)
	q := `select tag,name,modify_time from configs where status=?`
	args = append(args, ConfigStatusOk)
	if tag != "" {
		q += ` and tag = ?`
		args = append(args, tag)
	}
	if prefix != "" {
		q += ` and name like ?`
		args = append(args, prefix+"%")
	}
	q += ` order by modify_time desc limit ?,?`
	args = append(args, skip)
	args = append(args, limit)

	var items []ConfigInfo
	err := dbutil.Query(db, &items, q, args...)
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ConfigHistory config history
type ConfigHistory struct {
	ID         int64     `json:"id"`
	Tag        string    `json:"tag"`
	Name       string    `json:"name"`
	AppID      int64     `json:"modified_by"`
	Remark     string    `json:"remark"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
}

func (ctrl *ConfigCtrl) setDBConfig(tag, name string, appID int64, remark, value string) (rerr error) {
	tx, err := ctrl.db.Begin()
	if err != nil {
		glog.Errorf("new db tx fail: %v", err)
		return utils.NewError(utils.EcodeSystemError, "new db tx fail")
	}

	defer func() {
		if rerr != nil {
			if err := tx.Rollback(); err != nil {
				glog.Warningf("tx roolback fail: %v", err)
			}
		}
	}()

	var tagV sql.NullString
	if tag != "" {
		tagV.Valid = true
		tagV.String = tag
	}

	if _, err := tx.Exec(`insert into configs(status,tag,name,value,create_time,modify_time)
                          values(?,?,?,?,now(),now())
                          on duplicate key update status=?, tag=?, value=?, modify_time=now()`,
		ConfigStatusOk, tagV, name, value, ConfigStatusOk, tagV, value); err != nil {
		glog.Errorf("insert db config(%s) fail: %v", name, err)
		return utils.NewError(utils.EcodeSystemError, "update db config fail")
	}
	if _, err := tx.Exec(`insert into config_histories(tag, name,app_id,remark,value,create_time)
                          values(?,?,?,?,?,now())`, tagV, name, appID, remark, value); err != nil {
		glog.Errorf("insert db config history fail: %v", err)
		return utils.NewError(utils.EcodeSystemError, "insert db config history fail")
	}

	err = tx.Commit()
	if err != nil {
		glog.Errorf("set db config(%s), commit fail: %v", name, err)
		return utils.NewError(utils.EcodeSystemError, "commit db fail")
	}
	return nil
}

func (ctrl *ConfigCtrl) deleteDBConfig(name string) error {
	if _, err := ctrl.db.Exec(`update configs set status=? where name=?`, ConfigStatusDeleted, name); err != nil {
		return utils.NewError(utils.EcodeSystemError, "delete config fail")
	}
	return nil
}

// AppConfigState app config state table
type AppConfigState struct {
	ID         int64     `json:"id"`
	AppID      int64     `json:"app_id"`
	AppNode    string    `json:"app_node"`
	ConfigName string    `json:"config_name"`
	Version    int64     `json:"version"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

func (ctrl *ConfigCtrl) changeAppConfigState(appID int64, appNode, configName string, version int64) error {
	if appID <= 0 {
		return nil
	}
	_, err := ctrl.db.Exec(`insert into app_config_states(app_id,app_node,config_name,version,create_time,modify_time)
                            values(?,?,?,?,now(),now())
                            on duplicate key update version=?`,
		appID, appNode, configName, version, version)
	if err != nil {
		glog.Errorf("change app(%d - %s) config(%s) state(ver: %d) fail: %v", appID, appNode, configName, version, err)
		return utils.NewError(utils.EcodeSystemError, "change app config state fail")
	}
	return nil
}
