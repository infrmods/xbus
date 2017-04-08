package configs

import (
	"database/sql"
	"github.com/gocomm/dbutil"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"time"
)

const (
	ConfigStatusOk      = 0
	ConfigStatusDeleted = -1
)

type DBConfigItem struct {
	Id         int64     `json:"id"`
	Status     int       `json:"-"`
	Tag        string    `json:"tag"`
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

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
	if err := dbutil.Query(db, &count, q, args...); err == nil {
		return count, nil
	} else {
		return 0, err
	}
}

type ConfigInfo struct {
	Tag        *string   `json:"tag"`
	Name       string    `json:"name"`
	ModifyTime time.Time `json:"modify_time"`
}

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
	if err := dbutil.Query(db, &items, q, args...); err == nil {
		return items, nil
	} else {
		return nil, err
	}
}

type ConfigHistory struct {
	Id         int64     `json:"id"`
	Tag        string    `json:"tag"`
	Name       string    `json:"name"`
	AppId      int64     `json:"modified_by"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
}

func (ctrl *ConfigCtrl) setDBConfig(tag, name string, appId int64, value string) (rerr error) {
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

	if _, err := tx.Exec(`insert into configs(status,tag,name,value,create_time,modify_time)
                          values(?,?,?,?,now(),now())
                          on duplicate key update status=?, tag=?, value=?, modify_time=now()`,
		ConfigStatusOk, tag, name, value, ConfigStatusOk, tag, value); err != nil {
		glog.Errorf("insert db config(%s) fail: %v", name, err)
		return utils.NewError(utils.EcodeSystemError, "update db config fail")
	}
	if _, err := tx.Exec(`insert into config_histories(tag, name,app_id,value,create_time)
                          values(?,?,?,?,now())`, tag, name, appId, value); err != nil {
		glog.Errorf("insert db config history fail: %v", err)
		return utils.NewError(utils.EcodeSystemError, "insert db config history fail")
	}

	if err := tx.Commit(); err == nil {
		return nil
	} else {
		glog.Errorf("set db config(%s), commit fail: %v", name, err)
		return utils.NewError(utils.EcodeSystemError, "commit db fail")
	}
}

func (ctrl *ConfigCtrl) deleteDBConfig(name string) error {
	if _, err := ctrl.db.Exec(`update configs set status=? where name=?`, ConfigStatusDeleted, name); err != nil {
		return utils.NewError(utils.EcodeSystemError, "delete config fail")
	}
	return nil
}

type AppConfigState struct {
	Id         int64     `json:"id"`
	AppId      int64     `json:"app_id"`
	AppNode    string    `json:"app_node"`
	ConfigName string    `json:"config_name"`
	Version    int64     `json:"version"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

func (ctrl *ConfigCtrl) changeAppConfigState(appId int64, appNode, configName string, version int64) error {
	if appId <= 0 {
		return nil
	}

	if _, err := ctrl.db.Exec(`insert into app_config_states(app_id,app_node,config_name,version,create_time,modify_time)
                            values(?,?,?,?,now(),now())
                            on duplicate key update version=?`,
		appId, appNode, configName, version, version); err != nil {
		glog.Errorf("change app(%d - %s) config(%s) state(ver: %d) fail: %v", appId, appNode, configName, version, err)
		return utils.NewError(utils.EcodeSystemError, "change app config state fail")
	} else {
		return nil
	}
}
