package configs

import (
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"time"
)

type DBConfigItem struct {
	Id         int64     `json:"id"`
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

type ConfigHistory struct {
	Id         int64     `json:"id"`
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	CreateTime time.Time `json:"create_time"`
}

func (ctrl *ConfigCtrl) setDBConfig(name, value string) (rerr error) {
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

	if _, err := tx.Exec(`insert into configs(name,value,create_time,modify_time)
                          values(?,?,now(),now())
                          on duplicate key update value=?, modify_time=now()`,
		name, value, value); err != nil {
		glog.Errorf("insert db config(%s) fail: %v", name, err)
		return utils.NewError(utils.EcodeSystemError, "update db config fail")
	}
	if _, err := tx.Exec(`insert into config_histories(name,value,create_time)
                          values(?,?,now())`, name, value); err != nil {
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

type AppConfigState struct {
	Id         int64     `json:"id"`
	AppId      int64     `json:"app_id"`
	ConfigName string    `json:"config_name"`
	Version    int64     `json:"version"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

func (ctrl *ConfigCtrl) changeAppConfigState(appId int64, configName string, version int64) error {
	if appId <= 0 {
		return nil
	}

	if _, err := ctrl.db.Exec(`insert into app_config_states(app_id,config_name,version,create_time,modify_time)
                            values(?,?,?,now(),now())
                            on duplicate key update version=?,modify_time=now()`,
		appId, configName, version, version); err != nil {
		glog.Errorf("change app(%d) config(%s) state fail: %v", appId, configName, err)
		return utils.NewError(utils.EcodeSystemError, "change app config state fail")
	} else {
		return nil
	}
}
