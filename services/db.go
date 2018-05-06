package services

import (
	"database/sql"
	"github.com/gocomm/dbutil"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"time"
)

const (
	ServiceStatusOk      = 0
	ServiceStatusDeleted = -1
)

type DBServiceItem struct {
	Id         int64     `json:"id"`
	Status     int       `json:"-"`
	Name       string    `json:"name"`
	Version    string    `json:"version"`
	Proto      string    `json:"proto"`
	CreateTime time.Time `json:"create_time"`
	ModifyTime time.Time `json:"modify_time"`
}

func (ctrl *ServiceCtrl) updateService(name, version, proto string, canChangeProto bool) (rerr error) {
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

	if rows, err := tx.Query(`select id, proto from services for update
			where name=? and version=?`, name, version); err == nil {
		defer rows.Close()
		var id int64
		var oldProto string
		if err := rows.Scan(&id, &oldProto); err != nil {
			glog.Errorf("query db service(%s:%s) fail: %v", name, version, err)
			return utils.NewError(utils.EcodeSystemError, "query db service fail")
		}
		if oldProto != proto {
			if !canChangeProto {
				return utils.Errorf(utils.EcodeChangedServiceDesc,
					"service-desc[%s:%s] can't be change", name, version)
			}
			if _, err := tx.Exec("update services set proto=? where id=?", proto, id); err != nil {
				glog.Errorf("update db service(%s:%s) fail: %v", name, version, err)
				return utils.NewError(utils.EcodeSystemError, "update db service fail")
			}
		} else {
			if err := tx.Rollback(); err != nil {
				glog.Warningf("tx roolback fail: %v", err)
			}
			return nil
		}
	} else if err == sql.ErrNoRows {
		if _, err := tx.Exec("insert into services(status, name, version, proto) values(?,?,?,?)",
			ServiceStatusOk, name, version, proto); err != nil {
			glog.Errorf("insert db service(%s:%s) fail: %v", name, version, err)
			return utils.NewError(utils.EcodeSystemError, "insert db service fail")
		}
	} else {
		glog.Errorf("query db service(%s:%s) fail: %v", name, version, err)
		return utils.NewError(utils.EcodeSystemError, "query db service fail")
	}

	if err := tx.Commit(); err == nil {
		return nil
	} else {
		glog.Errorf("update db service(%s:%s) fail: %v", name, version, err)
		return utils.NewError(utils.EcodeSystemError, "update db service fail")
	}
}

type ServiceItem struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type SearchResult struct {
	Services []ServiceItem `json:"services"`
	Total    int           `json:"total"`
}

func (ctrl *ServiceCtrl) SearchService(name string, skip int, limit int) (*SearchResult, error) {
	like := "%" + name + "%"
	var total int
	if err := dbutil.Query(ctrl.db, `select count(*) from services where name like ?`, like); err != nil {
		glog.Errorf("query db services(%s) fail: %v", name, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}

	result := SearchResult{Services: make([]ServiceItem, 0, limit), Total: total}
	if total > skip {
		if rows, err := ctrl.db.Query(`select name, version from services where name like ?`, like); err == nil {
			defer rows.Close()
			for rows.Next() {
				var name, version string
				if err := rows.Scan(&name, &version); err != nil {
					glog.Errorf("query db services(%s) fail: %v", name, err)
					return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
				}
				result.Services = append(result.Services, ServiceItem{Name: name, Version: version})
			}
		} else {
			glog.Errorf("query db services(%s) fail: %v", name, err)
			return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
		}
	}
	return &result, nil
}
