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
	Id          int64     `json:"id"`
	Status      int       `json:"-"`
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	Type        string    `json:"type"`
	Proto       string    `json:"proto"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

func (ctrl *ServiceCtrl) updateServices(services []ServiceDesc) (rerr error) {
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

	for _, desc := range services {
		name, version := desc.Name, desc.Version
		var id int64
		var oldType string
		var oldProto string
		var oldDesc string
		row := tx.QueryRow(`select id, typ, proto from services
			where name=? and version=? for update`, name, version)
		if err := row.Scan(&id, &oldType, &oldProto); err == nil {
			if oldType != desc.Type || oldProto != desc.Proto || oldDesc != desc.Description {
				if !ctrl.config.PermitChangeDesc {
					return utils.Errorf(utils.EcodeChangedServiceDesc,
						"service-desc[%s:%s] can't be change", name, version)
				}
				if _, err := tx.Exec("update services set typ=?, proto=?, description=? where id=?",
					desc.Type, desc.Proto, desc.Description, id); err != nil {
					glog.Errorf("update db service(%s:%s) fail: %v", name, version, err)
					return utils.NewError(utils.EcodeSystemError, "update db service fail")
				}
			}
		} else if err == sql.ErrNoRows {
			if _, err := tx.Exec(`insert into services(status, name, version, typ, proto, 
					description) values(?,?,?,?,?,?)`,
				ServiceStatusOk, name, version, desc.Type, desc.Proto, desc.Description); err != nil {
				glog.Errorf("insert db service(%s:%s) fail: %v", name, version, err)
				return utils.NewError(utils.EcodeSystemError, "insert db service fail")
			}
		} else {
			glog.Errorf("query db service(%s:%s) fail: %v", name, version, err)
			return utils.NewError(utils.EcodeSystemError, "query db service fail")
		}
	}

	if err := tx.Commit(); err == nil {
		return nil
	} else {
		glog.Errorf("update db services fail: %v", err)
		return utils.NewError(utils.EcodeSystemError, "update db service fail")
	}
}

type ServiceItem struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Type    string `json:"type"`
}

type SearchResult struct {
	Services []ServiceItem `json:"services"`
	Total    int64         `json:"total"`
}

func (ctrl *ServiceCtrl) SearchService(name string, skip int64, limit int64) (*SearchResult, error) {
	like := "%" + name + "%"
	var total int64
	if err := dbutil.Query(ctrl.db, &total, `select count(*) from services where name like ?`, like); err != nil {
		glog.Errorf("query db services(%s) fail: %v", name, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}

	result := SearchResult{Services: make([]ServiceItem, 0, limit), Total: total}
	if total > skip {
		if rows, err := ctrl.db.Query(`select name, version, typ from services where name like ?
				order by modify_time desc limit ?,?`, like, skip, limit); err == nil {
			defer rows.Close()
			for rows.Next() {
				var name, version, typ string
				if err := rows.Scan(&name, &version, &typ); err != nil {
					glog.Errorf("query db services(%s) fail: %v", name, err)
					return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
				}
				result.Services = append(result.Services,
					ServiceItem{Name: name, Version: version, Type: typ})
			}
		} else {
			glog.Errorf("query db services(%s) fail: %v", name, err)
			return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
		}
	}
	return &result, nil
}
