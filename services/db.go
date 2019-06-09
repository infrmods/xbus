package services

import (
	"database/sql"
	"time"

	"github.com/gocomm/dbutil"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

const (
	serviceStatusOk      = 0
	serviceStatusDeleted = -1
)

// DBServiceItemV1 service table
type DBServiceItemV1 struct {
	ID          int64     `json:"id"`
	Status      int       `json:"-"`
	Service     string    `json:"service"`
	Zone        string    `json:"zone"`
	Type        string    `json:"type"`
	Proto       string    `json:"proto"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

func (ctrl *ServiceCtrl) updateServiceDBItems(services []ServiceDescV1) (rerr error) {
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
		var id int64
		var status int
		var oldType string
		var oldProto string
		var oldDesc string
		row := tx.QueryRow(`select id, status, typ, proto from services
			where service=? and zone=? for update`, desc.Service, desc.Zone)
		if err := row.Scan(&id, &status, &oldType, &oldProto); err == nil {
			if status != serviceStatusOk || oldType != desc.Type || oldProto != desc.Proto || oldDesc != desc.Description {
				if !ctrl.config.PermitChangeDesc {
					return utils.Errorf(utils.EcodeChangedServiceDesc,
						"service-desc[%s] can't be change", desc.Service)
				}
				if _, err := tx.Exec("update services set status=?, typ=?, proto=?, description=? where id=?",
					serviceStatusOk, desc.Type, desc.Proto, desc.Description, id); err != nil {
					glog.Errorf("update db service(%s:%s) fail: %v", desc.Service, desc.Zone, err)
					return utils.NewError(utils.EcodeSystemError, "update db service fail")
				}
			}
		} else if err == sql.ErrNoRows {
			if _, err := tx.Exec(`insert into services(status, service, zone, typ, proto, 
					description) values(?,?,?,?,?,?)`,
				serviceStatusOk, desc.Service, desc.Zone, desc.Type, desc.Proto, desc.Description); err != nil {
				glog.Errorf("insert db service(%s:%s) fail: %v", desc.Service, desc.Zone, err)
				return utils.NewError(utils.EcodeSystemError, "insert db service fail")
			}
		} else {
			glog.Errorf("query db service(%s:%s) fail: %v", desc.Service, desc.Zone, err)
			return utils.NewError(utils.EcodeSystemError, "query db service fail")
		}
	}

	if err := tx.Commit(); err != nil {
		glog.Errorf("update db services fail: %v", err)
		return utils.NewError(utils.EcodeSystemError, "update db service fail")
	}
	return nil
}

// ServiceItemV1 service item v1
type ServiceItemV1 struct {
	Service string `json:"service"`
	Zone    string `json:"zone"`
	Type    string `json:"type"`
}

// SearchResultV1 search result
type SearchResultV1 struct {
	Services []ServiceItemV1 `json:"services"`
	Total    int64           `json:"total"`
}

// SearchService search service via db
func (ctrl *ServiceCtrl) SearchService(service string, skip int64, limit int64) (*SearchResultV1, error) {
	like := "%" + service + "%"
	var total int64
	if err := dbutil.Query(ctrl.db, &total, `select count(*) from services where status=? and service like ?`, serviceStatusOk, like); err != nil {
		glog.Errorf("query db services(%s) fail: %v", service, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}

	result := SearchResultV1{Services: make([]ServiceItemV1, 0, limit), Total: total}
	if total > skip {
		if rows, err := ctrl.db.Query(`select service, zone, typ from services where status=? and service like ?
				order by modify_time desc limit ?,?`, serviceStatusOk, like, skip, limit); err == nil {
			defer rows.Close()
			for rows.Next() {
				var service, zone, typ string
				if err := rows.Scan(&service, &zone, &typ); err != nil {
					glog.Errorf("query db services(%s) fail: %v", service, err)
					return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
				}
				result.Services = append(result.Services,
					ServiceItemV1{Service: service, Zone: zone, Type: typ})
			}
		} else {
			glog.Errorf("query db services(%s) fail: %v", service, err)
			return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
		}
	}
	return &result, nil
}

func (ctrl *ServiceCtrl) deleteServiceDBItems(service, zone string) (err error) {
	if zone == "" {
		_, err = ctrl.db.Exec(`update services set status=? where service=?`, serviceStatusDeleted, service)
	} else {
		_, err = ctrl.db.Exec(`update services set status=? where service=? and zone=?`, serviceStatusDeleted, service, zone)
	}
	return
}
