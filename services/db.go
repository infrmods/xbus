package services

import (
	"fmt"
	"strings"
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
	Extension   string    `json:"extension"`
	Proto       string    `json:"proto"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
}

func (ctrl *ServiceCtrl) updateServiceDBItems(services []ServiceDescV1) error {
	sqlValues := make([]string, 0, len(services))
	values := make([]interface{}, 0, len(services)*7)
	for _, service := range services {
		sqlValues = append(sqlValues, "(?,?,?,?,?,?,?)")
		values = append(values, serviceStatusOk, service.Service, service.Zone, service.Type, service.Extension, service.Proto, service.Description)
	}
	sql := fmt.Sprintf(`insert into services(status, service, zone, typ, extension, proto, description) values %s 
						on duplicate key update status=values(status), typ=values(typ), extension=values(extension),
						proto=values(proto), description=values(description)`,
		strings.Join(sqlValues, ","))
	_, err := ctrl.db.Exec(sql, values...)
	return err
}

// ServiceItemV1 service item v1
type ServiceItemV1 struct {
	Service   string `json:"service"`
	Zone      string `json:"zone"`
	Type      string `json:"type"`
	Extension string `json:"extension,omitempty"`
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
		if rows, err := ctrl.db.Query(`select service, zone, typ, extension from services where status=? and service like ?
				order by modify_time desc limit ?,?`, serviceStatusOk, like, skip, limit); err == nil {
			defer rows.Close()
			for rows.Next() {
				var service, zone, typ, ext string
				if err := rows.Scan(&service, &zone, &typ, &ext); err != nil {
					glog.Errorf("query db services(%s) fail: %v", service, err)
					return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
				}
				result.Services = append(result.Services,
					ServiceItemV1{Service: service, Zone: zone, Type: typ, Extension: ext})
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
