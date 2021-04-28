package services

import (
	"bytes"
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
	Proto       string    `json:"proto"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
	ModifyTime  time.Time `json:"modify_time"`
	Md5         string    `json:"proto_md5"`
	Md5Status   int       `json:"md5_status"`
}

func (ctrl *ServiceCtrl) updateServiceDBItems(services []ServiceDescV1) error {
	if len(services) == 0 {
		return nil
	}

	sqlValues := make([]string, 0, len(services))
	values := make([]interface{}, 0, len(services)*9)
	for i := range services {
		service := &services[i]
		sqlValues = append(sqlValues, "(?,?,?,?,?,?,?,?)")
		values = append(values, serviceStatusOk, service.Service, service.Zone, service.Type, service.Proto, service.Description, service.Md5, 1)
	}
	sql := fmt.Sprintf(`insert into services(status, service, zone, typ, proto, description, proto_md5, md5_status) values %s 
						on duplicate key update status=values(status), typ=values(typ),
						proto=values(proto), description=values(description), proto_md5=values(proto_md5), md5_status=0 `,
		strings.Join(sqlValues, ","))
	_, err := ctrl.db.Exec(sql, values...)
	return err
}

func (ctrl *ServiceCtrl) updateServiceDBItemsCommit(services []ServiceDescV1) error {
	if len(services) == 0 {
		return nil
	}
	values := make([]string, 0, len(services))
	for i := range services {
		service := services[i]
		values = append(values, "'"+service.Md5+"'")
	}
	sql := fmt.Sprintf(`update services set md5_status = 1 where proto_md5 in (%s)`,
		strings.Join(values, ","))
	_, err := ctrl.db.Exec(sql)
	return err
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
	if limit-skip > 5000 {
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail: count more than 5000!")
	}
	like := "%" + service + "%"
	var total int64
	if err := dbutil.Query(ctrl.db, &total, `select count(*) from services where status=? and md5_status=1 and service like ?`, serviceStatusOk, like); err != nil {
		glog.Errorf("query db services(%s) fail: %v", service, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}

	result := SearchResultV1{Services: make([]ServiceItemV1, 0, limit), Total: total}
	if total > skip {
		if rows, err := ctrl.db.Query(`select service, zone, typ from services where status=? and md5_status=1 and service like ?
				order by create_time desc limit ?,?`, serviceStatusOk, like, skip, limit); err == nil {
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

// SearchBymd5 search ServiceDescV1 via db
func (ctrl *ServiceCtrl) SearchBymd5(service, zone string) (*ServiceDescV1, error) {
	if rows, err := ctrl.db.Query(`select service, zone, typ, proto, description, proto_md5 from services where zone = ? 
					and service = ? order by modify_time desc limit 0,1`, zone, service); err == nil {
		defer rows.Close()
		for rows.Next() {
			var service, zone, typ, proto, description, md5 string
			if err := rows.Scan(&service, &zone, &typ, &proto, &description, &md5); err != nil {
				glog.Errorf("query db services(%s %s) fail: %v", service, zone, err)
				return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
			}
			return &ServiceDescV1{
					Service:     service,
					Zone:        zone,
					Type:        typ,
					Proto:       proto,
					Description: description,
					Md5:         md5},
				nil
		}
	} else {
		glog.Errorf("query db services(%s %s) fail: %v", service, zone, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}
	return nil, nil
}

// SearchOnlyBymd5s search ServiceDescV1 via db
func (ctrl *ServiceCtrl) SearchOnlyBymd5s(md5s []string) ([]ServiceDescV1, error) {
	results := make([]ServiceDescV1, 0)
	if md5s == nil || len(md5s) == 0 {
		return results, nil
	}
	bs := bytes.NewBufferString("")
	for i := 0; i < len(md5s); i++ {
		if i == 0 {
			bs.WriteString("'")
			bs.WriteString(md5s[i])
			bs.WriteString("'")
		} else {
			bs.WriteString(",'")
			bs.WriteString(md5s[i])
			bs.WriteString("'")
		}
	}
	br := bs.String()
	if rows, err := ctrl.db.Query(
		fmt.Sprintf("select service, zone, typ, proto, description, proto_md5 from services where proto_md5 in (%s) ", br)); err == nil {
		defer rows.Close()
		for rows.Next() {
			var service, zone, typ, proto, description, md5 string
			if err := rows.Scan(&service, &zone, &typ, &proto, &description, &md5); err != nil {
				glog.Errorf("query db services(%s) fail: %v", service, err)
				return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
			}
			results = append(results, ServiceDescV1{
				Service:     service,
				Zone:        zone,
				Type:        typ,
				Proto:       proto,
				Description: description,
				Md5:         md5})
		}
		return results, nil
	} else {
		glog.Errorf("query db services(%v) fail: %v", md5s, err)
		return nil, utils.NewError(utils.EcodeSystemError, "query db services fail")
	}
}

func (ctrl *ServiceCtrl) deleteServiceDBItems(service, zone string) (err error) {
	if zone == "" {
		_, err = ctrl.db.Exec(`update services set status=? where service=?`, serviceStatusDeleted, service)
	} else {
		_, err = ctrl.db.Exec(`update services set status=? where service=? and zone=?`, serviceStatusDeleted, service, zone)
	}
	return
}

func (ctrl *ServiceCtrl) updateServiceDBItemsBack(services []ServiceDescV1) error {
	if len(services) == 0 {
		return nil
	}

	sqlValues := make([]string, 0, len(services))
	values := make([]interface{}, 0, len(services)*7)
	for _, service := range services {
		sqlValues = append(sqlValues, "(?,?,?,?,?,?)")
		values = append(values, serviceStatusOk, service.Service, service.Zone, service.Type, service.Proto, service.Description)
	}
	sql := fmt.Sprintf(`insert into services(status, service, zone, typ, proto, description) values %s 
						on duplicate key update status=values(status), typ=values(typ),
						proto=values(proto), description=values(description)`,
		strings.Join(sqlValues, ","))
	_, err := ctrl.db.Exec(sql, values...)
	return err
}
