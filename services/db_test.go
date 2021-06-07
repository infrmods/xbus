package services

import (
	"database/sql"
	"os"
	"testing"

	"github.com/golang/glog"

	_ "github.com/go-sql-driver/mysql"
)

func TestSearchOnlyBymd5s(t *testing.T) {
	db, err := sql.Open("mysql", "qa:NTQ0NjU5YjU0@tcp(rm-ks-qa.mysql.rds.aliyuncs.com:3306)/xbus?parseTime=true")
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	db.SetMaxOpenConns(50)
	services := &ServiceCtrl{config: Config{}, db: db, etcdClient: nil, ProtoSwitch: false}
	md5s := []string{"ce532e516c1a71c1dc31059130f09a13", "fd8122357b56cce94f2bdbeed9bb35a5"}
	result, err := services.SearchOnlyBymd5s(md5s)
	if err != nil {
		t.Errorf("fail: %v", err)
	} else {
		t.Logf("result %v", result)
	}
}

func TestSearchService(t *testing.T) {
	db, err := sql.Open("mysql", "qa:NTQ0NjU5YjU0@tcp(rm-ks-qa.mysql.rds.aliyuncs.com:3306)/xbus?parseTime=true")
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	db.SetMaxOpenConns(1)
	services := &ServiceCtrl{config: Config{}, db: db, etcdClient: nil, ProtoSwitch: false}
	zone := "default"
	service := "sktest.Java5:1.0"
	result, err := services.SearchByServiceZone(service, zone)
	if err != nil {
		t.Errorf("fail: %v", err)
	} else {
		t.Logf("result %v", result)
	}
}

func TestUpdateServiceDBItems(t *testing.T) {
	db, err := sql.Open("mysql", "qa:NTQ0NjU5YjU0@tcp(rm-ks-qa.mysql.rds.aliyuncs.com:3306)/xbus?parseTime=true")
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	db.SetMaxOpenConns(50)
	serviceDescs := []ServiceDescV1{
		{
			Service: "sktest.ab:1.0",
			Zone:    "default",
			Type:    "service-kit",
			Proto:   "types: {} service: hello: params: [] ",
			Md5:     "489eb5ee96fa6940ea1df133a81d91148",
		},
	}
	services := &ServiceCtrl{config: Config{}, db: db, etcdClient: nil, ProtoSwitch: false}
	err = services.updateServiceDBItems(serviceDescs)
	if err != nil {
		t.Errorf("fail: %v", err)
	} else {
		t.Logf("result success")
	}
}
