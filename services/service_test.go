package services

import (
	"database/sql"
	"os"
	"testing"

	"github.com/golang/glog"
)

func TestWatchServiceDesc(t *testing.T) {
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
