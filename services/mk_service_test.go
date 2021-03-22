package services

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

func TestMakeService(t *testing.T) {
	db, err := sql.Open("mysql", "qa:NTQ0NjU5YjU0@tcp(rm-ks-qa.mysql.rds.aliyuncs.com:3306)/xbus?parseTime=true")
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	config := utils.ETCDConfig{Endpoints: []string{"10.1.1.12:2379"}}
	etcdConfig := clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: time.Duration(5) * time.Second}
	client, err := clientv3.New(etcdConfig)
	if err != nil {
		t.Errorf("create etcd clientv3 fail: %v", err)
		os.Exit(-1)
	}
	services := &ServiceCtrl{config: Config{NetMappings: []NetMapping{}, KeyPrefix: "/services"}, db: db, etcdClient: client, ProtoSwitch: false}
	go func() {
		time.Sleep(time.Duration(5) * time.Second)
		client.Put(context.TODO(), "/services-md5s/default/sktest.A:1.0.0", "112345456767788")
	}()

	result, err := services.WatchServiceDesc(context.TODO(), "default", 479000)
	if err != nil {
		t.Errorf("%v", err)
	} else {
		t.Logf("%v", result)
	}

}
