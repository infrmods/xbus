package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/services"
	"io"
	"regexp"
	"strings"
)

var rServiceSplit = regexp.MustCompile(`/(.+)/(.+)/(.+)$`)

const serviceDescNodeKey = "desc"

// FixCmd run cmd
type ConsistencyFixCmd struct {
}

// Name cmd name
func (cmd *ConsistencyFixCmd) Name() string {
	return "consistency"
}

// Synopsis cmd synopsis
func (cmd *ConsistencyFixCmd) Synopsis() string {
	return "consistency fix proto"
}

// SetFlags cmd set flags
func (cmd *ConsistencyFixCmd) SetFlags(f *flag.FlagSet) {
}

// Usage cmd usgae
func (cmd *ConsistencyFixCmd) Usage() string {
	return ""
}

func (cmd *ConsistencyFixCmd) serviceM5NotifyKey(service, zone string) string {
	return fmt.Sprintf("/services-md5s/%s/%s", zone, service)
}

func (cmd *ConsistencyFixCmd) action(db *sql.DB, etcdClient *clientv3.Client, action string) subcommands.ExitStatus {
	start := 0
	total := -1
	if rows, err := db.Query(`select count(1) from services`); err == nil {
		for rows.Next() {
			if err := rows.Scan(&total); err != nil {
				glog.Errorf("get services count from db failed: %v", err)
				return subcommands.ExitSuccess
			}
		}
	} else {
		glog.Errorf("get services count from db failed: %v", err)
		return subcommands.ExitSuccess
	}
	println(total)
	dbServiceMap := make(map[string]string)
	for start <= total {
		if rows, err := db.Query(`select service, zone, typ, proto, description, proto_md5, status, md5_status from services
				order by create_time  desc limit ?,?`, start, 1000); err == nil {
			for rows.Next() {
				var service, zone, typ, proto, description, md5Str, status, md5Status string
				if err := rows.Scan(&service, &zone, &typ, &proto, &description, &md5Str, &status, &md5Status); err != nil {
					glog.Errorf("get service data to variable failed: %v", err)
					return subcommands.ExitSuccess
				}
				currentService := fmt.Sprintf("/services/%s/%s/desc", service, zone)
				dbServiceMap[currentService] = status + "$" + md5Status
			}
		} else {
			glog.Errorf("select data from services failed: %v", err)
			return subcommands.ExitSuccess
		}
		start += 1000
	}

	resp, err := etcdClient.Get(context.Background(), "/services/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		glog.Errorf("get services from etcd failed: %v", err)
		return subcommands.ExitSuccess
	}
	descMap := make(map[string]string)
	for _, kv := range resp.Kvs {
		//glog.Infof("key: %s \n %s", string(kv.Key), string(kv.Value))
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		suffix := matches[0][3]
		if suffix != serviceDescNodeKey {
			continue
		}
		descMap[string(kv.Key)] = ""

		//打印etcd存在的数据，数据库不存在
		if value, ok := dbServiceMap[string(kv.Key)]; "print" == action && !ok {
			glog.Infof("db not exists: %s %s", string(kv.Key), value)
			continue
		}

		//修复，etcd存在的数据，数据库不存在，将etcd数据入库
		if value, ok := dbServiceMap[string(kv.Key)]; "fix" == action && !ok {
			serviceDescKey := string(kv.Key)
			glog.Infof("db not exists fix: %s %s", serviceDescKey, value)
			if cmd.fixData(serviceDescKey, db, etcdClient) != nil {
				glog.Errorf("fix error: %s %s", string(kv.Key), value)
				return subcommands.ExitSuccess
			}
			continue
		}
	}

	//反向比较，数据库存在，etcd不存在，只打印
	if action == "reverse" {
		for key, val := range dbServiceMap {
			if _, ok := descMap[key]; !ok {
				split := strings.Split(val, "$")
				glog.Infof("etcd not exists: %s status: %s md5Status: %s", key, split[0], split[1])
			}
		}
	}
	return subcommands.ExitSuccess
}

func (cmd *ConsistencyFixCmd) fixData(fixKey string, db *sql.DB, etcdClient *clientv3.Client) error {
	resp, err := etcdClient.Get(context.Background(), fixKey)
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		var desc services.ServiceDescV1
		if err := json.Unmarshal(kv.Value, &desc); err != nil {
			glog.Errorf("unmarshal desc fail %s %s: %v", string(kv.Key), string(kv.Value), err)
			return err
		}
		w := md5.New()
		io.WriteString(w, desc.Proto)
		md5Tmp := fmt.Sprintf("%x", w.Sum(nil))
		println("key:" + string(kv.Key) + "\n" + md5Tmp)
		desc.Md5 = md5Tmp
		if dbError := cmd.updateServiceDBItems(db, []services.ServiceDescV1{desc}); dbError != nil {
			return dbError
		}
	}
	return nil
}

func (ctrl *ConsistencyFixCmd) updateServiceDBItems(db *sql.DB, services []services.ServiceDescV1) error {
	if len(services) == 0 {
		return nil
	}

	sqlValues := make([]string, 0, len(services))
	values := make([]interface{}, 0, len(services)*9)
	for i := range services {
		service := &services[i]
		sqlValues = append(sqlValues, "(?,?,?,?,?,?,?,?)")
		values = append(values, 0, service.Service, service.Zone, service.Type, service.Proto, service.Description, service.Md5, 1)
	}
	sql := fmt.Sprintf(`insert into services(status, service, zone, typ, proto, description, proto_md5, md5_status) values %s 
						on duplicate key update status=values(status), typ=values(typ),
						proto=values(proto), description=values(description), proto_md5=values(proto_md5), md5_status=1 `,
		strings.Join(sqlValues, ","))
	_, err := db.Exec(sql, values...)
	return err
}

// Execute cmd execute
func (cmd *ConsistencyFixCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fixStr := "修复，etcd存在的数据，数据库不存在，将etcd数据入库"
	reverseStr := "反向比较，数据库存在，etcd不存在，只打印"
	printStr := "打印etcd存在的数据，数据库不存在"
	x := NewXBus()
	db := x.NewDB()
	etcdClient := x.Config.Etcd.NewEtcdClient()
	if f.Arg(0) == "fix" {
		println("fix")
		return cmd.action(db, etcdClient, "fix")

	} else if f.Arg(0) == "reverse" {
		println("reverse")
		return cmd.action(db, etcdClient, "reverse")

	} else if f.Arg(0) == "print" {
		println("print")
		return cmd.action(db, etcdClient, "print")

	} else {
		println(fmt.Sprintf("Args Support:\nprint: %s\nreverse: %s\nfix: %s\n", printStr, reverseStr, fixStr))
		return subcommands.ExitSuccess
	}

	return subcommands.ExitSuccess
}
