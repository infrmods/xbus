package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/services"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var rServiceSplit = regexp.MustCompile(`/(.+)/(.+)/(.+)$`)

const serviceDescNodeKey = "desc"
const serviceKeyNodePrefix = "node_"

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
		if rows, err := db.Query(`select service, zone, typ, proto, description, status, md5_status from services
				order by create_time  desc limit ?,?`, start, 1000); err == nil {
			for rows.Next() {
				var service, zone, typ, proto, description, status, md5Status string
				if err := rows.Scan(&service, &zone, &typ, &proto, &description, &status, &md5Status); err != nil {
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

func (cmd *ConsistencyFixCmd) descEmptyJson(etcdClient *clientv3.Client, action string) subcommands.ExitStatus {
	respServices, err := etcdClient.Get(context.Background(), "/services/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		glog.Errorf("get services fail: %v", err)
		return subcommands.ExitSuccess
	}
	actions := strings.Split(action, "-")
	descCount := 0
	if len(actions) > 2 {
		descCount, _ = strconv.Atoi(actions[2])
	}
	for count, kv := range respServices.Kvs {
		if count >= descCount {
			return subcommands.ExitSuccess
		}
		serviceKey := string(kv.Key)
		matches := rServiceSplit.FindAllStringSubmatch(serviceKey, -1)
		suffix := matches[0][3]
		if suffix != serviceDescNodeKey {
			continue
		}
		//将desc的value设置为{}(空json串)
		if strings.Contains(action, "empty-json") {
			if _, err := etcdClient.Put(context.Background(), serviceKey, "{}"); err != nil {
				glog.Errorf("empty-json error key: %s", serviceKey)
				return subcommands.ExitSuccess
			}
			glog.Infof("set empty json desc key: %s", serviceKey)
		}
	}
	return subcommands.ExitSuccess
}

func (cmd *ConsistencyFixCmd) etcdDescMd5Action(etcdClient *clientv3.Client, action string) subcommands.ExitStatus {
	respServices, errServices := etcdClient.Get(context.Background(), "/services/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	respMd5s, errMd5s := etcdClient.Get(context.Background(), "/services-md5s/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if errServices != nil {
		glog.Errorf("get services fail: %v", errServices)
		return subcommands.ExitSuccess
	}
	if errMd5s != nil {
		glog.Errorf("get md5s fail: %v", errMd5s)
		return subcommands.ExitSuccess
	}

	if strings.Contains(action, "desc-delete-error") {
		cmd.deleteErrorMd5s(etcdClient, respMd5s.Kvs, action)
		return subcommands.ExitSuccess
	}

	servicesMap := make(map[string]bool)
	endpointsMap := make(map[string]string)
	for _, kv := range respServices.Kvs {
		serviceKey := string(kv.Key)
		matches := rServiceSplit.FindAllStringSubmatch(serviceKey, -1)
		suffix, zone := matches[0][3], matches[0][2]
		if strings.HasPrefix(suffix, serviceKeyNodePrefix) {
			service := strings.Split(matches[0][1], "/")[1]
			endpointsMap[service+"/"+zone] = suffix
			continue
		}
		if suffix != serviceDescNodeKey {
			continue
		}
		service, zone := strings.Split(matches[0][1], "/")[1], matches[0][2]
		servicesMap[service+"/"+zone] = true
	}

	deleteAction := "desc-delete"
	splitAction := strings.Split(action, "-")
	deleteLimit := -1
	if len(splitAction) >= 3 {
		deleteLimit, _ = strconv.Atoi(splitAction[2])
	}

	for _, kv := range respMd5s.Kvs {
		md5Key := string(kv.Key)
		matches := rServiceSplit.FindAllStringSubmatch(md5Key, -1)
		service, zone := matches[0][3], matches[0][2]
		serviceZoneKey := service + "/" + zone
		exists := false
		if _, exists = servicesMap[serviceZoneKey]; !exists {
			//service desc不存在，但是md5s存在
			if strings.Contains(action, deleteAction) && deleteLimit > 0 {
				//判断是否存有在线节点
				nodeStr := endpointsMap[serviceZoneKey]
				glog.Infof("service desc not exists delete md5s: %s node: %s", md5Key, nodeStr)
				if _, deleteErr := etcdClient.Delete(context.Background(), md5Key); deleteErr != nil {
					glog.Errorf("delete  fail: %v", deleteErr)
					return subcommands.ExitSuccess
				}
				deleteLimit--
				if deleteLimit <= 0 {
					return subcommands.ExitSuccess
				}
				continue
			}
		}
		if !exists {
			glog.Infof("service desc not exists: %s", md5Key)
		}
	}
	return subcommands.ExitSuccess
}

func (cmd *ConsistencyFixCmd) deleteErrorMd5s(etcdClient *clientv3.Client, md5Kvs []*mvccpb.KeyValue, action string) {
	splitAction := strings.Split(action, "-")
	deleteLimit := -1
	if len(splitAction) >= 4 {
		deleteLimit, _ = strconv.Atoi(splitAction[3])
	}
	for _, kv := range md5Kvs {
		md5Key := string(kv.Key)
		matches := rServiceSplit.FindAllStringSubmatch(md5Key, -1)
		service, zone := matches[0][3], matches[0][2]
		wrongFormatFlag := !strings.Contains(service, ":") && strings.Contains(zone, ":")

		if deleteLimit <= 0 && wrongFormatFlag {
			glog.Infof("print md5s wrong format: %s", md5Key)
			continue
		}

		//md5s格式错误的路径直接删除
		if deleteLimit > 0 && wrongFormatFlag {
			glog.Infof("delete md5s wrong format: %s", md5Key)
			if _, deleteErr := etcdClient.Delete(context.Background(), md5Key); deleteErr != nil {
				glog.Errorf("delete md5s wrong format fail: %v", deleteErr)
				return
			}
			deleteLimit--
			if deleteLimit <= 0 {
				return
			}
		}
	}
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
	descStr := "打印etcd里services-md5s/zone/service存在，不存在service/zone/desc"
	descDeleteStr := "清除etcd里services-md5s存在但是desc不存在的services-md5s路径(desc-delete-$count)"
	descDeleErrorteStr := "清除etcd里services-md5s错误格式的path(desc-delete-error-$count)"
	descEmptyJsonStr := "将desc的value设置为空json串{}(empty-json-$count)"
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

	} else if strings.Contains(f.Arg(0), "desc") {
		cmd.etcdDescMd5Action(etcdClient, f.Arg(0))
	} else if strings.Contains(f.Arg(0), "empty-json") {
		return cmd.descEmptyJson(etcdClient, f.Arg(0))
	} else {
		println(fmt.Sprintf("Args Support:\nprint: %s\nreverse: %s\nfix: %s\ndesc: %s\ndesc-delete: %s\ndesc-delete-error: %s\nempty-json: %s\n", printStr, reverseStr, fixStr, descStr, descDeleteStr, descDeleErrorteStr, descEmptyJsonStr))
		return subcommands.ExitSuccess
	}

	return subcommands.ExitSuccess
}
