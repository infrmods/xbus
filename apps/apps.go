package apps

import (
	"context"
	"crypto"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/gocomm/dbutil"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"google.golang.org/grpc/codes"
)

var SERIAL_CONFIG_ITEM = "cert_serial"

type DBSerialGenerator struct {
	db *sql.DB
}

func (g *DBSerialGenerator) Generate() (*big.Int, error) {
	var item *ConfigItem
	var err error
	for iter := 0; iter < 2; iter++ {
		if item, err = GetConfigItem(g.db, SERIAL_CONFIG_ITEM); err == nil {
			break
		} else if err == sql.ErrNoRows {
			item = &ConfigItem{Name: SERIAL_CONFIG_ITEM, Ver: 1}
			item.SetIntValue(2)
			if new, err := InsertConfigItem(g.db, item); err != nil {
				glog.Errorf("insert config item(%#v) fail: %v", *item, err)
				return nil, utils.NewSystemError("generate serial fail")
			} else if new {
				return big.NewInt(2), nil
			} else if iter == 1 {
				glog.Errorf("insert config item(%#v) deadloop", *item)
				return nil, utils.NewSystemError("generate serial fail")
			}
		} else {
			glog.Errorf("get serial config(%s) fail: %v", SERIAL_CONFIG_ITEM, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
	}

	for i := 0; i < 32; i++ {
		n, err := item.GetIntValue()
		if err != nil {
			glog.Errorf("get serial(%s) value fail: %v", SERIAL_CONFIG_ITEM, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
		item.SetIntValue(n + 1)
		if err := item.UpdateValue(g.db); err == nil {
			return big.NewInt(n + 1), nil
		} else if err == dbutil.ZeroEffected {
			continue
		} else {
			glog.Errorf("update serial(%s) fail: %v", SERIAL_CONFIG_ITEM, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
	}
	return nil, utils.NewError(utils.EcodeTooManyAttempts, "loop exceeded")
}

type Config struct {
	Cert         CertsConfig
	EcdsaCruve   string
	RSABits      int    `default:"2048"`
	Organization string `default:"XBus"`
	KeyPrefix    string `default:"/apps" yaml:"key_prefix"`
}

type AppCtrl struct {
	config       *Config
	db           *sql.DB
	CertsManager *CertsCtrl
	etcdClient   *clientv3.Client
}

func NewAppCtrl(config *Config, db *sql.DB, etcdClient *clientv3.Client) (*AppCtrl, error) {
	certs, err := NewCertsCtrl(&config.Cert, &DBSerialGenerator{db})
	if err != nil {
		return nil, err
	}
	return &AppCtrl{config: config, db: db, CertsManager: certs, etcdClient: etcdClient}, nil
}

func (ctrl *AppCtrl) GetAppCertPool() *x509.CertPool {
	return ctrl.CertsManager.CertPool()
}

var rAppName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-_]+$`)

func (ctrl *AppCtrl) NewApp(app *App, key crypto.Signer, dnsNames []string, ips []net.IP, days int) (crypto.Signer, error) {
	if !rAppName.MatchString(app.Name) {
		return nil, utils.Errorf(utils.EcodeInvalidName, "invalid app name: %s", app.Name)
	}
	lowerName := strings.ToLower(app.Name)
	for _, name := range []string{"public", "global", "app", "xbus", "null", "unknown"} {
		if lowerName == name {
			return nil, utils.Errorf(utils.EcodeInvalidName, "reserved name: %s", app.Name)
		}
	}

	var err error
	if key == nil {
		key, err = utils.NewPrivateKey(ctrl.config.EcdsaCruve, ctrl.config.RSABits)
		if err != nil {
			glog.Errorf("generate key fail: %v", err)
			return nil, utils.NewSystemError("generate key fail")
		}
	}

	name := pkix.Name{CommonName: app.Name,
		Organization: []string{ctrl.config.Organization}}
	if certPem, err := ctrl.CertsManager.NewCert(key.Public(), name, dnsNames, ips, days); err == nil {
		app.Cert = string(certPem)
	} else {
		glog.Errorf("generate cert fail: %v", err)
		return nil, utils.NewSystemError("generate cert fail")
	}

	if data, err := utils.EncodePrivateKeyToPem(key); err == nil {
		app.PrivateKey = data
	} else {
		glog.Errorf("encode private key to pem fail: {}", err)
		return nil, utils.NewSystemError("encode private key pem fail")
	}

	if err := InsertApp(ctrl.db, app); err != nil {
		if err == dbutil.ZeroEffected {
			return nil, utils.NewError(utils.EcodeNameDuplicated, "name duplicated")
		}

		glog.Errorf("insert app(%s) fail: %v", app.Name, err)
		return nil, utils.NewSystemError("create app fail")
	}
	return key, nil
}

func (ctrl *AppCtrl) GetPerms(typ int, app_name *string, group_name *string, can_write *bool, prefix *string) ([]Perm, error) {
	var target_type *int
	var target_id *int64
	if app_name != nil {
		if app, err := GetAppByName(ctrl.db, *app_name); err == nil {
			t := PermTargetApp
			target_type = &t
			target_id = &app.Id
		} else {
			glog.Errorf("get app(%s) fail: %v", app_name, err)
			return nil, utils.NewSystemError("get app fail")
		}
	} else if group_name != nil {
		if group, err := GetGroupByName(ctrl.db, *group_name); err == nil {
			t := PermTargetGroup
			target_type = &t
			target_id = &group.Id
		} else {
			glog.Errorf("get group(%s) fail: %v", group_name, err)
			return nil, utils.NewSystemError("get group fail")
		}
	}

	if perms, err := GetPerms(ctrl.db, typ, target_type, target_id, can_write, prefix); err == nil {
		return perms, nil
	} else {
		glog.Errorf("get perms fail: %v", err)
		return nil, utils.NewSystemError("get perms fail")
	}
}

func (ctrl *AppCtrl) GetAppByName(name string) (*App, error) {
	if app, err := GetAppByName(ctrl.db, name); err == nil {
		return app, nil
	} else {
		glog.Errorf("get app(%s) fail: %v", name, err)
		return nil, utils.NewSystemError("get app fail")
	}
}

func (ctrl *AppCtrl) ListApp(skip, limit int) ([]App, error) {
	if apps, err := ListApp(ctrl.db, skip, limit); err == nil {
		return apps, nil
	} else {
		glog.Errorf("list app fail: %v", err)
		return nil, utils.NewSystemError("list app fail")
	}
}

func (ctrl *AppCtrl) GetAppGroupByName(name string) (*App, []int64, error) {
	if app, groupIds, err := GetAppGroupByName(ctrl.db, name); err == nil {
		return app, groupIds, nil
	} else {
		glog.Errorf("get app&group(%s) fail: %v", name, err)
		return nil, nil, utils.NewSystemError("get app&group fail")
	}
}

func (ctrl *AppCtrl) NewGroup(group *Group) error {
	if err := InsertGroup(ctrl.db, group); err == nil {
		return nil
	} else if err == dbutil.ZeroEffected {
		return utils.NewError(utils.EcodeNameDuplicated, "name duplicated")
	} else {
		glog.Errorf("insert group(%s) fail: %v", group.Name, err)
		return utils.NewSystemError("create group fail")
	}
}

func (ctrl *AppCtrl) GetGroupByName(name string) (*Group, error) {
	if group, err := GetGroupByName(ctrl.db, name); err == nil {
		return group, nil
	} else {
		glog.Errorf("get group(%s) fail: %v", name, err)
		return nil, utils.NewSystemError("get group fail")
	}
}

func (ctrl *AppCtrl) AddGroupMember(groupId, appId int64) error {
	if err := NewGroupMember(ctrl.db, groupId, appId); err != nil {
		glog.Errorf("add group member(group: %d, app: %d) fail: %v", groupId, appId, err)
		return utils.NewSystemError("add member fail")
	}
	return nil
}

func (ctrl *AppCtrl) GetGroupMembers(groupId int64) ([]App, error) {
	if apps, err := GetGroupMembers(ctrl.db, groupId); err == nil {
		return apps, nil
	} else {
		glog.Errorf("get group(%d) members fail: %v", groupId, err)
		return nil, utils.NewSystemError("get group members fail")
	}
}

func (ctrl *AppCtrl) NewGroupPerm(permType int, groupId int64, canWrite bool, content string) (int64, error) {
	perm := Perm{PermType: permType, TargetType: PermTargetGroup,
		TargetId: groupId, CanWrite: canWrite, Content: content}
	if err := InsertPerm(ctrl.db, &perm); err == nil {
		return perm.Id, nil
	} else {
		glog.Errorf("new group perm fail: %v", err)
		return 0, utils.NewSystemError("new group perm fail")
	}
}

func (ctrl *AppCtrl) NewAppPerm(permType int, appId int64, canWrite bool, content string) (int64, error) {
	perm := Perm{PermType: permType, TargetType: PermTargetApp,
		TargetId: appId, CanWrite: canWrite, Content: content}
	if err := InsertPerm(ctrl.db, &perm); err == nil {
		return perm.Id, nil
	} else {
		glog.Errorf("new app perm fail: %v", err)
		return 0, utils.NewSystemError("new app perm fail")
	}
}

func (ctrl *AppCtrl) HasAnyPrefixPerm(typ int, appId int64, groupIds []int64, needWrite bool, content string) (bool, error) {
	if has, err := HasAnyPrefixPerm(ctrl.db, typ, appId, groupIds, needWrite, content); err == nil {
		return has, nil
	} else {
		glog.Errorf("get hasAnyPrefixPerm(type:%d, app:%d, groups:%v, needWrite:%v, content:%v) fail: %v",
			typ, appId, groupIds, needWrite, content, err)
		return false, utils.NewSystemError("get perm fail")
	}
}

type AppNode struct {
	Address string `json:"address"`
	Label   string `json:"label"`
	Config  string `json:"config"`
}

const holdValue = "{}"

func (ctrl *AppCtrl) PlugAppNode(ctx context.Context, appName string, node *AppNode, leaseID clientv3.LeaseID) (bool, error) {
	if node.Address == "" {
		return false, utils.Errorf(utils.EcodeInvalidAddress, "invalid app node address(empty)")
	}
	if node.Config == "" {
		return false, utils.Errorf(utils.EcodeInvalidValue, "invalid app node config(empty)")
	}
	label := node.Label
	if label == "" {
		label = "default"
	}

	holdKey := fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, appName, label, node.Address)
	if _, err := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(holdKey), "=", holdValue)).Else(
		clientv3.OpPut(holdKey, holdValue)).Commit(); err != nil {
		return false, utils.CleanErr(err, "put app holdKey fail", "put app holdKey faial: %v", err)
	}

	onlineKey := fmt.Sprintf("%s/%s/%s/node_%s/online", ctrl.config.KeyPrefix, appName, label, node.Address)
	newOnlineNode := false
	resp, err := ctrl.etcdClient.Get(ctx, onlineKey)
	if err != nil {
		if utils.GetErrCode(err) != codes.NotFound {
			return false, utils.CleanErr(err, "get app onlineNode fail", "get app onlineNode fail: %v", err)
		}
		newOnlineNode = true
	} else {
		newOnlineNode = len(resp.Kvs) == 0
	}
	if _, err := ctrl.etcdClient.Put(ctx, onlineKey, node.Config, clientv3.WithLease(leaseID)); err != nil {
		return false, utils.CleanErr(err, "put app onlineNode fail", "put app onlineNode fail: %v", err)
	}
	return newOnlineNode, nil
}

type AppNodes struct {
	Nodes    map[string]*string `json:"nodes"`
	Revision int64              `json:"revision"`
}

var rNodeKey = regexp.MustCompile(`/node_([^/]+)$`)
var rNodeOnlineKey = regexp.MustCompile(`/node_([^/]+)/online$`)

func (ctrl *AppCtrl) queryAppNodes(ctx context.Context, name, label string) (*AppNodes, error) {
	key := fmt.Sprintf("%s/%s/%s/", ctrl.config.KeyPrefix, name, label)
	if resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix()); err == nil {
		nodes := make(map[string]*string)
		for _, kv := range resp.Kvs {
			if matches := rNodeKey.FindStringSubmatch(string(kv.Key)); matches != nil {
				_, ok := nodes[matches[0]]
				if !ok {
					nodes[matches[0]] = nil
				}
			} else if matches = rNodeOnlineKey.FindStringSubmatch(string(kv.Key)); matches != nil {
				config := string(kv.Value)
				nodes[matches[0]] = &config
			}
		}
		return &AppNodes{Nodes: nodes, Revision: resp.Header.Revision}, nil
	} else {
		return nil, utils.CleanErr(err, "query fail", "query app nodes fail: %v", err)
	}
}

func (ctrl *AppCtrl) WatchAppNodes(ctx context.Context, name, label string, revision int64) (*AppNodes, error) {
	key := fmt.Sprintf("%s/%s/%s/", ctrl.config.KeyPrefix, name, label)
	if revision > 0 {
		watcher := clientv3.NewWatcher(ctrl.etcdClient)
		defer watcher.Close()
		<-watcher.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(revision))
	}

	return ctrl.queryAppNodes(ctx, name, label)
}

func (ctrl *AppCtrl) RemoveAppNode(ctx context.Context, name, label, address string) error {
	key := fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, name, label, address)
	if _, err := ctrl.etcdClient.Delete(ctx, key); err != nil {
		return utils.CleanErr(err, "delete app node fail", "delete app node fail: %v", err)
	}
	return nil
}
