package apps

import (
	"context"
	"crypto"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
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

var serialConfigItem = "cert_serial"

type dbSerialGenerator struct {
	db *sql.DB
}

func (g *dbSerialGenerator) Generate() (*big.Int, error) {
	var item *ConfigItem
	var err error
	for iter := 0; iter < 2; iter++ {
		if item, err = GetConfigItem(g.db, serialConfigItem); err == nil {
			break
		} else if err == sql.ErrNoRows {
			item = &ConfigItem{Name: serialConfigItem, Ver: 1}
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
			glog.Errorf("get serial config(%s) fail: %v", serialConfigItem, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
	}

	for i := 0; i < 32; i++ {
		n, err := item.GetIntValue()
		if err != nil {
			glog.Errorf("get serial(%s) value fail: %v", serialConfigItem, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
		item.SetIntValue(n + 1)
		if err := item.UpdateValue(g.db); err == nil {
			return big.NewInt(n + 1), nil
		} else if err == dbutil.ZeroEffected {
			continue
		} else {
			glog.Errorf("update serial(%s) fail: %v", serialConfigItem, err)
			return nil, utils.NewSystemError("generate serial fail")
		}
	}
	return nil, utils.NewError(utils.EcodeTooManyAttempts, "loop exceeded")
}

// Config module config
type Config struct {
	Cert         CertsConfig
	EcdsaCruve   string
	RSABits      int    `default:"2048"`
	Organization string `default:"XBus"`
	KeyPrefix    string `default:"/apps" yaml:"key_prefix"`
}

// AppCtrl app ctrl
type AppCtrl struct {
	config       *Config
	db           *sql.DB
	CertsManager *CertsCtrl
	etcdClient   *clientv3.Client
}

// NewAppCtrl new app ctrl
func NewAppCtrl(config *Config, db *sql.DB, etcdClient *clientv3.Client) (*AppCtrl, error) {
	certs, err := NewCertsCtrl(&config.Cert, &dbSerialGenerator{db})
	if err != nil {
		return nil, err
	}
	return &AppCtrl{config: config, db: db, CertsManager: certs, etcdClient: etcdClient}, nil
}

// GetAppCertPool get app certPool
func (ctrl *AppCtrl) GetAppCertPool() *x509.CertPool {
	return ctrl.CertsManager.CertPool()
}

var rAppName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-_]+$`)

// NewApp new app
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

	data, err := utils.EncodePrivateKeyToPem(key)
	if err != nil {
		glog.Errorf("encode private key to pem fail: %v", err)
		return nil, utils.NewSystemError("encode private key pem fail")
	}
	app.PrivateKey = data

	if err := InsertApp(ctrl.db, app); err != nil {
		if err == dbutil.ZeroEffected {
			return nil, utils.NewError(utils.EcodeNameDuplicated, "name duplicated")
		}

		glog.Errorf("insert app(%s) fail: %v", app.Name, err)
		return nil, utils.NewSystemError("create app fail")
	}
	return key, nil
}

// GetPerms get perms
func (ctrl *AppCtrl) GetPerms(typ int, appName *string, groupName *string, canWrite *bool, prefix *string) ([]Perm, error) {
	var targetType *int
	var targetID *int64
	if appName != nil {
		if app, err := GetAppByName(ctrl.db, *appName); err == nil {
			t := PermTargetApp
			targetType = &t
			targetID = &app.ID
		} else {
			glog.Errorf("get app(%s) fail: %v", *appName, err)
			return nil, utils.NewSystemError("get app fail")
		}
	} else if groupName != nil {
		if group, err := GetGroupByName(ctrl.db, *groupName); err == nil {
			t := PermTargetGroup
			targetType = &t
			targetID = &group.ID
		} else {
			glog.Errorf("get group(%s) fail: %v", *groupName, err)
			return nil, utils.NewSystemError("get group fail")
		}
	}

	perms, err := GetPerms(ctrl.db, typ, targetType, targetID, canWrite, prefix)
	if err != nil {
		glog.Errorf("get perms fail: %v", err)
		return nil, utils.NewSystemError("get perms fail")
	}
	return perms, nil
}

// GetAppByName get app byname
func (ctrl *AppCtrl) GetAppByName(name string) (*App, error) {
	app, err := GetAppByName(ctrl.db, name)
	if err != nil {
		glog.Errorf("get app(%s) fail: %v", name, err)
		return nil, utils.NewSystemError("get app fail")
	}
	return app, nil
}

// ListApp list app
func (ctrl *AppCtrl) ListApp(skip, limit int) ([]App, error) {
	apps, err := ListApp(ctrl.db, skip, limit)
	if err != nil {
		glog.Errorf("list app fail: %v", err)
		return nil, utils.NewSystemError("list app fail")
	}
	return apps, nil
}

// GetAppGroupByName get app group byname
func (ctrl *AppCtrl) GetAppGroupByName(name string) (*App, []int64, error) {
	app, groupIDs, err := GetAppGroupByName(ctrl.db, name)
	if err != nil {
		glog.Errorf("get app&group(%s) fail: %v", name, err)
		return nil, nil, utils.NewSystemError("get app&group fail")
	}
	return app, groupIDs, nil
}

// NewGroup new group
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

// GetGroupByName get group byname
func (ctrl *AppCtrl) GetGroupByName(name string) (*Group, error) {
	group, err := GetGroupByName(ctrl.db, name)
	if err != nil {
		glog.Errorf("get group(%s) fail: %v", name, err)
		return nil, utils.NewSystemError("get group fail")
	}
	return group, nil
}

// AddGroupMember add group member
func (ctrl *AppCtrl) AddGroupMember(groupID, appID int64) error {
	if err := NewGroupMember(ctrl.db, groupID, appID); err != nil {
		glog.Errorf("add group member(group: %d, app: %d) fail: %v", groupID, appID, err)
		return utils.NewSystemError("add member fail")
	}
	return nil
}

// GetGroupMembers get group members
func (ctrl *AppCtrl) GetGroupMembers(groupID int64) ([]App, error) {
	apps, err := GetGroupMembers(ctrl.db, groupID)
	if err != nil {
		glog.Errorf("get group(%d) members fail: %v", groupID, err)
		return nil, utils.NewSystemError("get group members fail")
	}
	return apps, nil
}

// NewGroupPerm new group perm
func (ctrl *AppCtrl) NewGroupPerm(permType int, groupID int64, canWrite bool, content string) (int64, error) {
	perm := Perm{PermType: permType, TargetType: PermTargetGroup,
		TargetID: groupID, CanWrite: canWrite, Content: content}
	err := InsertPerm(ctrl.db, &perm)
	if err != nil {
		glog.Errorf("new group perm fail: %v", err)
		return 0, utils.NewSystemError("new group perm fail")
	}
	return perm.ID, nil
}

// NewAppPerm new app perm
func (ctrl *AppCtrl) NewAppPerm(permType int, appID int64, canWrite bool, content string) (int64, error) {
	perm := Perm{PermType: permType, TargetType: PermTargetApp,
		TargetID: appID, CanWrite: canWrite, Content: content}
	if err := InsertPerm(ctrl.db, &perm); err != nil {
		glog.Errorf("new app perm fail: %v", err)
		return 0, utils.NewSystemError("new app perm fail")
	}
	return perm.ID, nil
}

// HasAnyPrefixPerm has any prefix perm
func (ctrl *AppCtrl) HasAnyPrefixPerm(typ int, appID int64, groupIDs []int64, needWrite bool, content string) (bool, error) {
	has, err := HasAnyPrefixPerm(ctrl.db, typ, appID, groupIDs, needWrite, content)
	if err != nil {
		glog.Errorf("get hasAnyPrefixPerm(type:%d, app:%d, groups:%v, needWrite:%v, content:%v) fail: %v",
			typ, appID, groupIDs, needWrite, content, err)
		return false, utils.NewSystemError("get perm fail")
	}
	return has, nil
}

// AppNode app node
type AppNode struct {
	Label  string `json:"label"`
	Key    string `json:"key"`
	Config string `json:"config"`
}

const holdValue = "{}"

// PlugAppNode plug app node
func (ctrl *AppCtrl) PlugAppNode(ctx context.Context, appName string, node *AppNode, leaseID clientv3.LeaseID) (bool, error) {
	if node.Key == "" {
		return false, utils.Errorf(utils.EcodeInvalidAddress, "invalid app node key(empty)")
	}
	if node.Config == "" {
		return false, utils.Errorf(utils.EcodeInvalidValue, "invalid app node config(empty)")
	}
	label := node.Label
	if label == "" {
		label = "default"
	}

	holdKey := ctrl.nodeHoldKey(appName, label, node.Key)
	if _, err := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(holdKey), "=", holdValue)).Else(
		clientv3.OpPut(holdKey, holdValue)).Commit(); err != nil {
		return false, utils.CleanErr(err, "put app holdKey fail", "put app holdKey faial: %v", err)
	}

	onlineKey := ctrl.nodeOnlineKey(appName, label, node.Key)
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

// AppNodes app nodes
type AppNodes struct {
	Nodes    map[string]*string `json:"nodes"`
	Revision int64              `json:"revision"`
}

func (ctrl *AppCtrl) queryAppNodes(ctx context.Context, name, label string) (*AppNodes, error) {
	key := ctrl.nodeKeyPrefix(name, label)
	resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, utils.CleanErr(err, "query fail", "query app nodes fail: %v", err)
	}
	nodes := make(map[string]*string)
	for _, kv := range resp.Kvs {
		if nodeKey := ctrl.parseHoldNodeKey(string(kv.Key)); nodeKey != "" {
			_, ok := nodes[nodeKey]
			if !ok {
				nodes[nodeKey] = nil
			}
		} else if nodeKey := ctrl.parseOnlineNodeKey(string(kv.Key)); nodeKey != "" {
			config := string(kv.Value)
			nodes[nodeKey] = &config
		}
	}
	return &AppNodes{Nodes: nodes, Revision: resp.Header.Revision}, nil
}

// WatchAppNodes watch app nodes
func (ctrl *AppCtrl) WatchAppNodes(ctx context.Context, name, label string, revision int64) (*AppNodes, error) {
	prefix := ctrl.nodeKeyPrefix(name, label)
	if revision > 0 {
		watcher := clientv3.NewWatcher(ctrl.etcdClient)
		defer watcher.Close()
		<-watcher.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
	}

	return ctrl.queryAppNodes(ctx, name, label)
}

// RemoveAppNode remove app node
func (ctrl *AppCtrl) RemoveAppNode(ctx context.Context, name, label, key string) error {
	holdKey := ctrl.nodeHoldKey(name, label, key)
	if _, err := ctrl.etcdClient.Delete(ctx, holdKey); err != nil {
		return utils.CleanErr(err, "delete app node fail", "delete app node fail: %v", err)
	}
	return nil
}

// IsAppNodeOnline is app node online
func (ctrl *AppCtrl) IsAppNodeOnline(ctx context.Context, name, label, key string) (bool, error) {
	onlineKey := ctrl.nodeOnlineKey(name, label, key)
	resp, err := ctrl.etcdClient.Get(ctx, onlineKey)
	if err != nil {
		code, err := utils.CleanErrWithCode(err, "check app node online fail", "get app online node fail: %v", err)
		if code == codes.NotFound {
			return false, nil
		}
		return false, err
	}
	return len(resp.Kvs) > 0, nil
}
