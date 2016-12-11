package apps

import (
	"crypto"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"github.com/gocomm/dbutil"
	_ "github.com/gocomm/dbutil/dialects/mysql"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"math/big"
	"net"
	"regexp"
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
}

type AppCtrl struct {
	config       *Config
	db           *sql.DB
	CertsManager *CertsCtrl
}

func NewAppCtrl(config *Config, db *sql.DB) (*AppCtrl, error) {
	certs, err := NewCertsCtrl(&config.Cert, &DBSerialGenerator{db})
	if err != nil {
		return nil, err
	}
	return &AppCtrl{config: config, db: db, CertsManager: certs}, nil
}

func (ctrl *AppCtrl) GetAppCertPool() *x509.CertPool {
	return ctrl.CertsManager.CertPool()
}

var rAppName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-_]+$`)

func (ctrl *AppCtrl) NewApp(app *App, pk crypto.PublicKey, dnsNames []string, ips []net.IP, days int) (privKey crypto.Signer, err error) {
	if !rAppName.MatchString(app.Name) {
		return nil, utils.Errorf(utils.EcodeInvalidName, "invalid app name: %s", app.Name)
	}
	for _, name := range []string{"public", "global", "app", "xbus"} {
		if app.Name == name {
			return nil, utils.Errorf(utils.EcodeInvalidName, "reserved name: %s", app.Name)
		}
	}

	if pk == nil {
		privKey, err = utils.NewPrivateKey(ctrl.config.EcdsaCruve, ctrl.config.RSABits)
		if err != nil {
			glog.Errorf("generate key fail: %v", err)
			return nil, utils.NewSystemError("generate key fail")
		}
		pk = privKey.Public()
	}

	name := pkix.Name{CommonName: app.Name,
		Organization: []string{ctrl.config.Organization}}
	if certPem, err := ctrl.CertsManager.NewCert(pk, name, dnsNames, ips, days); err == nil {
		app.Cert = string(certPem)
	} else {
		glog.Errorf("generate cert fail: %v", err)
		return nil, utils.NewSystemError("generate cert fail")
	}

	if err := InsertApp(ctrl.db, app); err != nil {
		if err == dbutil.ZeroEffected {
			return nil, utils.NewError(utils.EcodeNameDuplicated, "name duplicated")
		}

		glog.Errorf("insert app(%s) fail: %v", app.Name, err)
		return nil, utils.NewSystemError("create app fail")
	}
	return
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
