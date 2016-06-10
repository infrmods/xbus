package apps

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"github.com/gocomm/dbutil"
	_ "github.com/gocomm/dbutil/dialects/mysql"
	"math/big"
)

var SERIAL_CONFIG_ITEM = "cert_serial"

type DBSerialGenerator struct {
	db *sql.DB
}

func (g *DBSerialGenerator) Generate() (*big.Int, error) {
	item, err := GetConfigItem(g.db, SERIAL_CONFIG_ITEM)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 32; i++ {
		n, err := item.GetIntValue()
		if err != nil {
			return nil, err
		}
		item.SetIntValue(n + 1)
		if err := item.UpdateValue(g.db); err == nil {
			return big.NewInt(n + 1), nil
		} else if err == dbutil.ZeroEffected {
			continue
		} else {
			return nil, err
		}
	}
	return nil, fmt.Errorf("update loop exceeded")
}

type Config struct {
	Cert         CertsConfig
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

func (ctrl *AppCtrl) NewApp(app *App, pk crypto.PublicKey, dnsNames []string, days int) (privKey crypto.Signer, err error) {
	if pk == nil {
		privKey, err = rsa.GenerateKey(rand.Reader, ctrl.config.RSABits)
		if err != nil {
			return
		}
		pk = privKey.Public()
	}

	name := pkix.Name{CommonName: app.Name,
		Organization: []string{ctrl.config.Organization}}
	if certPem, err := ctrl.CertsManager.NewCert(pk, name, dnsNames, days); err == nil {
		app.Cert = string(certPem)
	} else {
		return nil, err
	}

	if err := InsertApp(ctrl.db, app); err != nil {
		return nil, err
	}
	return
}

func (ctrl *AppCtrl) GetAppByName(name string) (*App, error) {
	return GetAppByName(ctrl.db, name)
}

func (ctrl *AppCtrl) NewGroup(group *Group) error {
	return InsertGroup(ctrl.db, group)
}

func (ctrl *AppCtrl) GetGroupByName(name string) (*Group, error) {
	return GetGroupByName(ctrl.db, name)
}

func (ctrl *AppCtrl) AddGroupMember(groupId, appId int64) error {
	return NewGroupMember(ctrl.db, groupId, appId)
}

func (ctrl *AppCtrl) GetGroupMembers(groupId int64) ([]App, error) {
	return GetGroupMembers(ctrl.db, groupId)
}

func (ctrl *AppCtrl) NewGroupPerm(groupId int64, content string) (int64, error) {
	perm := Perm{TargetType: PermTargetGroup,
		TargetId: groupId, Content: content}
	if err := InsertPerm(ctrl.db, &perm); err == nil {
		return perm.Id, nil
	} else {
		return 0, err
	}
}

func (ctrl *AppCtrl) NewAppPerm(appId int64, content string) (int64, error) {
	perm := Perm{TargetType: PermTargetApp,
		TargetId: appId, Content: content}
	if err := InsertPerm(ctrl.db, &perm); err == nil {
		return perm.Id, nil
	} else {
		return 0, err
	}
}

func (ctrl *AppCtrl) HasAnyPerm(typ int, appId int64, groupIds []int64, needWrite bool, content string) (bool, error) {
	return HasAnyPerm(ctrl.db, typ, appId, groupIds, needWrite, content)
}

func (ctrl *AppCtrl) HasAnyPrefixPerm(typ int, appId int64, groupIds []int64, needWrite bool, content string) (bool, error) {
	return HasAnyPrefixPerm(ctrl.db, typ, appId, groupIds, needWrite, content)
}
