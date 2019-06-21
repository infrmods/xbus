package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"flag"
	"os"

	"github.com/coreos/etcd/clientv3"
	"github.com/gocomm/config"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/api"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Etcd     utils.ETCDConfig
	Services services.Config
	Configs  configs.Config
	Apps     apps.Config
	Api      api.Config

	DB struct {
		Driver  string `default:"mysql"`
		Source  string `default:"root:passwd@/xbus?parseTime=true"`
		MaxConn int    `default:"20"`
	}
}

var cfgPath = flag.String("config", "config.yaml", "config file path")

type XBus struct {
	Config Config
}

func NewXBus() *XBus {
	var x XBus
	if *cfgPath == "" {
		if err := config.DefaultConfig(&x.Config); err != nil {
			glog.Errorf("set default config file fail: %v", err)
			os.Exit(-1)
		}
	} else if err := config.LoadFromFileF(*cfgPath, &x.Config, yaml.Unmarshal); err != nil {
		glog.Errorf("load config file fail: %v", err)
		os.Exit(-1)
	}

	return &x
}

func (x *XBus) NewDB() *sql.DB {
	db, err := sql.Open(x.Config.DB.Driver, x.Config.DB.Source)
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	db.SetMaxOpenConns(x.Config.DB.MaxConn)
	return db
}

func (x *XBus) NewEtcdClient() *clientv3.Client {
	var tlsConfig *tls.Config
	if x.Config.Etcd.CACert != "" {
		cert, err := utils.ReadPEMCertificate(x.Config.Etcd.CACert)
		if err != nil {
			glog.Errorf("read etcd's cacertfail: %v", err)
			os.Exit(-1)
		}

		pool := x509.NewCertPool()
		pool.AddCert(cert)
		tlsConfig = &tls.Config{RootCAs: pool}
	}
	etcdConfig := clientv3.Config{
		Endpoints:   x.Config.Etcd.Endpoints,
		DialTimeout: x.Config.Etcd.Timeout,
		TLS:         tlsConfig}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		glog.Errorf("create etcd clientv3 fail: %v", err)
		os.Exit(-1)
	}
	return etcdClient
}

func (x *XBus) NewAppCtrl(db *sql.DB) *apps.AppCtrl {
	appCtrl, err := apps.NewAppCtrl(&x.Config.Apps, db)
	if err != nil {
		glog.Errorf("create appsCtrl fail: %v", err)
		os.Exit(-1)
	}
	return appCtrl
}

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&NewAppCmd{}, "")
	subcommands.Register(&RunCommand{}, "")
	subcommands.Register(&GenRootCmd{}, "")
	subcommands.Register(&ListAppCmd{}, "")
	subcommands.Register(&ListGroupCmd{}, "")
	subcommands.Register(&ListPerm{}, "")
	subcommands.Register(&GrantCmd{}, "")
	subcommands.Register(&KeyCertCmd{}, "")

	flag.Set("logtostderr", "true")
	flag.Parse()
	subcommands.Execute(context.Background())
}
