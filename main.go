package main

import (
	"context"
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

	_ "github.com/gocomm/dbutil/dialects/mysql"
)

// Config xbus config
type Config struct {
	Etcd     utils.ETCDConfig
	Services services.Config
	Configs  configs.Config
	Apps     apps.Config
	API      api.Config

	DB struct {
		Driver  string `default:"mysql"`
		Source  string `default:"root:passwd@/xbus?parseTime=true"`
		MaxConn int    `default:"100"`
	}
}

var cfgPath = flag.String("config", "config.yaml", "config file path")

// XBus xbus
type XBus struct {
	Config Config
}

// NewXBus new xbus
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

// NewDB new db
func (x *XBus) NewDB() *sql.DB {
	db, err := sql.Open(x.Config.DB.Driver, x.Config.DB.Source)
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	db.SetMaxOpenConns(x.Config.DB.MaxConn)
	return db
}

// NewAppCtrl new app ctrl
func (x *XBus) NewAppCtrl(db *sql.DB, etcdClient *clientv3.Client) *apps.AppCtrl {
	appCtrl, err := apps.NewAppCtrl(&x.Config.Apps, db, etcdClient)
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
	subcommands.Register(&RunCmd{}, "")
	subcommands.Register(&GenRootCmd{}, "")
	subcommands.Register(&FixCmd{}, "")
	subcommands.Register(&ConsistencyFixCmd{}, "")
	subcommands.Register(&ListGroupCmd{}, "")
	subcommands.Register(&ListPermCmd{}, "")
	subcommands.Register(&GrantCmd{}, "")
	subcommands.Register(&KeyCertCmd{}, "")

	flag.Set("logtostderr", "true")
	flag.Parse()
	subcommands.Execute(context.Background())
}
