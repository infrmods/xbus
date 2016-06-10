package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509/pkix"
	"database/sql"
	"flag"
	"github.com/coreos/etcd/clientv3"
	"github.com/gocomm/config"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/api"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"gopkg.in/yaml.v2"
	"os"
	"strings"
)

type Config struct {
	Etcd     utils.ETCDConfig
	Services services.Config
	Configs  configs.Config
	Apps     apps.Config
	Api      api.Config

	DB struct {
		Driver string `default:"mysql"`
		Source string `default:"root:passwd@/xbus?parseTime=true"`
	}
}

var cfg_path = flag.String("config", "", "config file path")
var newApiCert = flag.Bool("new-api-cert", false, "create xbus api cert/key")
var apiDnsNames = flag.String("api-dns", "", "api dns names, separated by comma")
var apiKeyBits = flag.Int("api-rsa-bits", 2048, "api private key size in bits")

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	var cfg Config
	if *cfg_path == "" {
		if err := config.DefaultConfig(&cfg); err != nil {
			glog.Errorf("set default config file fail: %v", err)
			os.Exit(-1)
		}
	} else if err := config.LoadFromFileF(*cfg_path, &cfg, yaml.Unmarshal); err != nil {
		glog.Errorf("load config file fail: %v", err)
		os.Exit(-1)
	}

	db, err := sql.Open(cfg.DB.Driver, cfg.DB.Source)
	if err != nil {
		glog.Errorf("open database fail: %v", err)
		os.Exit(-1)
	}
	appsCtrl, err := apps.NewAppCtrl(&cfg.Apps, db)
	if err != nil {
		glog.Errorf("create appsCtrl fail: %v", err)
		os.Exit(-1)
	}
	if *newApiCert {
		privKey, err := rsa.GenerateKey(rand.Reader, *apiKeyBits)
		if err != nil {
			glog.Errorf("generate rsa key fail: %v", err)
			os.Exit(-1)
		}

		dnsNames := strings.Split(*apiDnsNames, ",")
		pemBytes, err := appsCtrl.CertsManager.NewCert(privKey.Public(),
			pkix.Name{CommonName: "api.xbus", Organization: []string{cfg.Apps.Organization}},
			dnsNames, 8*365)
		if err != nil {
			glog.Errorf("create cert fail: %v", err)
			os.Exit(-1)
		}
		if err := utils.WriteFile("apicert.pem", 0644, pemBytes); err != nil {
			glog.Errorf("write api cert fail: %v", err)
			os.Exit(-1)
		}
		if err := utils.WritePrivateKey("apikey.pem", 0600, privKey); err != nil {
			glog.Errorf("write private key fail: %v", err)
			os.Exit(-1)
		}
		return
	}

	etcdConfig := clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.Timeout,
		TLS:         cfg.Etcd.TLS}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		glog.Errorf("create etcd clientv3 fail: %v", err)
		os.Exit(-1)
	}

	services := services.NewServiceCtrl(&cfg.Services, etcdClient)
	configs := configs.NewConfigCtrl(&cfg.Configs, etcdClient)
	apiServer := api.NewAPIServer(&cfg.Api, services, configs, appsCtrl)
	if err := apiServer.Start(); err != nil {
		glog.Errorf("start api_sersver fail: %v", err)
		os.Exit(-1)
	}
	if err := apiServer.Wait(); err != nil {
		glog.Errorf("wait api_server fail: %v", err)
		os.Exit(-1)
	}
}
