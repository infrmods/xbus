package main

import (
	"flag"
	"github.com/gocomm/config"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/api"
	"github.com/infrmods/xbus/service"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Xbus service.Config
	Api  api.Config
}

var cfg_path = flag.String("config", "", "config file path")

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	var cfg Config
	if *cfg_path == "" {
		if err := config.DefaultConfig(&cfg); err != nil {
			glog.Errorf("set default config file fail: %v", err)
			return
		}
	} else if err := config.LoadFromFileF(*cfg_path, &cfg, yaml.Unmarshal); err != nil {
		glog.Errorf("load config file fail: %v", err)
		return
	}

	xbus := service.NewXBus(&cfg.Xbus)
	if err := xbus.Init(); err != nil {
		glog.Errorf("init xbus fail: %v", err)
		return
	}

	api_server := api.NewAPIServer(&cfg.Api, xbus)
	if err := api_server.Start(); err != nil {
		glog.Errorf("start api_sersver fail: %v", err)
		return
	}
	if err := api_server.Wait(); err != nil {
		glog.Errorf("wait api_server fail: %v", err)
		return
	}
}
