package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/api"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"golang.org/x/net/context"
	"os"
)

type RunCommand struct {
}

func (cmd *RunCommand) Name() string {
	return "run"
}

func (cmd *RunCommand) Synopsis() string {
	return "run server"
}

func (cmd *RunCommand) SetFlags(f *flag.FlagSet) {
}

func (cmd *RunCommand) Usage() string {
	return ""
}

func (cmd *RunCommand) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	etcdClient := x.NewEtcdClient()
	services := services.NewServiceCtrl(&x.Config.Services, etcdClient)
	configs := configs.NewConfigCtrl(&x.Config.Configs, db, etcdClient)
	apiServer := api.NewAPIServer(&x.Config.Api, etcdClient, services, configs, x.NewAppCtrl(db))
	if err := apiServer.Start(); err != nil {
		glog.Errorf("start api_sersver fail: %v", err)
		os.Exit(-1)
	}
	if err := apiServer.Wait(); err != nil {
		glog.Errorf("wait api_server fail: %v", err)
		os.Exit(-1)
	}
	return subcommands.ExitSuccess
}
