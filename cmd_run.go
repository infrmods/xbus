package main

import (
	"flag"
	"os"

	"context"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/api"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
)

// RunCmd run cmd
type RunCmd struct {
}

// Name cmd name
func (cmd *RunCmd) Name() string {
	return "run"
}

// Synopsis cmd synopsis
func (cmd *RunCmd) Synopsis() string {
	return "run server"
}

// SetFlags cmd set flags
func (cmd *RunCmd) SetFlags(f *flag.FlagSet) {
}

// Usage cmd usgae
func (cmd *RunCmd) Usage() string {
	return ""
}

// Execute cmd execute
func (cmd *RunCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	etcdClient := x.NewEtcdClient()
	services, err := services.NewServiceCtrl(&x.Config.Services, db, etcdClient)
	if err != nil {
		glog.Errorf("create service fail: %v", err)
		os.Exit(-1)
	}
	configs := configs.NewConfigCtrl(&x.Config.Configs, db, etcdClient)
	apiServer := api.NewServer(&x.Config.API, etcdClient, services, configs, x.NewAppCtrl(db, etcdClient))
	if err := apiServer.Run(); err != nil {
		glog.Errorf("start api_sersver fail: %v", err)
		os.Exit(-1)
	}
	return subcommands.ExitSuccess
}
