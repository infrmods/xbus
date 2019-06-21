package main

import (
	"context"
	"flag"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
)

type KeyCertCmd struct {
}

func (cmd *KeyCertCmd) Name() string {
	return "keycert"
}

func (cmd *KeyCertCmd) Synopsis() string {
	return "get app's key/cert"
}

func (cmd *KeyCertCmd) Usage() string {
	return "keycert [OPTIONS] app"
}

func (cmd *KeyCertCmd) SetFlags(f *flag.FlagSet) {
}

func (cmd *KeyCertCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	app_name := f.Args()[0]

	if app, err := apps.GetAppByName(db, app_name); err == nil {
		if err := utils.WriteFile(app_name+"cert.pem", 0644, []byte(app.Cert)); err != nil {
			glog.Errorf("write cert fail: %v", err)
			return subcommands.ExitFailure
		}
		if err := utils.WriteFile(app_name+"key.pem", 0600, []byte(app.PrivateKey)); err != nil {
			glog.Errorf("write key fail: %v", err)
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	} else {
		glog.Errorf("get app fail: %v", err)
		return subcommands.ExitFailure
	}
}
