package main

import (
	"context"
	"flag"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
)

// KeyCertCmd key cert cmd
type KeyCertCmd struct {
}

// Name cmd name
func (cmd *KeyCertCmd) Name() string {
	return "keycert"
}

// Synopsis cmd synopsis
func (cmd *KeyCertCmd) Synopsis() string {
	return "get app's key/cert"
}

// Usage cmd usage
func (cmd *KeyCertCmd) Usage() string {
	return "keycert [OPTIONS] app"
}

// SetFlags cmd set flags
func (cmd *KeyCertCmd) SetFlags(f *flag.FlagSet) {
}

// Execute cmd execute
func (cmd *KeyCertCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	appName := f.Args()[0]

	app, err := apps.GetAppByName(db, appName)
	if err != nil {
		glog.Errorf("get app fail: %v", err)
		return subcommands.ExitFailure
	}
	if err := utils.WriteFile(appName+"cert.pem", 0644, []byte(app.Cert)); err != nil {
		glog.Errorf("write cert fail: %v", err)
		return subcommands.ExitFailure
	}
	if err := utils.WriteFile(appName+"key.pem", 0600, []byte(app.PrivateKey)); err != nil {
		glog.Errorf("write key fail: %v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
