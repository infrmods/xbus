package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
	"golang.org/x/net/context"
	"strings"
)

type NewAppCmd struct {
	AppName     string
	Description string
	DNSNames    string
	RSABits     int
	Days        int

	CertFile string
	KeyFile  string
}

func (cmd *NewAppCmd) Name() string {
	return "new-app"
}

func (cmd *NewAppCmd) Synopsis() string {
	return "create new app"
}

func (cmd *NewAppCmd) Usage() string { return "" }

func (cmd *NewAppCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.AppName, "name", "", "app name")
	f.StringVar(&cmd.Description, "desc", "", "app description")
	f.StringVar(&cmd.DNSNames, "dns", "", "DNSNames, sparated by comma")
	f.IntVar(&cmd.RSABits, "rsa-bits", 2048, "RSA key length in bits")
	f.IntVar(&cmd.Days, "days", 365*8, "cert valid for N days")

	f.StringVar(&cmd.CertFile, "cert-out", "", "cert output path, default: {name}cert.pem")
	f.StringVar(&cmd.KeyFile, "key-out", "", "key output path, default: {name}key.pem")
}

func (cmd *NewAppCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	if cmd.AppName == "" {
		glog.Errorf("please specify the app name")
		return subcommands.ExitUsageError
	}
	if cmd.CertFile == "" {
		cmd.CertFile = cmd.AppName + "cert.pem"
	}
	if cmd.KeyFile == "" {
		cmd.KeyFile = cmd.AppName + "key.pem"
	}

	x := NewXBus()
	appCtrl := x.NewAppCtrl()
	app := apps.App{Status: utils.StatusOk, Name: cmd.AppName,
		Description: cmd.Description}
	privKey, err := appCtrl.NewApp(&app, nil, strings.Split(cmd.DNSNames, ","), cmd.Days)
	if err != nil {
		glog.Errorf("create app fail: %v", err)
		return subcommands.ExitFailure
	}
	if err := utils.WriteFile(cmd.CertFile, 0644, []byte(app.Cert)); err != nil {
		glog.Errorf("write cert fail: %v", err)
		return subcommands.ExitFailure
	}
	if err := utils.WritePrivateKey(cmd.KeyFile, 0600, privKey); err != nil {
		glog.Errorf("write key fail: %v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
