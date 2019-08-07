package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
)

// GrantCmd grant cmd
type GrantCmd struct {
	isConfigs  bool
	isServices bool
	isApps     bool
	isApp      bool
	isGroup    bool
	canWrite   bool
}

// Name cmd name
func (cmd *GrantCmd) Name() string {
	return "grant"
}

// Synopsis cmd synopsis
func (cmd *GrantCmd) Synopsis() string {
	return "grant permission"
}

// Usage cmd usage
func (cmd *GrantCmd) Usage() string {
	return "grant [OPTIONS] target content\n"
}

// SetFlags cmd set flags
func (cmd *GrantCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&cmd.isConfigs, "configs", false, "list config perms")
	f.BoolVar(&cmd.isServices, "services", false, "list services perms")
	f.BoolVar(&cmd.isApps, "apps", false, "list app perms")
	f.BoolVar(&cmd.isApp, "app", false, "target is app")
	f.BoolVar(&cmd.isGroup, "group", false, "target is group")
	f.BoolVar(&cmd.canWrite, "write", false, "need write")
}

// Execute cmd execute
func (cmd *GrantCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	args := f.Args()
	if len(args) != 2 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	perm := apps.Perm{}
	if cmd.isApps {
		perm.PermType = apps.PermTypeApp
	} else if cmd.isServices {
		perm.PermType = apps.PermTypeService
	} else {
		perm.PermType = apps.PermTypeConfig
	}
	if cmd.isGroup {
		perm.TargetType = apps.PermTargetGroup
		if group, err := apps.GetGroupByName(db, args[0]); err == nil {
			if group == nil {
				fmt.Printf("no such group: %s\n", args[0])
				return subcommands.ExitFailure
			}
			perm.TargetID = group.ID
		} else {
			glog.Errorf("get group(%v) fail: %v", args[0], err)
			return subcommands.ExitFailure
		}
	} else {
		perm.TargetType = apps.PermTargetApp
		if args[0] == "public" {
			perm.TargetID = apps.PermPublicTargetID
		} else {
			if app, err := apps.GetAppByName(db, args[0]); err == nil {
				if app == nil {
					fmt.Printf("no such app: %s\n", args[0])
					return subcommands.ExitFailure
				}
				perm.TargetID = app.ID
			} else {
				glog.Errorf("get app(%v), fail: %v", args[0], err)
				return subcommands.ExitFailure
			}
		}
	}
	perm.CanWrite = cmd.canWrite
	perm.Content = args[1]

	if err := apps.InsertPerm(db, &perm); err != nil {
		glog.Errorf("new perm fail: %v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
