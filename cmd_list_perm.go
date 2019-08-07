package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
)

// ListPermCmd cmd list perm
type ListPermCmd struct {
	isConfigs  bool
	isServices bool
	isApps     bool
	appName    string
	groupName  string
	canWrite   bool
	prefix     string
}

// Name cmd name
func (cmd *ListPermCmd) Name() string {
	return "list-perm"
}

// Synopsis cmd synopsis
func (cmd *ListPermCmd) Synopsis() string {
	return "list perms"
}

// Usage cmd usage
func (cmd *ListPermCmd) Usage() string {
	return ""
}

// SetFlags cmd set flags
func (cmd *ListPermCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&cmd.isConfigs, "configs", false, "list config perms")
	f.BoolVar(&cmd.isServices, "services", false, "list services perms")
	f.BoolVar(&cmd.isApps, "apps", false, "list app perms")
	f.StringVar(&cmd.appName, "app", "", "app name")
	f.StringVar(&cmd.groupName, "group", "", "group name")
	f.BoolVar(&cmd.canWrite, "write", false, "need write")
	f.StringVar(&cmd.prefix, "prefix", "", "content prefix")
}

// Execute cmd execute
func (cmd *ListPermCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	appCtrl := x.NewAppCtrl(db, x.NewEtcdClient())

	appList, err := apps.GetAppList(db)
	if err != nil {
		glog.Errorf("get apps fail: %v", err)
		return subcommands.ExitFailure
	}
	appMap := make(map[int64]apps.App)
	for _, app := range appList {
		appMap[app.ID] = app
	}
	groupList, err := apps.GetGroupList(db)
	if err != nil {
		glog.Errorf("get groups fail: %v", err)
		return subcommands.ExitFailure
	}
	groupMap := make(map[int64]*apps.Group)
	for _, group := range groupList {
		groupMap[group.ID] = &group
	}

	var typ int
	var appName *string
	var groupName *string
	var canWrite *bool
	var prefix *string
	if cmd.isConfigs {
		typ = apps.PermTypeConfig
	} else if cmd.isServices {
		typ = apps.PermTypeService
	} else if cmd.isApps {
		typ = apps.PermTypeApp
	} else {
		typ = apps.PermTypeConfig
	}
	if cmd.appName != "" {
		appName = &cmd.appName
	} else if cmd.groupName != "" {
		groupName = &cmd.groupName
	}
	if cmd.prefix != "" {
		prefix = &cmd.prefix
	}
	f.Visit(func(x *flag.Flag) {
		if x.Name == "write" {
			canWrite = &cmd.canWrite
		}
	})
	if perms, err := appCtrl.GetPerms(typ, appName, groupName, canWrite, prefix); err == nil {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "id\ttype\ttarget_type\ttarget\twrite\tcontent\tcreate_time\n")
		for _, perm := range perms {
			typeName := "unknown"
			targetTypeName := "unknown"
			target := ""
			switch perm.PermType {
			case apps.PermTypeConfig:
				typeName = "config"
			case apps.PermTypeService:
				typeName = "service"
			case apps.PermTypeApp:
				typeName = "app"
			}
			switch perm.TargetType {
			case apps.PermTargetApp:
				targetTypeName = "app"
				if perm.TargetID == apps.PermPublicTargetID {
					targetTypeName = "<public>"
					target = "<public>"
				} else {
					targetTypeName = "app"
					if app, exists := appMap[perm.TargetID]; !exists {
						target = fmt.Sprintf("<invalid: %d>", perm.TargetID)
					} else {
						target = fmt.Sprintf("%s[%d]", app.Name, app.ID)
					}
				}
			case apps.PermTargetGroup:
				targetTypeName = "group"
				group := groupMap[perm.TargetID]
				if group == nil {
					target = fmt.Sprintf("<invalid: %d>", perm.TargetID)
				} else {
					target = fmt.Sprintf("%s[%d]", group.Name, group.ID)
				}
			}
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
				perm.ID, typeName, targetTypeName, target,
				perm.CanWrite, perm.Content, perm.CreateTime.Format(timeFmt))
		}
		w.Flush()
	} else {
		glog.Errorf("get perms fail: %v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
