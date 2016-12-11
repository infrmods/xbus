package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/apps"
	"golang.org/x/net/context"
	"os"
	"text/tabwriter"
)

type ListPerm struct {
	isConfigs  bool
	isServices bool
	isApps     bool
	appName    string
	groupName  string
	canWrite   bool
	prefix     string
}

func (cmd *ListPerm) Name() string {
	return "list-perm"
}

func (cmd *ListPerm) Synopsis() string {
	return "list perms"
}

func (cmd *ListPerm) Usage() string {
	return ""
}

func (cmd *ListPerm) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&cmd.isConfigs, "configs", false, "list config perms")
	f.BoolVar(&cmd.isServices, "services", false, "list services perms")
	f.BoolVar(&cmd.isApps, "apps", false, "list app perms")
	f.StringVar(&cmd.appName, "app", "", "app name")
	f.StringVar(&cmd.groupName, "group", "", "group name")
	f.BoolVar(&cmd.canWrite, "write", false, "need write")
	f.StringVar(&cmd.prefix, "prefix", "", "content prefix")
}

func (cmd *ListPerm) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	app_ctrl := x.NewAppCtrl(db)

	app_list, err := apps.GetAppList(db)
	if err != nil {
		glog.Errorf("get apps fail: %v", err)
		return subcommands.ExitFailure
	}
	app_map := make(map[int64]*apps.App)
	for _, app := range app_list {
		app_map[app.Id] = &app
	}
	group_list, err := apps.GetGroupList(db)
	if err != nil {
		glog.Errorf("get groups fail: %v", err)
		return subcommands.ExitFailure
	}
	group_map := make(map[int64]*apps.Group)
	for _, group := range group_list {
		group_map[group.Id] = &group
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
	canWrite = &cmd.canWrite
	if perms, err := app_ctrl.GetPerms(typ, appName, groupName, canWrite, prefix); err == nil {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "id\ttype\ttarget_type\ttarget\twrite\tcontent\tcreate_time\n")
		for _, perm := range perms {
			type_name := "unknown"
			target_type_name := "unknown"
			target := ""
			switch perm.PermType {
			case apps.PermTypeConfig:
				type_name = "config"
			case apps.PermTypeService:
				type_name = "service"
			case apps.PermTypeApp:
				type_name = "app"
			}
			switch perm.TargetType {
			case apps.PermTargetApp:
				target_type_name = "app"
				if perm.TargetId == apps.PermPublicTargetId {
					target_type_name = "<public>"
					target = "<public>"
				} else {
					target_type_name = "app"
					app := app_map[perm.TargetId]
					if app == nil {
						target = fmt.Sprintf("<invalid: %d>", perm.TargetId)
					} else {
						target = fmt.Sprintf("%s[%d]", app.Name, app.Id)
					}
				}
			case apps.PermTargetGroup:
				target_type_name = "group"
				group := group_map[perm.TargetId]
				if group == nil {
					target = fmt.Sprintf("<invalid: %d>", perm.TargetId)
				} else {
					target = fmt.Sprintf("%s[%d]", group.Name, group.Id)
				}
			}
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
				perm.Id, type_name, target_type_name, target,
				perm.CanWrite, perm.Content, perm.CreateTime.Format(TIME_FMT))
		}
		w.Flush()
	} else {
		glog.Errorf("get perms fail: %v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
