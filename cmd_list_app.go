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

type ListAppCmd struct{}

func (cmd *ListAppCmd) Name() string {
	return "list-app"
}

func (cmd *ListAppCmd) Synopsis() string {
	return "list apps"
}

func (cmd *ListAppCmd) Usage() string {
	return ""
}

func (cmd *ListAppCmd) SetFlags(f *flag.FlagSet) {

}

const TIME_FMT = "2006-01-02 15:04:05"

func (cmd *ListAppCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	apps, err := apps.GetAppList(db)
	if err != nil {
		glog.Errorf("get app list fail: %v", err)
		return subcommands.ExitFailure
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "id\tstatus\tname\tcreate time\tmodify time\n")
	for _, app := range apps {
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\n",
			app.Id, app.Status, app.Name,
			app.CreateTime.Format(TIME_FMT),
			app.ModifyTime.Format(TIME_FMT))
	}
	w.Flush()
	return subcommands.ExitSuccess
}
