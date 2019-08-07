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

// ListAppCmd list app cmd
type ListAppCmd struct{}

// Name cmd name
func (cmd *ListAppCmd) Name() string {
	return "list-app"
}

// Synopsis cmd synopsis
func (cmd *ListAppCmd) Synopsis() string {
	return "list apps"
}

// Usage cmd usage
func (cmd *ListAppCmd) Usage() string {
	return ""
}

// SetFlags cmd set flags
func (cmd *ListAppCmd) SetFlags(f *flag.FlagSet) {

}

const timeFmt = "2006-01-02 15:04:05"

// Execute cmd execute
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
			app.ID, app.Status, app.Name,
			app.CreateTime.Format(timeFmt),
			app.ModifyTime.Format(timeFmt))
	}
	w.Flush()
	return subcommands.ExitSuccess
}
