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

// ListGroupCmd list group cmd
type ListGroupCmd struct{}

// Name cmd name
func (cmd *ListGroupCmd) Name() string {
	return "list-group"
}

// Synopsis cmd synopsis
func (cmd *ListGroupCmd) Synopsis() string {
	return "list groups"
}

// Usage cmd usage
func (cmd *ListGroupCmd) Usage() string {
	return ""
}

// SetFlags cmd set flags
func (cmd *ListGroupCmd) SetFlags(f *flag.FlagSet) {

}

// Execute cmd execute
func (cmd *ListGroupCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	groups, err := apps.GetGroupList(db)
	if err != nil {
		glog.Errorf("get app list fail: %v", err)
		return subcommands.ExitFailure
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "id\tstatus\tname\tcreate time\tmodify time\n")
	for _, group := range groups {
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\n",
			group.ID, group.Status, group.Name,
			group.CreateTime.Format(timeFmt),
			group.ModifyTime.Format(timeFmt))
	}
	w.Flush()
	return subcommands.ExitSuccess
}
