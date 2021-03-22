package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"time"

	"context"

	"github.com/golang/glog"
	"github.com/google/subcommands"
)

// FixCmd run cmd
type FixCmd struct {
}

// Name cmd name
func (cmd *FixCmd) Name() string {
	return "fix"
}

// Synopsis cmd synopsis
func (cmd *FixCmd) Synopsis() string {
	return "fix proto"
}

// SetFlags cmd set flags
func (cmd *FixCmd) SetFlags(f *flag.FlagSet) {
}

// Usage cmd usgae
func (cmd *FixCmd) Usage() string {
	return ""
}

func (cmd *FixCmd) serviceM5NotifyKey(service, zone string) string {
	return fmt.Sprintf("/services-md5s/%s/%s", zone, service)
}

// Execute cmd execute
func (cmd *FixCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	x := NewXBus()
	db := x.NewDB()
	etcdClient := x.Config.Etcd.NewEtcdClient()
	start := 0
	total := -1
	if rows, err := db.Query(`select count(1) from services`); err == nil {
		for rows.Next() {
			if err := rows.Scan(&total); err != nil {
				println(err)
				return subcommands.ExitSuccess
			}
		}
	} else {
		println(err)
		return subcommands.ExitSuccess
	}
	println(total)
	for start <= total {
		if rows, err := db.Query(`select service, zone, typ, proto, description, proto_md5 from services 
				order by modify_time desc limit ?,?`, start, 1000); err == nil {
			for rows.Next() {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				var service, zone, typ, proto, description, md55 string
				if err := rows.Scan(&service, &zone, &typ, &proto, &description, &md55); err != nil {
					println(err)
					continue
				}
				w := md5.New()
				io.WriteString(w, proto)
				md5Tmp := fmt.Sprintf("%x", w.Sum(nil))
				println(md5Tmp)
				key := cmd.serviceM5NotifyKey(service, zone)
				println(key)
				_, err := etcdClient.Put(ctx, key, md5Tmp)
				if err != nil {
					glog.Infof("get proto switch fail %v", err)
				} else {

				}
			}

		} else {
			println(err)
			return subcommands.ExitSuccess
		}
		start += 1000
	}
	return subcommands.ExitSuccess
}
