package services

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"regexp"
	"time"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)
var rValidVersion = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_.-]*$`)

func checkNameVersion(name, version string) error {
	if !rValidName.MatchString(name) {
		return comm.NewError(comm.EcodeInvalidName, "")
	}
	if !rValidVersion.MatchString(version) {
		return comm.NewError(comm.EcodeInvalidVersion, "")
	}
	return nil
}

var rValidAddress = regexp.MustCompile(`(?i)^[a-z0-9:_-]+$`)

func checkAddress(addr string) error {
	if !rValidAddress.MatchString(addr) {
		return comm.NewError(comm.EcodeInvalidAddress, "")
	}
	return nil
}

func (services *Services) serviceKeyPrefix(name, version string) string {
	return fmt.Sprintf("%s/%s/%s", services.config.KeyPrefix, name, version)
}

func (services *Services) serviceKey(name, version, addr string) string {
	return fmt.Sprintf("%s/%s/%s/%s", services.config.KeyPrefix, name, version, addr)
}

const MAX_NEW_UNIQUE_TRY = 5

func (services *Services) newServiceNode(ctx context.Context, ttl time.Duration,
	key, value string) (leaseId clientv3.LeaseID, rerr error) {
	if ttl > 0 {
		if resp, err := services.etcdClient.Lease.Grant(ctx, int64(ttl.Seconds())); err == nil {
			leaseId = clientv3.LeaseID(resp.ID)
			defer func() {
				if rerr != nil {
					leaseId = 0
					if _, err := services.etcdClient.Revoke(context.Background(), leaseId); err != nil {
						glog.Errorf("revoke lease(%d) fail: %v", leaseId, err)
					}
				}
			}()
		} else {
			return 0, comm.CleanErr(err, "create lease fail", "create lease fail: %v", err)
		}
	}

	var err error
	if ttl > 0 {
		_, err = services.etcdClient.Put(ctx, key, value, clientv3.WithLease(leaseId))
	} else {
		_, err = services.etcdClient.Put(ctx, key, value)
	}

	if err != nil {
		return 0, comm.CleanErr(err, "create unique key fail",
			"Txn(create unique key(%s)) fail: %v", key, err)
	}
	return leaseId, nil
}
