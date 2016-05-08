package services

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"regexp"
	"strconv"
	"time"
)

var rValidName = regexp.MustCompile(`(?i)[a-z][a-z0-9_.-]{5,}`)
var rValidVersion = regexp.MustCompile(`(?i)[a-z0-9][a-z0-9_.-]*`)

func checkNameVersion(name, version string) error {
	if !rValidName.MatchString(name) {
		return comm.NewError(comm.EcodeInvalidName, "")
	}
	if !rValidVersion.MatchString(version) {
		return comm.NewError(comm.EcodeInvalidVersion, "")
	}
	return nil
}

var rValidServiceId = regexp.MustCompile(`(?i)[a-f0-9]+`)

func checkServiceId(id string) error {
	if !rValidServiceId.MatchString(id) {
		return comm.NewError(comm.EcodeInvalidServiceId, "")
	}
	return nil
}

func (services *Services) serviceKeyPrefix(name, version string) string {
	return fmt.Sprintf("%s/%s/%s", services.config.KeyPrefix, name, version)
}

func (services *Services) serviceKey(name, version, id string) string {
	return fmt.Sprintf("%s/%s/%s/%s", services.config.KeyPrefix, name, version, id)
}

const MAX_NEW_UNIQUE_TRY = 5

func (services *Services) newUniqueNode(ctx context.Context, ttl time.Duration,
	prefix string, value string) (node string, leaseId clientv3.LeaseID, rerr error) {
	if ttl > 0 {
		if resp, err := services.etcdClient.Lease.Create(ctx, int64(ttl.Seconds())); err == nil {
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
			return "", 0, comm.CleanErr(err, "create lease fail", "create lease fail: %v", err)
		}
	}

	for tried := 0; tried < MAX_NEW_UNIQUE_TRY; tried++ {
		id := strconv.FormatInt(time.Now().UnixNano(), 16)
		key := fmt.Sprintf("%s/%s", prefix, id)
		cmp := clientv3.Compare(clientv3.Version(key), "=", 0)
		var opPut clientv3.Op
		if ttl > 0 {
			opPut = clientv3.OpPut(key, value, clientv3.WithLease(leaseId))
		} else {
			opPut = clientv3.OpPut(key, value)
		}

		if resp, err := services.etcdClient.Txn(ctx).If(cmp).Then(opPut).Commit(); err != nil {
			return "", 0, comm.CleanErr(err, "create unique key fail",
				"Txn(create unique key(%s)) fail: %v", key, err)
		} else if resp.Succeeded {
			return id, leaseId, nil
		}
	}

	return "", 0, comm.NewError(comm.EcodeLoopExceeded, "tried too many times(newUniqueEphemeralNode)")
}
