package service

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"strconv"
	"time"
)

const (
	MAX_NEW_UNIQUE_TRY = 5
)

func (xbus *XBus) etcdKeyPrefix(name, version string) string {
	return fmt.Sprintf("%s/%s/%s/", xbus.config.KeyPrefix, name, version)
}

func (xbus *XBus) etcdKey(name, version, id string) string {
	return fmt.Sprintf("%s/%s/%s/%s", xbus.config.KeyPrefix, name, version, id)
}

func (xbus *XBus) newUniqueEphemeralNode(ctx context.Context, ttl time.Duration,
	prefix string, value string) (string, clientv3.LeaseID, error) {
	for tried := 0; tried < MAX_NEW_UNIQUE_TRY; tried++ {
		var leaseId clientv3.LeaseID
		if rep, err := xbus.etcdClient.Lease.Create(ctx, int64(ttl.Seconds())); err == nil {
			leaseId = clientv3.LeaseID(rep.ID)
		} else {
			glog.Errorf("create lease fail: %v", err)
			return "", 0, comm.NewError(comm.EcodeSystemError, "create lease fail")
		}

		id := strconv.FormatInt(time.Now().UnixNano(), 16)
		key := fmt.Sprintf("%s/%s", prefix, id)
		cmp := clientv3.Compare(clientv3.Version(key), "=", 0)
		opPut := clientv3.OpPut(key, value, clientv3.WithLease(leaseId))
		if resp, err := xbus.etcdClient.Txn(ctx).If(cmp).Then(opPut).Commit(); err != nil {
			glog.Errorf("Txn(create unique key(%s)) fail: %v", key, err)
			return "", 0, comm.NewError(comm.EcodeSystemError, "create unique key fail")
		} else if resp.Succeeded {
			return id, leaseId, nil
		} else {
			if _, err := xbus.etcdClient.Revoke(context.Background(), leaseId); err != nil {
				glog.Errorf("revoke lease(%v) fail: %v", leaseId, err)
				return "", 0, comm.NewError(comm.EcodeSystemError, "revoke lease fail")
			}
		}
	}
	return "", 0, comm.NewError(comm.EcodeLoopExceeded, "tried too many times(newUniqueEphemeralNode)")
}
