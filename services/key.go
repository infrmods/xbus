package services

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"golang.org/x/net/context"
	"regexp"
	"time"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)
var rValidVersion = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_.-]*$`)

func checkName(name string) error {
	if !rValidName.MatchString(name) {
		return utils.NewError(utils.EcodeInvalidName, "")
	}
	return nil
}

func checkNameVersion(name, version string) error {
	if !rValidName.MatchString(name) {
		return utils.NewError(utils.EcodeInvalidName, "")
	}
	if !rValidVersion.MatchString(version) {
		return utils.NewError(utils.EcodeInvalidVersion, "")
	}
	return nil
}

var rValidAddress = regexp.MustCompile(`(?i)^[a-z0-9:_.-]+$`)

func (ctrl *ServiceCtrl) checkAddress(addr string) error {
	if !rValidAddress.MatchString(addr) {
		return utils.NewError(utils.EcodeInvalidAddress, "")
	}
	if ctrl.config.isAddressBanned(addr) {
		return utils.NewError(utils.EcodeInvalidAddress, "banned")
	}
	return nil
}

func (ctrl *ServiceCtrl) serviceEntryPrefix(name string) string {
	return fmt.Sprintf("%s/%s/", ctrl.config.KeyPrefix, name)
}

func (ctrl *ServiceCtrl) serviceKeyPrefix(name, version string) string {
	return fmt.Sprintf("%s/%s/%s", ctrl.config.KeyPrefix, name, version)
}

var serviceDescNodeKey = "desc"

func (ctrl *ServiceCtrl) serviceDescKey(name, version string) string {
	return fmt.Sprintf("%s/%s/%s/desc", ctrl.config.KeyPrefix, name, version)
}

var serviceKeyNodePrefix = "node_"

func (ctrl *ServiceCtrl) serviceKey(name, version, addr string) string {
	return fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, name, version, addr)
}

const MAX_NEW_UNIQUE_TRY = 3

func (ctrl *ServiceCtrl) ensureServiceDesc(ctx context.Context, name, version, value string) error {
	key := ctrl.serviceDescKey(name, version)
	txn := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "=", value)).Then().Else(clientv3.OpPut(key, value))
	if _, err := txn.Commit(); err != nil {
		return utils.CleanErr(err, "put service-desc fail", "exec service-desc txn fail: %v", err)
	}
	return nil
}

func (ctrl *ServiceCtrl) setServiceNode(ctx context.Context, ttl time.Duration, leaseId clientv3.LeaseID,
	key, value string) (_ clientv3.LeaseID, rerr error) {
	if ttl > 0 && leaseId == 0 {
		if resp, err := ctrl.etcdClient.Lease.Grant(ctx, int64(ttl.Seconds())); err == nil {
			leaseId = clientv3.LeaseID(resp.ID)
			defer func() {
				if rerr != nil {
					leaseId = 0
					if _, err := ctrl.etcdClient.Revoke(context.Background(), leaseId); err != nil {
						glog.Errorf("revoke lease(%d) fail: %v", leaseId, err)
					}
				}
			}()
		} else {
			return 0, utils.CleanErr(err, "create lease fail", "create lease fail: %v", err)
		}
	}

	txn := ctrl.etcdClient.Txn(ctx).If(clientv3.Compare(clientv3.Value(key), "=", value)).Then()
	if ttl > 0 {
		txn = txn.Else(clientv3.OpPut(key, value, clientv3.WithLease(leaseId)))
	} else {
		txn = txn.Else(clientv3.OpPut(key, value))
	}

	if _, err := txn.Commit(); err != nil {
		return 0, utils.CleanErr(err, "plug service fail",
			"put service(%s) node fail: %v", key, err)
	}
	return leaseId, nil
}
