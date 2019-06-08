package services

import (
	"fmt"
	"regexp"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"golang.org/x/net/context"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)
var rValidService = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}:[a-z0-9][a-z0-9_.-]*$`)
var rValidZone = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_-]*$`)

func checkName(name string) error {
	if !rValidName.MatchString(name) {
		return utils.NewError(utils.EcodeInvalidName, "")
	}
	return nil
}

func checkService(service string) error {
	if !rValidService.MatchString(service) {
		return utils.NewError(utils.EcodeInvalidService, "")
	}
	return nil
}

func checkServiceZone(service, zone string) error {
	if !rValidService.MatchString(service) {
		return utils.NewError(utils.EcodeInvalidService, "")
	}
	if !rValidZone.MatchString(zone) {
		return utils.NewError(utils.EcodeInvalidZone, "")
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

const serviceDescNodeKey = "desc"

func (ctrl *ServiceCtrl) serviceDescKey(service, zone string) string {
	return fmt.Sprintf("%s/%s/%s/desc", ctrl.config.KeyPrefix, service, zone)
}

const serviceKeyNodePrefix = "node_"

func (ctrl *ServiceCtrl) serviceKey(service, zone, addr string) string {
	return fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, service, zone, addr)
}

func (ctrl *ServiceCtrl) ensureServiceDesc(ctx context.Context, service, zone, value string) error {
	key := ctrl.serviceDescKey(service, zone)
	txn := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "=", value)).Then().Else(clientv3.OpPut(key, value))
	if _, err := txn.Commit(); err != nil {
		return utils.CleanErr(err, "put service-desc fail", "exec service-desc txn fail: %v", err)
	}
	return nil
}

func (ctrl *ServiceCtrl) setServiceNode(ctx context.Context, ttl time.Duration, leaseID clientv3.LeaseID,
	key, value string) (_ clientv3.LeaseID, rerr error) {
	if ttl > 0 && leaseID == 0 {
		if resp, err := ctrl.etcdClient.Lease.Grant(ctx, int64(ttl.Seconds())); err == nil {
			leaseID = clientv3.LeaseID(resp.ID)
			defer func() {
				if rerr != nil {
					leaseID = 0
					if _, err := ctrl.etcdClient.Revoke(context.Background(), leaseID); err != nil {
						glog.Errorf("revoke lease(%d) fail: %v", leaseID, err)
					}
				}
			}()
		} else {
			return 0, utils.CleanErr(err, "create lease fail", "create lease fail: %v", err)
		}
	}

	var opPut clientv3.Op
	if leaseID > 0 {
		opPut = clientv3.OpPut(key, value, clientv3.WithLease(leaseID))
	} else {
		opPut = clientv3.OpPut(key, value)
	}
	txn := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "=", value),
		clientv3.Compare(clientv3.LeaseValue(key), "=", leaseID)).Then().Else(opPut)
	if _, err := txn.Commit(); err != nil {
		return 0, utils.CleanErr(err, "plug service fail",
			"put service(%s) node fail: %v", key, err)
	}

	return leaseID, nil
}
