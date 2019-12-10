package services

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/utils"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)
var rValidService = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}:[a-z0-9][a-z0-9_.-]*$`)
var rValidZone = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_-]{3,}$`)

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
	if addr == "" {
		return utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
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

func (ctrl *ServiceCtrl) extensionKey(ext, zone, service string) string {
	return fmt.Sprintf("%s-extensions/%s/%s/%s", ctrl.config.KeyPrefix, ext, zone, service)
}

func (ctrl *ServiceCtrl) extensionKeyPrefix(ext, zone string) string {
	if zone == "" {
		return fmt.Sprintf("%s-extensions/%s", ctrl.config.KeyPrefix, ext)
	}
	return fmt.Sprintf("%s-extensions/%s/%s", ctrl.config.KeyPrefix, ext, zone)
}

type extensionKey struct {
	Extension string
	Zone      string
	Service   string
}

func (ctrl *ServiceCtrl) splitExtensionKey(key string) *extensionKey {
	parts := strings.Split(key, "/")
	if len(parts) < 4 {
		return nil
	}
	len := len(parts)
	return &extensionKey{Extension: parts[len-3], Zone: parts[len-2], Service: parts[len-1]}
}

func (ctrl *ServiceCtrl) ensureServiceDesc(ctx context.Context, service, zone, value string) error {
	key := ctrl.serviceDescKey(service, zone)
	txn := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "!=", value)).Then(clientv3.OpPut(key, value))
	if _, err := txn.Commit(); err != nil {
		return utils.CleanErr(err, "put service-desc fail", "exec service-desc txn fail: %v", err)
	}
	return nil
}

func (ctrl *ServiceCtrl) setServiceNode(ctx context.Context, leaseID clientv3.LeaseID,
	desc *ServiceDescV1, addr, value string) (_ clientv3.LeaseID, rerr error) {
	key := ctrl.serviceKey(desc.Service, desc.Zone, addr)
	extKey := ""
	if desc.Extension != "" {
		extKey = ctrl.extensionKey(desc.Extension, desc.Zone, desc.Service)
	}

	var opPut, opPutExt clientv3.Op
	if leaseID > 0 {
		opPut = clientv3.OpPut(key, value, clientv3.WithLease(leaseID))
		if extKey != "" {
			opPutExt = clientv3.OpPut(extKey, "", clientv3.WithLease(leaseID))
		}
	} else {
		opPut = clientv3.OpPut(key, value)
		if extKey != "" {
			opPutExt = clientv3.OpPut(extKey, "")
		}
	}
	txn := ctrl.etcdClient.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "=", value),
		clientv3.Compare(clientv3.LeaseValue(key), "=", leaseID))
	if desc.Extension != "" {
		txn = txn.Then(opPutExt).Else(opPut, opPutExt)
	} else {
		txn.Else(opPut)
	}
	if _, err := txn.Commit(); err != nil {
		return 0, utils.CleanErr(err, "plug service fail",
			"put service(%s) node fail: %v", key, err)
	}

	return leaseID, nil
}
