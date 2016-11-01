package services

import (
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"golang.org/x/net/context"
	"strings"
	"time"
)

type ServiceDesc struct {
	Name        string `json:"name,omitempty"`
	Version     string `json:"version,omitempty"`
	Type        string `json:"type"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
}

func (desc *ServiceDesc) Marshal() ([]byte, error) {
	if data, err := json.Marshal(desc); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal service-desc(%#v) fail: %v", desc, err)
		return nil, utils.NewSystemError("marshal service-desc fail")
	}
}

type ServiceEndpoint struct {
	Address string `json:"address"`
	Config  string `json:"config,omitempty"`
}

func (endpoint *ServiceEndpoint) Marshal() ([]byte, error) {
	if data, err := json.Marshal(endpoint); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal endpoint(%#v) fail: %v", endpoint, err)
		return nil, utils.NewSystemError("marshal endpoint fail")
	}
}

type Service struct {
	Endpoints []ServiceEndpoint `json:"endpoints"`

	ServiceDesc
}

type Config struct {
	KeyPrefix string `default:"/services" yaml:"key_prefix"`
}

type ServiceCtrl struct {
	config     Config
	etcdClient *clientv3.Client
}

func NewServiceCtrl(config *Config, etcdClient *clientv3.Client) *ServiceCtrl {
	services := &ServiceCtrl{config: *config, etcdClient: etcdClient}
	if strings.HasSuffix(services.config.KeyPrefix, "/") {
		services.config.KeyPrefix = services.config.KeyPrefix[:len(services.config.KeyPrefix)-1]
	}
	return services
}

func checkDesc(desc *ServiceDesc) error {
	if err := checkNameVersion(desc.Name, desc.Version); err != nil {
		return err
	}
	if desc.Type == "" {
		return utils.Errorf(utils.EcodeInvalidEndpoint, "%s:%s missing type", desc.Name, desc.Version)
	}
	return nil
}

func (ctrl *ServiceCtrl) Plug(ctx context.Context,
	ttl time.Duration, leaseId clientv3.LeaseID,
	desc *ServiceDesc, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if err := checkDesc(desc); err != nil {
		return 0, err
	}
	if endpoint.Address == "" {
		return 0, utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
	if err := checkAddress(endpoint.Address); err != nil {
		return 0, err
	}

	desc_data, err := desc.Marshal()
	endpoint_data, err := endpoint.Marshal()
	if err != nil {
		return 0, err
	}
	if err := ctrl.ensureServiceDesc(ctx, desc.Name, desc.Version, string(desc_data)); err != nil {
		return 0, err
	}
	return ctrl.setServiceNode(ctx, ttl, leaseId,
		ctrl.serviceKey(desc.Name, desc.Version, endpoint.Address), string(endpoint_data))
}

func (ctrl *ServiceCtrl) Unplug(ctx context.Context, name, version, addr string) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkAddress(addr); err != nil {
		return err
	}
	if _, err := ctrl.etcdClient.Delete(ctx, ctrl.serviceKey(name, version, addr)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", ctrl.serviceKey(name, version, addr), err)
		return utils.NewSystemError("delete key fail")
	}
	return nil
}

func (ctrl *ServiceCtrl) PlugAllService(ctx context.Context,
	ttl time.Duration, leaseId clientv3.LeaseID,
	desces []ServiceDesc, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if endpoint.Address == "" {
		return 0, utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
	if err := checkAddress(endpoint.Address); err != nil {
		return 0, err
	}
	endpoint_data, err := endpoint.Marshal()
	if err != nil {
		return 0, err
	}

	for _, desc := range desces {
		if err := checkDesc(&desc); err != nil {
			return 0, err
		}
		desc_data, err := desc.Marshal()
		if err != nil {
			return 0, err
		}
		if err := ctrl.ensureServiceDesc(ctx, desc.Name, desc.Version, string(desc_data)); err != nil {
			return 0, err
		}
	}
	for _, desc := range desces {
		if leaseId, err = ctrl.setServiceNode(ctx, ttl, leaseId,
			ctrl.serviceKey(desc.Name, desc.Version, endpoint.Address),
			string(endpoint_data)); err != nil {
			return 0, err
		}
	}
	return leaseId, nil
}

func (ctrl *ServiceCtrl) UnplugAllService(ctx context.Context, leaseId clientv3.LeaseID) error {
	if _, err := ctrl.etcdClient.Lease.Revoke(ctx, leaseId); err == nil {
		return nil
	} else {
		return utils.CleanErr(err, "unplug fail", "revoke lease(%v) fail: v", leaseId, err)
	}
}

func (ctrl *ServiceCtrl) Update(ctx context.Context, name, version, addr string, endpoint *ServiceEndpoint) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkAddress(addr); err != nil {
		return err
	}
	key := ctrl.serviceKey(name, version, addr)
	data, err := endpoint.Marshal()
	if err != nil {
		return err
	}

	resp, err := ctrl.etcdClient.Txn(
		ctx,
	).If(
		clientv3.Compare(clientv3.Version(key), ">", 0),
	).Then(clientv3.OpPut(key, string(data))).Commit()
	if err == nil {
		if resp.Succeeded {
			return nil
		}
		return utils.NewError(utils.EcodeNotFound, "")
	} else {
		return utils.CleanErr(err, "update fail", "tnx update(%s) fail: %v", key, err)
	}
}

func (ctrl *ServiceCtrl) Query(ctx context.Context, name, version string) (*Service, int64, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, 0, err
	}
	return ctrl.query(ctx, name, version)
}

func (ctrl *ServiceCtrl) QueryAllVersions(ctx context.Context, name string) (map[string]*Service, int64, error) {
	if err := checkName(name); err != nil {
		return nil, 0, err
	}
	key := ctrl.serviceEntryPrefix(name)
	if resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix()); err == nil {
		if services, err := ctrl.makeAllService(name, resp.Kvs); err == nil {
			return services, resp.Header.Revision, nil
		} else {
			return nil, 0, err
		}
	} else {
		return nil, 0, utils.CleanErr(err, "query fail", "QueryAll(%s) fail: %v", key, err)
	}
}

func (ctrl *ServiceCtrl) query(ctx context.Context, name, version string) (*Service, int64, error) {
	key := ctrl.serviceKeyPrefix(name, version)
	if resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix()); err == nil {
		if service, err := ctrl.makeService(name, version, resp.Kvs); err == nil {
			return service, resp.Header.Revision, nil
		} else {
			return nil, 0, err
		}
	} else {
		return nil, 0, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", key, err)
	}
}

func (ctrl *ServiceCtrl) Watch(ctx context.Context, name, version string,
	revision int64) (*Service, int64, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, 0, err
	}
	key := ctrl.serviceKeyPrefix(name, version)
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	} else {
		watchCh = watcher.Watch(ctx, key, clientv3.WithPrefix())
	}

	resp := <-watchCh
	if !resp.Canceled {
		return ctrl.query(ctx, name, version)
	}
	return nil, 0, nil
}
