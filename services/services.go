package services

import (
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"strings"
	"time"
)

type ServiceDesc struct {
	Type  string `json:"type"`
	Proto string `json:"proto,omitempty"`
	Desc  string `json:"desc,omitempty"`
}

func (desc *ServiceDesc) Marshal() ([]byte, error) {
	if data, err := json.Marshal(desc); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal service-desc(%#v) fail: %v", desc, err)
		return nil, utils.NewError(utils.EcodeSystemError, "marshal service-desc fail")
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
		return nil, utils.NewError(utils.EcodeSystemError, "marshal endpoint fail")
	}
}

type Service struct {
	Desc      ServiceDesc       `json:"desc"`
	Endpoints []ServiceEndpoint `json:"endpoints"`
}

type Config struct {
	KeyPrefix string `default:"/services" yaml:"key_prefix"`
}

type ServiceCtrl struct {
	config     Config
	etcdClient *clientv3.Client
}

func NewServices(config *Config, etcdClient *clientv3.Client) *ServiceCtrl {
	services := &ServiceCtrl{config: *config, etcdClient: etcdClient}
	if strings.HasSuffix(services.config.KeyPrefix, "/") {
		services.config.KeyPrefix = services.config.KeyPrefix[:len(services.config.KeyPrefix)-1]
	}
	return services
}

func (ctrl *ServiceCtrl) Plug(ctx context.Context, name, version string,
	ttl time.Duration, desc *ServiceDesc, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if err := checkNameVersion(name, version); err != nil {
		return 0, err
	}
	if desc.Type == "" {
		return 0, utils.NewError(utils.EcodeInvalidEndpoint, "missing type")
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
	if err := ctrl.ensureServiceDesc(ctx, name, version, string(desc_data)); err != nil {
		return 0, err
	}
	return ctrl.newServiceNode(ctx, ttl,
		ctrl.serviceKey(name, version, endpoint.Address), string(endpoint_data))
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
		return utils.NewError(utils.EcodeSystemError, "delete key fail")
	}
	return nil
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

func (ctrl *ServiceCtrl) KeepAlive(ctx context.Context, name, version, addr string, keepId clientv3.LeaseID) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkAddress(addr); err != nil {
		return err
	}
	if _, err := ctrl.etcdClient.Lease.KeepAliveOnce(ctx, keepId); err != nil {
		code, err := utils.CleanErrWithCode(err, "keepalive fail", "KeepAliveOnce(%d) fail: %#v", keepId, err)
		if code == codes.NotFound {
			return utils.NewError(utils.EcodeNotFound, "keepId not found")
		}
		return err
	}
	return nil
}

func (ctrl *ServiceCtrl) Query(ctx context.Context, name, version string) (*Service, int64, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, 0, err
	}
	key := ctrl.serviceKeyPrefix(name, version)
	return ctrl.query(ctx, key)
}

func (ctrl *ServiceCtrl) query(ctx context.Context, key string) (*Service, int64, error) {
	if resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix()); err == nil {
		if service, err := ctrl.makeService(resp.Kvs); err == nil {
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
	watchCh := watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	resp := <-watchCh
	if !resp.Canceled {
		return ctrl.query(ctx, key)
	}
	return nil, 0, nil
}
