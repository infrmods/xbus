package services

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"strings"
	"time"
)

type Config struct {
	KeyPrefix string `default:"/services" yaml:"key_prefix"`
}

type Services struct {
	config     Config
	etcdClient *clientv3.Client
}

func NewServices(config *Config, etcdClient *clientv3.Client) *Services {
	services := &Services{config: *config, etcdClient: etcdClient}
	if strings.HasSuffix(services.config.KeyPrefix, "/") {
		services.config.KeyPrefix = services.config.KeyPrefix[:len(services.config.KeyPrefix)-1]
	}
	return services
}

func (services *Services) Plug(ctx context.Context, name, version string,
	ttl time.Duration, endpoint *comm.ServiceEndpoint) (string, clientv3.LeaseID, error) {
	if err := checkNameVersion(name, version); err != nil {
		return "", 0, err
	}
	if endpoint.Type == "" {
		return "", 0, comm.NewError(comm.EcodeInvalidEndpoint, "missing type")
	}
	if endpoint.Address == "" {
		return "", 0, comm.NewError(comm.EcodeInvalidEndpoint, "missing address")
	}
	data, err := endpoint.Marshal()
	if err != nil {
		return "", 0, err
	}
	return services.newUniqueNode(ctx, ttl, services.serviceKeyPrefix(name, version), string(data))
}

func (services *Services) Unplug(ctx context.Context, name, version, id string) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	if _, err := services.etcdClient.Delete(ctx, services.serviceKey(name, version, id)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", services.serviceKey(name, version, id), err)
		return comm.NewError(comm.EcodeSystemError, "delete key fail")
	}
	return nil
}

func (services *Services) Update(ctx context.Context, name, version, id string, endpoint *comm.ServiceEndpoint) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	key := services.serviceKey(name, version, id)
	data, err := endpoint.Marshal()
	if err != nil {
		return err
	}

	resp, err := services.etcdClient.Txn(
		ctx,
	).If(
		clientv3.Compare(clientv3.Version(key), ">", 0),
	).Then(clientv3.OpPut(key, string(data))).Commit()
	if err == nil {
		if resp.Succeeded {
			return nil
		}
		return comm.NewError(comm.EcodeNotFound, "")
	} else {
		return comm.CleanErr(err, "update fail", "tnx update(%s) fail: %v", key, err)
	}
}

func (services *Services) KeepAlive(ctx context.Context, name, version, id string, keepId clientv3.LeaseID) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	if _, err := services.etcdClient.Lease.KeepAliveOnce(ctx, keepId); err != nil {
		return comm.CleanErr(err, "keepalive fail", "KeepAliveOnce(%d) fail: %v", keepId, err)
	}
	return nil
}

func (services *Services) Query(ctx context.Context, name, version string) ([]comm.ServiceEndpoint, int64, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, 0, err
	}
	key := services.serviceKeyPrefix(name, version)
	return services.query(ctx, key)
}

func (services *Services) query(ctx context.Context, key string) ([]comm.ServiceEndpoint, int64, error) {
	if resp, err := services.etcdClient.Get(ctx, key, clientv3.WithFromKey()); err == nil {
		if endpoints, err := services.makeEndpoints(resp.Kvs); err == nil {
			return endpoints, resp.Header.Revision, nil
		} else {
			return nil, 0, err
		}
	} else {
		return nil, 0, comm.CleanErr(err, "query fail", "Query(%s) fail: %v", key, err)
	}
}

func (services *Services) Watch(ctx context.Context, name, version string,
	revision int64) ([]comm.ServiceEndpoint, int64, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, 0, err
	}
	key := services.serviceKeyPrefix(name, version)
	watcher := clientv3.NewWatcher(services.etcdClient)
	defer watcher.Close()
	watchCh := watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	resp := <-watchCh
	if !resp.Canceled {
		return services.query(ctx, key)
	}
	return nil, 0, nil
}
