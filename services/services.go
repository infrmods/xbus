package services

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"regexp"
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

var rValidName = regexp.MustCompile(`(?i)[a-z][a-z0-9_.]{5,}`)
var rValidVersion = regexp.MustCompile(`(?i)[a-z0-9][a-z0-9_.]*`)

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
	return services.newUniqueNode(ctx, ttl, services.etcdKeyPrefix(name, version), string(data))
}

func (services *Services) Unplug(ctx context.Context, name, version, id string) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	if _, err := services.etcdClient.Delete(ctx, services.etcdKey(name, version, id)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", services.etcdKey(name, version, id), err)
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
	key := services.etcdKey(name, version, id)
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
	key := services.etcdKeyPrefix(name, version)
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
	key := services.etcdKeyPrefix(name, version)
	watcher := clientv3.NewWatcher(services.etcdClient)
	defer watcher.Close()
	watchCh := watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	resp := <-watchCh
	if !resp.Canceled {
		return services.query(ctx, key)
	}
	return nil, 0, nil
}
