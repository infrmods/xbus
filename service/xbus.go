package service

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"regexp"
	"strings"
	"time"
)

type Config struct {
	EtcdEndpoints []string      `default:"[\"127.0.0.1:2378\"]" yaml:"etcd_endpoints"`
	EtcdTimeout   time.Duration `default:"5s" yaml:"etcd_timeout"`
	EtcdTLS       *tls.Config   `yaml:"etcd_tls"`
	KeyPrefix     string        `default:"/services/"`
}

type XBus struct {
	config     Config
	etcdClient *clientv3.Client
}

func NewXBus(config *Config) *XBus {
	xbus := &XBus{config: *config}
	if strings.HasSuffix(xbus.config.KeyPrefix, "/") {
		xbus.config.KeyPrefix = xbus.config.KeyPrefix[:len(xbus.config.KeyPrefix)-1]
	}
	return xbus
}

func (xbus *XBus) Init() (err error) {
	etcd_config := clientv3.Config{
		Endpoints:   xbus.config.EtcdEndpoints,
		DialTimeout: xbus.config.EtcdTimeout,
		TLS:         xbus.config.EtcdTLS}
	if xbus.etcdClient, err = clientv3.New(etcd_config); err == nil {
		return nil
	} else {
		return fmt.Errorf("create etcd clientv3 fail(%v)", err)
	}
}

var rValidNameVersion = regexp.MustCompile(`(?i)[a-z_.]+`)

func checkNameVersion(name, version string) error {
	if !rValidNameVersion.MatchString(name) {
		return comm.NewError(comm.EcodeInvalidName, "")
	}
	if !rValidNameVersion.MatchString(version) {
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

func (xbus *XBus) Plug(ctx context.Context, name, version string,
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
	data, err := json.Marshal(endpoint)
	if err != nil {
		glog.Errorf("marchal endpoint(%#v) fail: %v", endpoint, err)
		return "", 0, comm.NewError(comm.EcodeSystemError, "marchal endpoint fail")
	}
	return xbus.newUniqueEphemeralNode(ctx, ttl, xbus.etcdKeyPrefix(name, version), string(data))
}

func (xbus *XBus) Unplug(ctx context.Context, name, version, id string) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	if _, err := xbus.etcdClient.Delete(ctx, xbus.etcdKey(name, version, id)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", xbus.etcdKey(name, version, id), err)
		return comm.NewError(comm.EcodeSystemError, "delete key fail")
	}
	return nil
}

func (xbus *XBus) Keepalive(ctx context.Context, name, version, id string, keepId clientv3.LeaseID) error {
	if err := checkNameVersion(name, version); err != nil {
		return err
	}
	if err := checkServiceId(id); err != nil {
		return err
	}
	if _, err := xbus.etcdClient.Lease.KeepAliveOnce(ctx, keepId); err != nil {
		// TODO: err detail
		return comm.NewError(comm.EcodeMissing, "")
	}
	return nil
}

func (xbus *XBus) Query(ctx context.Context, name, version string) ([]comm.ServiceEndpoint, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, err
	}
	return nil, nil
}

func (xbux *XBus) Watch(ctx context.Context, name, version string, timeout time.Duration) ([]comm.ServiceEndpoint, error) {
	if err := checkNameVersion(name, version); err != nil {
		return nil, err
	}
	return nil, nil
}
