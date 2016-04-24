package configs

import (
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"strings"
)

type Config struct {
	KeyPrefix string `default:"/apis" yaml:"key_prefix"`
}

type Configs struct {
	config     Config
	etcdClient *clientv3.Client
}

func NewConfigs(config *Config, etcdClient *clientv3.Client) *Configs {
	configs := &Configs{config: *config, etcdClient: etcdClient}
	if strings.HasSuffix(configs.config.KeyPrefix, "/") {
		configs.config.KeyPrefix = configs.config.KeyPrefix[:len(configs.config.KeyPrefix)-1]
	}
	return configs
}

func (configs *Configs) Get(ctx context.Context, name string) (string, int64, error) {
	return "", 0, nil
}

func (configs *Configs) Put(ctx context.Context, name, value string) error {
	return nil
}

func (configs *Configs) Watch(ctx context.Context, name string) (string, int64, error) {
	return "", 0, nil
}
