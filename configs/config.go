package configs

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/storage/storagepb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
	"strings"
)

type Config struct {
	KeyPrefix string `default:"/configs" yaml:"key_prefix"`
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

const RANGE_LIMIT = 20

func (configs *Configs) Range(ctx context.Context, from, end string, sortOption *clientv3.SortOption) ([]comm.Config, bool, error) {
	fromKey := configs.configKey(from)
	endKey := configs.configKey(end)
	if end == "" {
		endKey = configs.endKey()
	}

	if resp, err := configs.etcdClient.Get(
		ctx, fromKey, clientv3.WithRange(endKey),
		clientv3.WithSort(sortOption.Target, sortOption.Order),
		clientv3.WithLimit(RANGE_LIMIT)); err == nil {
		cfgs := make([]comm.Config, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			if strings.HasPrefix(key, configs.config.KeyPrefix) {
				name := key[len(configs.config.KeyPrefix)+1:]
				cfgs = append(cfgs, configFromKv(name, kv))
			} else {
				glog.Errorf("invalid key from range(%s, %s): %s", from, end, key)
			}
		}
		return cfgs, resp.More, nil
	} else {
		return nil, false, comm.CleanErr(err, "", "get range(%s, %s) fail: %v", from, end, err)
	}
}

func (configs *Configs) Get(ctx context.Context, name string) (*comm.Config, int64, error) {
	if err := checkName(name); err != nil {
		return nil, 0, err
	}

	if resp, err := configs.etcdClient.Get(ctx, configs.configKey(name)); err == nil {
		if resp.Kvs == nil {
			return nil, 0, comm.NewError(comm.EcodeNotFound, "")
		}
		cfg := configFromKv(name, resp.Kvs[0])
		return &cfg, resp.Header.Revision, nil
	} else {
		return nil, 0, comm.CleanErr(err, "", "get config key(%s) fail: %v", name, err)
	}
}

func configFromKv(name string, kv *storagepb.KeyValue) comm.Config {
	return comm.Config{Name: name,
		Value:   string(kv.Value),
		Version: kv.Version}
}

func (configs *Configs) Put(ctx context.Context, name, value string, version int64) (int64, error) {
	if err := checkName(name); err != nil {
		return 0, err
	}
	key := configs.configKey(name)
	if version < 0 {
		if resp, err := configs.etcdClient.Put(ctx, key, value); err == nil {
			return resp.Header.Revision, nil
		} else {
			return 0, comm.CleanErr(err, "", "put config key(%s) fail: %v", name, err)
		}
	} else {
		cmp := clientv3.Compare(clientv3.Version(key), "=", version)
		opPut := clientv3.OpPut(key, value)
		if resp, err := configs.etcdClient.Txn(ctx).If(cmp).Then(opPut).Commit(); err == nil {
			return 0, comm.CleanErr(err, "", "put config key(%s) with version(%d) fail: %v", name, version, err)
		} else if !resp.Succeeded {
			return 0, comm.NewError(comm.EcodeInvalidVersion, "")
		} else {
			return resp.Header.Revision, nil
		}
	}
}

func (configs *Configs) Watch(ctx context.Context, name string, revision int64) (*comm.Config, int64, error) {
	if err := checkName(name); err != nil {
		return nil, 0, err
	}
	watcher := clientv3.NewWatcher(configs.etcdClient)
	defer watcher.Close()

	key := configs.configKey(name)
	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision))
	} else {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision))
	}
	resp := <-watchCh
	if err := resp.Err(); err != nil {
		return nil, 0, comm.CleanErr(err, "", "watch key(%s) with revision(%d) fail: %v", name, revision, err)
	}
	if resp.Canceled || resp.Events == nil {
		return nil, resp.Header.Revision, nil
	}
	for _, event := range resp.Events {
		switch event.Type {
		case storagepb.PUT:
			cfg := configFromKv(name, event.Kv)
			return &cfg, resp.Header.Revision, nil
		case storagepb.EXPIRE, storagepb.DELETE:
			return nil, 0, comm.NewError(comm.EcodeDeleted, "")
		}
	}
	return nil, 0, comm.NewError(comm.EcodeSystemError, "unexpected event")
}
