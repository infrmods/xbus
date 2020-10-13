package configs

import (
	"context"
	"database/sql"
	"strings"

	"github.com/coreos/etcd/clientv3"
	v3rpc "github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

// ConfigItem config item
type ConfigItem struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

// Config module config
type Config struct {
	KeyPrefix string `default:"/configs" yaml:"key_prefix"`
}

// ConfigCtrl config ctrl
type ConfigCtrl struct {
	config     Config
	db         *sql.DB
	etcdClient *clientv3.Client
}

// NewConfigCtrl new config ctrl
func NewConfigCtrl(config *Config, db *sql.DB, etcdClient *clientv3.Client) *ConfigCtrl {
	configs := &ConfigCtrl{config: *config, db: db, etcdClient: etcdClient}
	if strings.HasSuffix(configs.config.KeyPrefix, "/") {
		configs.config.KeyPrefix = configs.config.KeyPrefix[:len(configs.config.KeyPrefix)-1]
	}
	return configs
}

const rangeLimit = 20

// Range range
func (ctrl *ConfigCtrl) Range(ctx context.Context, from, end string, sortOption *clientv3.SortOption) ([]ConfigItem, bool, error) {
	if err := checkNamePrefix(from); err != nil {
		return nil, false, err
	}
	if err := checkNamePrefix(end); err != nil {
		return nil, false, err
	}
	fromKey := ctrl.configKey(from)
	endKey := ctrl.configKey(end)
	if end == "" {
		endKey = ctrl.endKey()
	}

	resp, err := ctrl.etcdClient.Get(
		ctx, fromKey, clientv3.WithRange(endKey),
		clientv3.WithSort(sortOption.Target, sortOption.Order),
		clientv3.WithLimit(rangeLimit))
	if err != nil {
		return nil, false, utils.CleanErr(err, "", "get range(%s, %s) fail: %v", from, end, err)
	}
	cfgs := make([]ConfigItem, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.HasPrefix(key, ctrl.config.KeyPrefix) {
			name := key[len(ctrl.config.KeyPrefix)+1:]
			cfgs = append(cfgs, configFromKv(name, kv))
		} else {
			glog.Errorf("invalid key from range(%s, %s): %s", from, end, key)
		}
	}
	return cfgs, resp.More, nil
}

// ListDBConfigs list db configs
func (ctrl *ConfigCtrl) ListDBConfigs(ctx context.Context,
	tag, prefix string, skip, limit int) (int64, []ConfigInfo, error) {
	count, err := GetDBConfigCount(ctrl.db, tag, prefix)
	if err != nil {
		glog.Errorf("get db configs(prefix: %s) fail: %v", prefix, err)
		return 0, nil, utils.NewSystemError("get configs count fail")
	}
	items, err := ListDBConfigs(ctrl.db, tag, prefix, skip, limit)
	if err != nil {
		glog.Errorf("get db configs fail: %v", err)
		return 0, nil, utils.NewSystemError("get configs fail")
	}
	if items == nil {
		items = make([]ConfigInfo, 0)
	}
	return count, items, nil
}

// Get get config
func (ctrl *ConfigCtrl) Get(ctx context.Context, appID int64, node, name string) (*ConfigItem, int64, error) {
	if err := checkName(name); err != nil {
		return nil, 0, err
	}

	resp, err := ctrl.etcdClient.Get(ctx, ctrl.configKey(name))
	if err != nil {
		return nil, 0, utils.CleanErr(err, "", "get config key(%s) fail: %v", name, err)
	}
	if resp.Kvs == nil {
		return nil, 0, utils.NewError(utils.EcodeNotFound, name)
	}
	cfg := configFromKv(name, resp.Kvs[0])
	if err := ctrl.changeAppConfigState(appID, node, name, cfg.Version); err != nil {
		return nil, 0, err
	}
	return &cfg, resp.Header.Revision, nil
}

// Delete delete config
func (ctrl *ConfigCtrl) Delete(ctx context.Context, name string) error {
	if err := ctrl.deleteDBConfig(name); err != nil {
		return err
	}
	if _, err := ctrl.etcdClient.Delete(ctx, ctrl.configKey(name)); err != nil {
		return utils.CleanErr(err, "", "delete config(%s) fail: %v", name, err)
	}
	return nil
}

func configFromKv(name string, kv *mvccpb.KeyValue) ConfigItem {
	return ConfigItem{Name: name,
		Value:   string(kv.Value),
		Version: kv.Version}
}

// Put put config
func (ctrl *ConfigCtrl) Put(ctx context.Context, tag, name string, appID int64, remark, value string, version int64) (int64, error) {
	if err := checkName(name); err != nil {
		return 0, err
	}
	key := ctrl.configKey(name)
	if version < 0 {
		resp, err := ctrl.etcdClient.Put(ctx, key, value)
		if err != nil {
			return 0, utils.CleanErr(err, "", "put config key(%s) fail: %v", name, err)
		}
		if err := ctrl.setDBConfig(tag, name, appID, remark, value); err != nil {
			return 0, err
		}
		return resp.Header.Revision, nil
	}

	cmp := clientv3.Compare(clientv3.Version(key), "=", version)
	opPut := clientv3.OpPut(key, value)
	if resp, err := ctrl.etcdClient.Txn(ctx).If(cmp).Then(opPut).Commit(); err != nil {
		return 0, utils.CleanErr(err, "", "put config key(%s) with version(%d) fail: %v", name, version, err)
	} else if !resp.Succeeded {
		return 0, utils.NewError(utils.EcodeInvalidVersion, "")
	} else {
		if err := ctrl.setDBConfig(tag, name, appID, remark, value); err != nil {
			return 0, err
		}
		return resp.Header.Revision, nil
	}
}

// Watch watch config
func (ctrl *ConfigCtrl) Watch(ctx context.Context, appID int64, node, name string, revision int64) (*ConfigItem, int64, error) {
	if err := checkName(name); err != nil {
		return nil, 0, err
	}
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	key := ctrl.configKey(name)
	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision))
	} else {
		watchCh = watcher.Watch(ctx, key)
	}
	resp := <-watchCh
	if err := resp.Err(); err != nil {
		// if revision is compacted, return latest revision
		if err == v3rpc.ErrCompacted {
			glog.Warningf("key [%s] with revision [%d] is compacted, call get instead", key, revision)
			return ctrl.Get(ctx, appID, node, name)
		}
		return nil, 0, utils.CleanErr(err, "", "watch key(%s) with revision(%d) fail: %v", name, revision, err)
	}
	if resp.Canceled || resp.Events == nil {
		return nil, resp.Header.Revision, nil
	}
	for _, event := range resp.Events {
		switch event.Type {
		case mvccpb.PUT:
			cfg := configFromKv(name, event.Kv)
			if err := ctrl.changeAppConfigState(appID, node, name, cfg.Version); err != nil {
				return nil, 0, err
			}
			return &cfg, resp.Header.Revision, nil
		case mvccpb.DELETE:
			return nil, 0, utils.NewError(utils.EcodeDeleted, "")
		}
	}
	return nil, 0, utils.NewSystemError("unexpected event")
}
