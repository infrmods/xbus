package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

// DefaultZone default service zone
const DefaultZone = "default"

// ServiceDescV1 service descriptor
type ServiceDescV1 struct {
	Service     string `json:"service"`
	Zone        string `json:"zone,omitempty"`
	Type        string `json:"type"`
	Extension   string `json:"extension,omitempty"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
}

// Marshal marshal impl
func (desc *ServiceDescV1) Marshal() ([]byte, error) {
	data, err := json.Marshal(desc)
	if err != nil {
		glog.Errorf("marshal service-desc(%#v) fail: %v", desc, err)
		return nil, utils.NewSystemError("marshal service-desc fail")
	}
	return data, nil
}

// ServiceEndpoint service endpoint
type ServiceEndpoint struct {
	Address string `json:"address"`
	Config  string `json:"config,omitempty"`
}

// Marshal marshal impl
func (endpoint *ServiceEndpoint) Marshal() ([]byte, error) {
	data, err := json.Marshal(endpoint)
	if err != nil {
		glog.Errorf("marshal endpoint(%#v) fail: %v", endpoint, err)
		return nil, utils.NewSystemError("marshal endpoint fail")
	}
	return data, nil
}

// ServiceZoneV1 service zone
type ServiceZoneV1 struct {
	Endpoints []ServiceEndpoint `json:"endpoints"`

	ServiceDescV1
}

// ServiceV1 service
type ServiceV1 struct {
	Service string                    `json:"service"`
	Zones   map[string]*ServiceZoneV1 `json:"zones"`
}

// NetMapping net mapping
type NetMapping struct {
	SrcNet string `yaml:"src_net"`
	DestIP string `yaml:"dest_ip"`
	srcNet *net.IPNet
}

// Config service module config
type Config struct {
	KeyPrefix               string       `default:"/services" yaml:"key_prefix"`
	NetMappings             []NetMapping `yaml:"net_mappings"`
	ExtNotifyTTL            int64        `default:"86400" yaml:"extension_nofiy_tty"`
	BannedEndpointAddresses []string     `yaml:"banned_endpoint_addresses"`
	bannedAddrRs            []*regexp.Regexp
}

func (config *Config) prepare() error {
	config.bannedAddrRs = make([]*regexp.Regexp, 0, len(config.BannedEndpointAddresses))
	for _, addr := range config.BannedEndpointAddresses {
		if r, err := regexp.Compile(addr); err == nil {
			config.bannedAddrRs = append(config.bannedAddrRs, r)
		} else {
			return fmt.Errorf("invalid banned address: %s", addr)
		}
	}
	for i := range config.NetMappings {
		mapping := &config.NetMappings[i]
		if _, srcNet, err := net.ParseCIDR(mapping.SrcNet); err == nil {
			mapping.srcNet = srcNet
		} else {
			return fmt.Errorf("invalid SrcNet: %s", mapping.SrcNet)
		}
		if ip := net.ParseIP(mapping.DestIP); ip == nil {
			return fmt.Errorf("invalid DestIp: %s", mapping.DestIP)
		}
	}
	return nil
}

func (config *Config) isAddressBanned(addr string) bool {
	for _, r := range config.bannedAddrRs {
		if r.MatchString(addr) {
			return true
		}
	}
	return false
}

func (config *Config) mapAddress(addr string, clientIP net.IP) string {
	if clientIP != nil {
		if host, port, err := net.SplitHostPort(addr); err == nil {
			if ip := net.ParseIP(host); ip != nil {
				for _, mapping := range config.NetMappings {
					if mapping.srcNet.Contains(ip) && !mapping.srcNet.Contains(clientIP) {
						return net.JoinHostPort(mapping.DestIP, port)
					}
				}
			}
		}
	}
	return addr
}

// ServiceCtrl service module controller
type ServiceCtrl struct {
	config     Config
	db         *sql.DB
	etcdClient *clientv3.Client
}

// NewServiceCtrl new service ctrl
func NewServiceCtrl(config *Config, db *sql.DB, etcdClient *clientv3.Client) (*ServiceCtrl, error) {
	if err := config.prepare(); err != nil {
		return nil, err
	}
	glog.Infof("%#v", *config)
	services := &ServiceCtrl{config: *config, db: db, etcdClient: etcdClient}
	if strings.HasSuffix(services.config.KeyPrefix, "/") {
		services.config.KeyPrefix = services.config.KeyPrefix[:len(services.config.KeyPrefix)-1]
	}
	return services, nil
}

func checkDesc(desc *ServiceDescV1) error {
	if err := checkServiceZone(desc.Service, desc.Zone); err != nil {
		return err
	}
	if desc.Type == "" {
		return utils.Errorf(utils.EcodeInvalidEndpoint, "%s:%s missing type", desc.Service, desc.Zone)
	}
	return nil
}

// PlugAll plug services
func (ctrl *ServiceCtrl) PlugAll(ctx context.Context,
	ttl time.Duration, leaseID clientv3.LeaseID,
	desces []ServiceDescV1, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if err := ctrl.checkAddress(endpoint.Address); err != nil {
		return 0, err
	}
	for _, desc := range desces {
		if err := checkDesc(&desc); err != nil {
			return 0, err
		}
	}
	endpointData, err := endpoint.Marshal()
	if err != nil {
		return 0, err
	}
	endpointValue := string(endpointData)
	if ttl > 0 && leaseID == 0 {
		if resp, err := ctrl.etcdClient.Lease.Grant(ctx, int64(ttl.Seconds())); err == nil {
			leaseID = clientv3.LeaseID(resp.ID)
		} else {
			return 0, utils.CleanErr(err, "create lease fail", "create lease fail: %v", err)
		}
	}

	var notifyLeaseID clientv3.LeaseID
	updateOps := make([]clientv3.Op, 0, len(desces)*2)
	for _, desc := range desces {
		descData, err := desc.Marshal()
		if err != nil {
			return 0, err
		}
		descValue := string(descData)
		descKey := ctrl.serviceDescKey(desc.Service, desc.Zone)
		descPuts := []clientv3.Op{clientv3.OpPut(descKey, descValue)}
		if desc.Extension != "" {
			if notifyLeaseID == 0 {
				resp, err := ctrl.etcdClient.Grant(ctx, ctrl.config.ExtNotifyTTL)
				if err != nil {
					return 0, utils.CleanErr(err, "create notify lease fail", "create ext notify lease fail: %v", err)
				}
				notifyLeaseID = resp.ID
			}
			descPuts = append(descPuts, clientv3.OpPut(ctrl.extNotifyKey(&desc), desc.Extension, clientv3.WithLease(notifyLeaseID)))
		}
		updateOps = append(updateOps,
			clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(descKey), "=", descValue)},
				nil,
				descPuts,
			))

		nodeKey := ctrl.serviceKey(desc.Service, desc.Zone, endpoint.Address)
		var opPut clientv3.Op
		if leaseID > 0 {
			opPut = clientv3.OpPut(nodeKey, endpointValue, clientv3.WithLease(leaseID))
		} else {
			opPut = clientv3.OpPut(nodeKey, endpointValue)
		}
		updateOps = append(updateOps,
			clientv3.OpTxn(
				[]clientv3.Cmp{
					clientv3.Compare(clientv3.Value(nodeKey), "=", endpointValue),
					clientv3.Compare(clientv3.LeaseValue(nodeKey), "=", leaseID),
				},
				nil,
				[]clientv3.Op{opPut},
			))
	}
	if _, err := ctrl.etcdClient.Txn(ctx).Then(updateOps...).Commit(); err != nil {
		return 0, utils.CleanErr(err, "plug service fail",
			"put services node fail: %v", err)
	}

	if err := ctrl.updateServiceDBItems(desces); err != nil {
		glog.Errorf("update service db items fail: %v", err)
		return 0, utils.NewError(utils.EcodeSystemError, "update db fail")
	}
	return leaseID, nil
}

// Unplug unplug service
func (ctrl *ServiceCtrl) Unplug(ctx context.Context, service, zone, addr string) error {
	if err := checkServiceZone(service, zone); err != nil {
		return err
	}
	if err := ctrl.checkAddress(addr); err != nil {
		return err
	}
	if _, err := ctrl.etcdClient.Delete(ctx, ctrl.serviceKey(service, zone, addr)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", ctrl.serviceKey(service, zone, addr), err)
		return utils.NewSystemError("delete key fail")
	}
	return nil
}

// Query query service
func (ctrl *ServiceCtrl) Query(ctx context.Context, clientIP net.IP, service string) (*ServiceV1, int64, error) {
	if err := checkService(service); err != nil {
		return nil, 0, err
	}
	return ctrl._query(ctx, clientIP, service)
}

func (ctrl *ServiceCtrl) _query(ctx context.Context, clientIP net.IP, serviceKey string) (*ServiceV1, int64, error) {
	key := ctrl.serviceEntryPrefix(serviceKey)
	resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, 0, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey)
	}
	service, err := ctrl.makeService(clientIP, serviceKey, resp.Kvs)
	if err != nil {
		return nil, 0, err
	}
	return service, resp.Header.Revision, nil
}

// Watch watch service
func (ctrl *ServiceCtrl) Watch(ctx context.Context, clientIP net.IP, serviceKey string, revision int64) (*ServiceV1, int64, error) {
	if err := checkService(serviceKey); err != nil {
		return nil, 0, err
	}
	key := ctrl.serviceEntryPrefix(serviceKey)
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	} else {
		watchCh = watcher.Watch(ctx, key, clientv3.WithPrefix())
	}

	_ = <-watchCh
	return ctrl._query(ctx, clientIP, serviceKey)
}

// Delete delete service
func (ctrl *ServiceCtrl) Delete(ctx context.Context, serviceKey string, zone string) error {
	entryPrefix := ctrl.serviceEntryPrefix(serviceKey)
	if zone != "" {
		entryPrefix += zone + "/"
	}
	if resp, err := ctrl.etcdClient.Get(ctx, entryPrefix, clientv3.WithPrefix()); err == nil {
		for _, kv := range resp.Kvs {
			if !strings.HasSuffix(string(kv.Key), serviceDescNodeKey) {
				return utils.NewError("HAS_ENDPOINTS", "has endpoints plugged on")
			}
		}
		if len(resp.Kvs) > 0 {
			_, err := ctrl.etcdClient.Delete(ctx, entryPrefix, clientv3.WithPrefix())
			if err != nil {
				return utils.CleanErr(err, "delete service keys fail", "delete service keys(%s) fail: %v", entryPrefix, err)
			}
		}
	} else {
		return utils.CleanErr(err, "get service keys fail", "precheck delete(%s) fail: %v", entryPrefix, err)
	}
	return ctrl.deleteServiceDBItems(serviceKey, zone)
}

// ExtensionEvent extension event
type ExtensionEvent struct {
	Service string `json:"service"`
	Zone    string `json:"zone"`
}

// WatchExtensions watch extensions
func (ctrl *ServiceCtrl) WatchExtensions(ctx context.Context, ext string, revision int64) ([]ExtensionEvent, int64, error) {
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	key := ctrl.extNotifyPrefix(ext)
	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	} else {
		watchCh = watcher.Watch(ctx, key, clientv3.WithPrefix())
	}

	for {
		resp := <-watchCh
		if resp.Canceled {
			return nil, resp.Header.Revision, nil
		}
		events := make([]ExtensionEvent, 0, len(resp.Events))
		for _, event := range resp.Events {
			if event.IsCreate() {
				service, zone := ctrl.parseNotifyKey(string(event.Kv.Key))
				if service != nil {
					events = append(events, ExtensionEvent{
						Service: *service,
						Zone:    *zone,
					})
				}
			}
		}
		if len(events) == 0 {
			continue
		}
		return events, resp.Header.Revision, nil
	}
}
