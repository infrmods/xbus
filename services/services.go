package services

import (
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
	"golang.org/x/net/context"
)

// DefaultZone default service zone
const DefaultZone = "default"

// ServiceDescV1 service descriptor
type ServiceDescV1 struct {
	Service     string `json:"service"`
	Zone        string `json:"zone,omitempty"`
	Type        string `json:"type"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
}

// Marshal marshal impl
func (desc *ServiceDescV1) Marshal() ([]byte, error) {
	if data, err := json.Marshal(desc); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal service-desc(%#v) fail: %v", desc, err)
		return nil, utils.NewSystemError("marshal service-desc fail")
	}
}

// ServiceEndpoint service endpoint
type ServiceEndpoint struct {
	Address string `json:"address"`
	Config  string `json:"config,omitempty"`
}

// Marshal marshal impl
func (endpoint *ServiceEndpoint) Marshal() ([]byte, error) {
	if data, err := json.Marshal(endpoint); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal endpoint(%#v) fail: %v", endpoint, err)
		return nil, utils.NewSystemError("marshal endpoint fail")
	}
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
	PermitChangeDesc        bool         `default:"false" yaml:"permit_change_desc"`
	NetMappings             []NetMapping `yaml:"net_mappings"`
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

// Plug plug service
func (ctrl *ServiceCtrl) Plug(ctx context.Context,
	ttl time.Duration, leaseID clientv3.LeaseID,
	desc *ServiceDescV1, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if err := checkDesc(desc); err != nil {
		return 0, err
	}
	if endpoint.Address == "" {
		return 0, utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
	if err := ctrl.checkAddress(endpoint.Address); err != nil {
		return 0, err
	}

	descData, err := desc.Marshal()
	endpointData, err := endpoint.Marshal()
	if err != nil {
		return 0, err
	}
	if err := ctrl.updateServiceDBItems([]ServiceDescV1{*desc}); err != nil {
		return 0, err
	}
	if err := ctrl.ensureServiceDesc(ctx, desc.Service, desc.Zone, string(descData)); err != nil {
		return 0, err
	}
	return ctrl.setServiceNode(ctx, ttl, leaseID,
		ctrl.serviceKey(desc.Service, desc.Zone, endpoint.Address), string(endpointData))
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

// PlugAllService plug services
func (ctrl *ServiceCtrl) PlugAllService(ctx context.Context,
	ttl time.Duration, leaseID clientv3.LeaseID,
	desces []ServiceDescV1, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if endpoint.Address == "" {
		return 0, utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
	if err := ctrl.checkAddress(endpoint.Address); err != nil {
		return 0, err
	}
	endpointData, err := endpoint.Marshal()
	if err != nil {
		return 0, err
	}

	if err := ctrl.updateServiceDBItems(desces); err != nil {
		return 0, err
	}
	for _, desc := range desces {
		if err := checkDesc(&desc); err != nil {
			return 0, err
		}
		descData, err := desc.Marshal()
		if err != nil {
			return 0, err
		}
		if err := ctrl.ensureServiceDesc(ctx, desc.Service, desc.Zone, string(descData)); err != nil {
			return 0, err
		}
	}
	for _, desc := range desces {
		if leaseID, err = ctrl.setServiceNode(ctx, ttl, leaseID,
			ctrl.serviceKey(desc.Service, desc.Zone, endpoint.Address),
			string(endpointData)); err != nil {
			return 0, err
		}
	}
	return leaseID, nil
}

// Update update service endpoint
func (ctrl *ServiceCtrl) Update(ctx context.Context, service, zone, addr string, endpoint *ServiceEndpoint) error {
	if err := checkServiceZone(service, zone); err != nil {
		return err
	}
	if err := ctrl.checkAddress(addr); err != nil {
		return err
	}
	key := ctrl.serviceKey(service, zone, addr)
	data, err := endpoint.Marshal()
	if err != nil {
		return err
	}

	txn := ctrl.etcdClient.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), ">", 0)).Then(clientv3.OpPut(key, string(data)))
	if resp, err := txn.Commit(); err != nil {
		return utils.CleanErr(err, "update fail", "tnx update(%s) fail: %v", key, err)
	} else {
		if resp.Succeeded {
			return nil
		}
		return utils.NewError(utils.EcodeNotFound, "")
	}
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
	if resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix()); err == nil {
		if len(resp.Kvs) == 0 {
			return nil, 0, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey)
		}
		if service, err := ctrl.makeService(clientIP, serviceKey, resp.Kvs); err == nil {
			return service, resp.Header.Revision, nil
		} else {
			return nil, 0, err
		}
	} else {
		return nil, 0, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", key, err)
	}
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
			if _, err := ctrl.etcdClient.Delete(ctx, entryPrefix, clientv3.WithPrefix()); err != nil {
				return utils.CleanErr(err, "delete service keys fail", "delete service keys(%s) fail: %v", entryPrefix, err)
			}
		}
	} else {
		return utils.CleanErr(err, "get service keys fail", "precheck delete(%s) fail: %v", entryPrefix, err)
	}
	return ctrl.deleteServiceDBItems(serviceKey, zone)
}
