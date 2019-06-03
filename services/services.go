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

const DefaultZone = "default"

type ServiceDescV1 struct {
	Service     string `json:"service"`
	Zone        string `json:"zone,omitempty"`
	Type        string `json:"type"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
}

func (desc *ServiceDescV1) Marshal() ([]byte, error) {
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

type ServiceZoneV1 struct {
	Endpoints []ServiceEndpoint `json:"endpoints"`

	ServiceDescV1
}

type ServiceV1 struct {
	Zones map[string]*ServiceZoneV1 `json:"zones"`
}

type NetMapping struct {
	SrcNet string `yaml:"src_net"`
	DestIp string `yaml:"dest_ip"`
	srcNet *net.IPNet
}

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
	for i, _ := range config.NetMappings {
		mapping := &config.NetMappings[i]
		if _, srcNet, err := net.ParseCIDR(mapping.SrcNet); err == nil {
			mapping.srcNet = srcNet
		} else {
			return fmt.Errorf("invalid SrcNet: %s", mapping.SrcNet)
		}
		if ip := net.ParseIP(mapping.DestIp); ip == nil {
			return fmt.Errorf("invalid DestIp: %s", mapping.DestIp)
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

func (config *Config) mapAddress(addr string, clientIp net.IP) string {
	if clientIp != nil {
		if host, port, err := net.SplitHostPort(addr); err == nil {
			if ip := net.ParseIP(host); ip != nil {
				for _, mapping := range config.NetMappings {
					if mapping.srcNet.Contains(ip) && !mapping.srcNet.Contains(clientIp) {
						return net.JoinHostPort(mapping.DestIp, port)
					}
				}
			}
		}
	}
	return addr
}

type ServiceCtrl struct {
	config     Config
	db         *sql.DB
	etcdClient *clientv3.Client
}

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

func (ctrl *ServiceCtrl) Plug(ctx context.Context,
	ttl time.Duration, leaseId clientv3.LeaseID,
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
	if err := ctrl.updateServices([]ServiceDescV1{*desc}); err != nil {
		return 0, err
	}
	if err := ctrl.ensureServiceDesc(ctx, desc.Service, desc.Zone, string(descData)); err != nil {
		return 0, err
	}
	return ctrl.setServiceNode(ctx, ttl, leaseId,
		ctrl.serviceKey(desc.Service, desc.Zone, endpoint.Address), string(endpointData))
}

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

func (ctrl *ServiceCtrl) PlugAllService(ctx context.Context,
	ttl time.Duration, leaseId clientv3.LeaseID,
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

	if err := ctrl.updateServices(desces); err != nil {
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
		if leaseId, err = ctrl.setServiceNode(ctx, ttl, leaseId,
			ctrl.serviceKey(desc.Service, desc.Zone, endpoint.Address),
			string(endpointData)); err != nil {
			return 0, err
		}
	}
	return leaseId, nil
}

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

func (ctrl *ServiceCtrl) Query(ctx context.Context, clientIp net.IP, service string) (*ServiceV1, int64, error) {
	if err := checkService(service); err != nil {
		return nil, 0, err
	}
	return ctrl._query(ctx, clientIp, service)
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
