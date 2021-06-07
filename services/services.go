package services

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	v3rpc "github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"io"
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
	Type        string `json:"type,omitempty"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
	Md5         string `json:"-"`
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

// ServiceWithRawZone service with raw zone
type ServiceWithRawZone struct {
	Service string   `json:"service"`
	Zones   []string `json:"zones"`
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
	config      Config
	db          *sql.DB
	etcdClient  *clientv3.Client
	ProtoSwitch bool
}

// NewServiceCtrl new service ctrl
func NewServiceCtrl(config *Config, db *sql.DB, etcdClient *clientv3.Client) (*ServiceCtrl, error) {
	if err := config.prepare(); err != nil {
		return nil, err
	}
	glog.Infof("%#v", *config)
	services := &ServiceCtrl{config: *config, db: db, etcdClient: etcdClient, ProtoSwitch: false}
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
	descs []ServiceDescV1, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {

	if !ctrl.ProtoSwitch {
		return ctrl.PlugAllBack(ctx, ttl, leaseID, descs, endpoint)
	}

	if err := ctrl.checkAddress(endpoint.Address); err != nil {
		return 0, err
	}
	for _, desc := range descs {
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

	updateOps := make([]clientv3.Op, 0, len(descs)*2)

	for i := range descs {
		desc := &descs[i]
		//descData, err := desc.Marshal()
		if err != nil {
			return 0, err
		}
		//descValue := string(descData)
		protoStr := desc.Proto
		w := md5.New()
		io.WriteString(w, protoStr)
		desc.Md5 = fmt.Sprintf("%x", w.Sum(nil))
		descKey := ctrl.serviceDescKey(desc.Service, desc.Zone)
		protoMd5Key := ctrl.serviceM5NotifyKey(desc.Service, desc.Zone)
		updateOps = append(updateOps,
			clientv3.OpTxn(
				[]clientv3.Cmp{
					clientv3.Compare(clientv3.Value(descKey), "!=", ""),
					clientv3.Compare(clientv3.Value(protoMd5Key), "=", desc.Md5),
				},
				nil,
				[]clientv3.Op{
					clientv3.OpPut(protoMd5Key, desc.Md5),
					clientv3.OpPut(descKey, "{}"),
					//clientv3.OpPut(ctrl.serviceDescNotifyKey(desc.Service, desc.Zone), descValue),
				},
			))

		nodeKey := ctrl.serviceNodeKey(desc.Service, desc.Zone, endpoint.Address)
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
	if err := ctrl.updateServiceDBItems(descs); err != nil {
		glog.Errorf("update service db items fail: %v", err)
		return 0, utils.NewError(utils.EcodeSystemError, "update db fail")
	}
	if _, err := ctrl.etcdClient.Txn(ctx).Then(updateOps...).Commit(); err != nil {
		return 0, utils.CleanErr(err, "plug service fail", "put services node fail: %v", err)
	}
	if err := ctrl.updateServiceDBItemsCommit(descs); err != nil {
		glog.Errorf("update service db items fail: %v", err)
		return 0, utils.NewError(utils.EcodeSystemError, "update db fail")
	}
	return leaseID, nil
}

// PlugAll plug services
func (ctrl *ServiceCtrl) PlugAllBack(ctx context.Context,
	ttl time.Duration, leaseID clientv3.LeaseID,
	descs []ServiceDescV1, endpoint *ServiceEndpoint) (clientv3.LeaseID, error) {
	if err := ctrl.checkAddress(endpoint.Address); err != nil {
		return 0, err
	}
	for _, desc := range descs {
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

	updateOps := make([]clientv3.Op, 0, len(descs)*2)
	for _, desc := range descs {
		descData, err := desc.Marshal()
		if err != nil {
			return 0, err
		}
		descValue := string(descData)
		descKey := ctrl.serviceDescKey(desc.Service, desc.Zone)
		updateOps = append(updateOps,
			clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(descKey), "=", descValue)},
				nil,
				[]clientv3.Op{
					clientv3.OpPut(descKey, descValue),
					clientv3.OpPut(ctrl.serviceDescNotifyKey(desc.Service, desc.Zone), descValue),
				},
			))

		nodeKey := ctrl.serviceNodeKey(desc.Service, desc.Zone, endpoint.Address)
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

	if err := ctrl.updateServiceDBItemsBack(descs); err != nil {
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
	if _, err := ctrl.etcdClient.Delete(ctx, ctrl.serviceNodeKey(service, zone, addr)); err != nil {
		glog.Errorf("delete key(%s) fail: %v", ctrl.serviceNodeKey(service, zone, addr), err)
		return utils.NewSystemError("delete key fail")
	}
	return nil
}

// Query query service
func (ctrl *ServiceCtrl) Query(ctx context.Context, clientIP net.IP, service string) (*ServiceV1, int64, error) {
	if err := checkService(service); err != nil {
		return nil, 0, err
	}
	if ctrl.ProtoSwitch {
		return ctrl._query(ctx, clientIP, service)
	}
	return ctrl._queryBack(ctx, clientIP, service)
}

// QueryZones query services with raw zone
func (ctrl *ServiceCtrl) QueryZones(ctx context.Context, clientIP net.IP, service string) (*ServiceWithRawZone, int64, error) {
	if err := checkService(service); err != nil {
		return nil, 0, err
	}

	serviceKey := ctrl.serviceEntryPrefix(service)
	resp, err := ctrl.etcdClient.Get(ctx, serviceKey, clientv3.WithPrefix(), clientv3.WithKeysOnly())

	if err != nil {
		return nil, 0, utils.CleanErr(err, "query fail", "Query_QueryZones(%s) fail: %v", service, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, 0, utils.Errorf(utils.EcodeNotFound, "no such service: %s", service)
	}

	zones, err := ctrl.makeServiceWithRawZone(serviceKey, resp.Kvs)

	return &ServiceWithRawZone{
		Service: service,
		Zones:   zones,
	}, resp.Header.Revision, nil
}

// QueryServiceZone query service zone with service key and zone
func (ctrl *ServiceCtrl) QueryServiceZone(ctx context.Context, clientIP net.IP, service string, zone string) (*ServiceV1, int64, error) {
	key := ctrl.serviceZoneKey(service, zone)
	return ctrl._query(ctx, clientIP, key) // key 为 `service/zone`
}

func (ctrl *ServiceCtrl) _queryBack(ctx context.Context, clientIP net.IP, serviceKey string) (*ServiceV1, int64, error) {
	key := ctrl.serviceEntryPrefix(serviceKey)
	resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, utils.CleanErr(err, "query fail", "Query_queryBack(%s) fail: %v", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, 0, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey)
	}
	service, err := ctrl.makeServiceBack(clientIP, serviceKey, resp.Kvs)
	if err != nil {
		return nil, 0, err
	}
	return service, resp.Header.Revision, nil
}

func (ctrl *ServiceCtrl) _query(ctx context.Context, clientIP net.IP, serviceKey string) (*ServiceV1, int64, error) {
	key := ctrl.serviceEntryPrefix(serviceKey)
	if !ctrl.ProtoSwitch {
		return ctrl._queryBack(ctx, clientIP, serviceKey)
	}
	resp, err := ctrl.etcdClient.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, 0, utils.CleanErr(err, "query fail", "Query_query(%s) fail: %v", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, 0, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey)
	}
	service, err := ctrl.makeService(ctx, clientIP, serviceKey, resp.Kvs)
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

// ServiceDescEvent desc event
type ServiceDescEvent struct {
	EventType string        `json:"event_type"`
	Service   ServiceDescV1 `json:"service"`
}

// ServiceDescWatchResult desc watch result
type ServiceDescWatchResult struct {
	Events   []ServiceDescEvent `json:"events"`
	Revision int64              `json:"revision"`
}

// WatchServiceDesc watch service desc
func (ctrl *ServiceCtrl) WatchServiceDesc(ctx context.Context, zone string, revision int64) (*ServiceDescWatchResult, error) {
	prefix := ctrl.serviceM5NotifyPrefix(zone)
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, prefix, clientv3.WithRev(revision), clientv3.WithPrefix())
	} else {
		watchCh = watcher.Watch(ctx, prefix, clientv3.WithPrefix())
	}

	for {
		resp, ok := <-watchCh
		if !ok {
			return nil, nil
		}
		if err := resp.Err(); err != nil {
			// if revision is compacted, return latest revision
			if err == v3rpc.ErrCompacted {
				glog.Warningf("services-md5s key with revision [%d] is compacted, call get instead", revision)
				watchCh = watcher.Watch(ctx, prefix, clientv3.WithPrefix())
				continue
			}
			return nil, utils.CleanErr(resp.Err(), "watch service desc fail", "watch service desc(zone:%s) fail: %v", zone, resp.Err())
		}

		events := make([]ServiceDescEvent, 0, 8)
		for _, event := range resp.Events {
			var eventType string
			var serviceDesc = ServiceDescV1{}
			var md5 string

			if event.Type == clientv3.EventTypePut {
				eventType = "put"
				md5 = string(event.Kv.Value)
				matches := rServiceSplit.FindAllStringSubmatch(string(event.Kv.Key), -1)
				if len(matches) != 1 {
					glog.Warningf("got unexpected service node: %s", string(event.Kv.Key))
					continue
				}
				zoneTmp, service := matches[0][2], matches[0][3]
				sDTmp, err := ctrl.SearchByServiceZone(service, zoneTmp)
				if err != nil {
					continue
				}
				if sDTmp == nil {
					glog.Errorf("find by serviceZone not found %s,%s,%s", service, zoneTmp, md5)
					continue
				}
				serviceDesc.Description = sDTmp.Description
				serviceDesc.Md5 = sDTmp.Md5
				serviceDesc.Proto = sDTmp.Proto
				serviceDesc.Type = sDTmp.Type
				serviceDesc.Service = sDTmp.Service
				serviceDesc.Zone = sDTmp.Zone
			} else if event.Type == clientv3.EventTypeDelete {
				eventType = "delete"
				key := ctrl.splitServiceM5NotifyKey(string(event.Kv.Key))
				if key == nil {
					glog.Warningf("invalid service-desc key: %s", string(event.Kv.Key))
					continue
				}
				serviceDesc.Service = key.service
				serviceDesc.Zone = key.zone
			} else {
				continue
			}

			events = append(events, ServiceDescEvent{EventType: eventType, Service: serviceDesc})
		}
		if len(events) > 0 {
			return &ServiceDescWatchResult{Events: events, Revision: resp.Header.Revision}, nil
		}
	}
}

// WatchServiceDesc watch service desc
func (ctrl *ServiceCtrl) WatchServiceDescBack(ctx context.Context, zone string, revision int64) (*ServiceDescWatchResult, error) {
	prefix := ctrl.serviceDescNotifyKeyPrefix(zone)
	watcher := clientv3.NewWatcher(ctrl.etcdClient)
	defer watcher.Close()

	var watchCh clientv3.WatchChan
	if revision > 0 {
		watchCh = watcher.Watch(ctx, prefix, clientv3.WithRev(revision), clientv3.WithPrefix())
	} else {
		watchCh = watcher.Watch(ctx, prefix, clientv3.WithPrefix())
	}

	for {
		resp, ok := <-watchCh
		if !ok {
			return nil, nil
		}
		if resp.Err() != nil {
			return nil, utils.CleanErr(resp.Err(), "watch service desc fail", "watch service desc(zone:%s) fail: %v", zone, resp.Err())
		}

		events := make([]ServiceDescEvent, 0, 8)
		for _, event := range resp.Events {
			var eventType string
			var serviceDesc ServiceDescV1

			if event.Type == clientv3.EventTypePut {
				eventType = "put"
				if err := json.Unmarshal(event.Kv.Value, &serviceDesc); err != nil {
					glog.Errorf("unmarshal service desc(key: %s) fail: %v", string(event.Kv.Key), err)
					continue
				}
			} else if event.Type == clientv3.EventTypeDelete {
				eventType = "delete"
				key := ctrl.splitServiceDescNotifyKey(string(event.Kv.Key))
				if key == nil {
					glog.Warningf("invalid service-desc key: %s", string(event.Kv.Key))
					continue
				}
				serviceDesc.Service = key.service
				serviceDesc.Zone = key.zone
			} else {
				continue
			}

			events = append(events, ServiceDescEvent{EventType: eventType, Service: serviceDesc})
		}
		if len(events) > 0 {
			return &ServiceDescWatchResult{Events: events, Revision: resp.Header.Revision}, nil
		}
	}
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
			_, err := ctrl.etcdClient.Txn(ctx).Then([]clientv3.Op{
				clientv3.OpDelete(ctrl.serviceDescKey(serviceKey, zone)),
				clientv3.OpDelete(ctrl.serviceDescNotifyKey(serviceKey, zone)),
				clientv3.OpDelete(ctrl.serviceM5NotifyKey(serviceKey, zone)),
			}...).Commit()
			if err != nil {
				return utils.CleanErr(err, "delete service keys fail", "delete service keys(%s) fail: %v", entryPrefix, err)
			}
		}
	} else {
		return utils.CleanErr(err, "get service keys fail", "precheck delete(%s) fail: %v", entryPrefix, err)
	}
	return ctrl.deleteServiceDBItems(serviceKey, zone)
}
