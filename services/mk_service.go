package services

import (
	"context"
	"encoding/json"
	"net"
	"regexp"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

var rServiceSplit = regexp.MustCompile(`/(.+)/(.+)/(.+)$`)

var lServiceSplit = regexp.MustCompile(`(.+)/(.+)$`)

func (ctrl *ServiceCtrl) makeServiceWithRawZone(serviceKey string, kvs []*mvccpb.KeyValue) ([]string, error) {
	zonesMap := make(map[string]bool)
	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			glog.Warningf("got unexpected service node: %s", string(kv.Key))
			continue
		}

		zone := matches[0][2]
		zonesMap[zone] = true
	}

	zones := []string{}
	for k := range zonesMap {
		zones = append(zones, k)
	}

	return zones, nil
}

func (ctrl *ServiceCtrl) makeService(ctx context.Context, clientIP net.IP, serviceKey string, kvs []*mvccpb.KeyValue) (*ServiceV1, error) {
	zones := make(map[string]*ServiceZoneV1)

	getOps := make([]clientv3.Op, 0)
	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			glog.Warningf("got unexpected service node: %s", string(kv.Key))
			continue
		}
		zone, suffix := matches[0][2], matches[0][3]
		if strings.HasPrefix(suffix, serviceKeyNodePrefix) {
			serviceZone := zones[zone]
			if serviceZone == nil {
				serviceZone = &ServiceZoneV1{Endpoints: make([]ServiceEndpoint, 0)}
				zones[zone] = serviceZone
			}
			getOps = append(getOps, clientv3.OpGet(string(kv.Key)))
		}
	}

	if len(getOps) > 0 {
		resp, err := ctrl.etcdClient.Txn(context.TODO()).If().Then(getOps...).Commit()
		if err != nil {
			return nil, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", serviceKey, err)
		}
		for _, rp := range resp.Responses {
			for _, ev := range rp.GetResponseRange().Kvs {
				if ev.Value == nil || len(ev.Value) == 0 {
					continue
				}
				var endpoint ServiceEndpoint
				if err := json.Unmarshal(ev.Value, &endpoint); err != nil {
					glog.Errorf("unmarshal endpoint fail(%#v): %v", string(ev.Value), err)
					return nil, utils.NewError(utils.EcodeDamagedEndpointValue, "")
				}
				endpoint.Address = ctrl.config.mapAddress(endpoint.Address, clientIP)
				matches := rServiceSplit.FindAllStringSubmatch(string(ev.Key), -1)
				zone := matches[0][2]
				serviceZone := zones[zone]
				serviceZone.Endpoints = append(serviceZone.Endpoints, endpoint)
			}
		}
	}

	getOps = make([]clientv3.Op, 0)
	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			continue
		}
		suffix := matches[0][3]
		if suffix != serviceDescNodeKey {
			continue
		}
		zone := matches[0][2]
		serviceZone := zones[zone]
		if serviceZone == nil || len(serviceZone.Endpoints) == 0 {
			continue
		}
		getOps = append(getOps,
			clientv3.OpGet(ctrl.serviceM5NotifyKey(strings.Split(matches[0][1], "/")[1], zone)))
	}
	if len(getOps) > 0 {
		resp, err := ctrl.etcdClient.Txn(context.TODO()).If().Then(getOps...).Commit()
		if err != nil {
			return nil, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", serviceKey, err)
		}
		for _, rp := range resp.Responses {
			for _, ev := range rp.GetResponseRange().Kvs {
				matches := rServiceSplit.FindAllStringSubmatch(string(ev.Key), -1)
				if len(matches) != 1 {
					continue
				}
				zone, service := matches[0][2], matches[0][3]
				//TODO batch use SearchOnlyBymd5s
				serviceDesc, err := ctrl.SearchBymd5(service, string(ev.Value))
				if err != nil {
					return nil, err
				}
				if serviceDesc == nil {
					glog.Errorf("find by md5 not found %s,%s", service, string(ev.Value))
					continue
				}
				serviceZone := zones[zone]
				serviceZone.Description = serviceDesc.Description
				serviceZone.Md5 = serviceDesc.Md5
				serviceZone.Proto = serviceDesc.Proto
				serviceZone.Type = serviceDesc.Type
				serviceZone.Service = serviceKey
				serviceZone.Zone = zone
			}
		}
	}
	return &ServiceV1{Service: serviceKey, Zones: zones}, nil
}

//change serviceKey awaly single ?
func (ctrl *ServiceCtrl) makeServiceBatch(ctx context.Context, clientIP net.IP, serviceKey string, kvs []*mvccpb.KeyValue) (*ServiceV1, error) {
	zones := make(map[string]*ServiceZoneV1)

	getOps := make([]clientv3.Op, 0)
	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			glog.Warningf("got unexpected service node: %s", string(kv.Key))
			continue
		}
		zone, suffix := matches[0][2], matches[0][3]
		if strings.HasPrefix(suffix, serviceKeyNodePrefix) {
			serviceZone := zones[zone]
			if serviceZone == nil {
				serviceZone = &ServiceZoneV1{Endpoints: make([]ServiceEndpoint, 0)}
				zones[zone] = serviceZone
			}
			getOps = append(getOps, clientv3.OpGet(string(kv.Key)))
		}
	}

	if len(getOps) > 0 {
		resp, err := ctrl.etcdClient.Txn(context.TODO()).If().Then(getOps...).Commit()
		if err != nil {
			return nil, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", serviceKey, err)
		}
		for _, rp := range resp.Responses {
			for _, ev := range rp.GetResponseRange().Kvs {
				if ev.Value == nil || len(ev.Value) == 0 {
					continue
				}
				var endpoint ServiceEndpoint
				if err := json.Unmarshal(ev.Value, &endpoint); err != nil {
					glog.Errorf("unmarshal endpoint fail(%#v): %v", string(ev.Value), err)
					return nil, utils.NewError(utils.EcodeDamagedEndpointValue, "")
				}
				endpoint.Address = ctrl.config.mapAddress(endpoint.Address, clientIP)
				matches := rServiceSplit.FindAllStringSubmatch(string(ev.Key), -1)
				zone := matches[0][2]
				serviceZone := zones[zone]
				serviceZone.Endpoints = append(serviceZone.Endpoints, endpoint)
			}
		}
	}

	getOps = make([]clientv3.Op, 0)
	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			continue
		}
		suffix := matches[0][3]
		if suffix != serviceDescNodeKey {
			continue
		}
		zone := matches[0][2]
		serviceZone := zones[zone]
		if serviceZone == nil || len(serviceZone.Endpoints) == 0 {
			continue
		}
		getOps = append(getOps,
			clientv3.OpGet(ctrl.serviceM5NotifyKey(strings.Split(matches[0][1], "/")[1], zone)))
	}

	if len(getOps) == 0 {
		return &ServiceV1{Service: serviceKey, Zones: zones}, nil
	}
	resp, err := ctrl.etcdClient.Txn(context.TODO()).If().Then(getOps...).Commit()
	if err != nil {
		return nil, utils.CleanErr(err, "query fail", "Query(%s) fail: %v", serviceKey, err)
	}
	for _, rp := range resp.Responses {
		for _, ev := range rp.GetResponseRange().Kvs {
			matches := rServiceSplit.FindAllStringSubmatch(string(ev.Key), -1)
			if len(matches) != 1 {
				continue
			}
			zone, service := matches[0][2], matches[0][3]
			//TODO batch use SearchOnlyBymd5s
			serviceDesc, err := ctrl.SearchBymd5(service, string(ev.Value))
			if err != nil {
				return nil, err
			}
			if serviceDesc == nil {
				glog.Errorf("find by md5 not found %s,%s", service, string(ev.Value))
				continue
			}
			serviceZone := zones[zone]
			serviceZone.Description = serviceDesc.Description
			serviceZone.Md5 = serviceDesc.Md5
			serviceZone.Proto = serviceDesc.Proto
			serviceZone.Type = serviceDesc.Type
			serviceZone.Service = serviceKey
			serviceZone.Zone = zone
		}
	}
	return &ServiceV1{Service: serviceKey, Zones: zones}, nil
}

func (ctrl *ServiceCtrl) makeServiceBack(clientIP net.IP, serviceKey string, kvs []*mvccpb.KeyValue) (*ServiceV1, error) {
	zones := make(map[string]*ServiceZoneV1)

	for _, kv := range kvs {
		matches := rServiceSplit.FindAllStringSubmatch(string(kv.Key), -1)
		if len(matches) != 1 {
			glog.Warningf("got unexpected service node: %s", string(kv.Key))
			continue
		}
		_, zone, suffix := matches[0][1], matches[0][2], matches[0][3]
		serviceZone := zones[zone]
		if serviceZone == nil {
			serviceZone = &ServiceZoneV1{Endpoints: make([]ServiceEndpoint, 0)}
			zones[zone] = serviceZone
		}
		if suffix == serviceDescNodeKey {
			if err := json.Unmarshal(kv.Value, &serviceZone.ServiceDescV1); err != nil {
				glog.Errorf("invalid desc(%s), unmarshal fail: %v", string(kv.Key), string(kv.Value))
				return nil, utils.NewSystemError("service-data damanged")
			}
			serviceZone.Service = serviceKey
			serviceZone.Zone = zone
		} else if strings.HasPrefix(suffix, serviceKeyNodePrefix) {
			var endpoint ServiceEndpoint
			if err := json.Unmarshal(kv.Value, &endpoint); err != nil {
				glog.Errorf("unmarshal endpoint fail(%#v): %v", string(kv.Value), err)
				return nil, utils.NewError(utils.EcodeDamagedEndpointValue, "")
			}
			endpoint.Address = ctrl.config.mapAddress(endpoint.Address, clientIP)
			serviceZone.Endpoints = append(serviceZone.Endpoints, endpoint)
		} else {
			glog.Warningf("got unexpected service node: %s", string(kv.Key))
		}
	}
	return &ServiceV1{Service: serviceKey, Zones: zones}, nil
}
