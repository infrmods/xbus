package services

import (
	"encoding/json"
	"net"
	"regexp"
	"strings"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
)

var rServiceSplit = regexp.MustCompile(`/(.+)/(.+)/(.+)$`)

func (ctrl *ServiceCtrl) makeService(clientIP net.IP, serviceKey string, kvs []*mvccpb.KeyValue) (*ServiceV1, error) {
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
