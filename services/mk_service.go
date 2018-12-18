package services

import (
	"encoding/json"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"net"
	"strings"
)

func (ctrl *ServiceCtrl) makeService(clientIp net.IP, name, version string, kvs []*mvccpb.KeyValue) (*Service, error) {
	var service Service
	service.Name = name
	service.Version = version

	service.Endpoints = make([]ServiceEndpoint, 0, len(kvs))
	for _, kv := range kvs {
		parts := strings.Split(string(kv.Key), "/")
		name := parts[len(parts)-1]
		if strings.HasPrefix(name, serviceKeyNodePrefix) {
			var endpoint ServiceEndpoint
			if err := json.Unmarshal(kv.Value, &endpoint); err != nil {
				glog.Errorf("unmarshal endpoint fail(%#v): %v", string(kv.Value), err)
				return nil, utils.NewError(utils.EcodeDamagedEndpointValue, "")
			}
			endpoint.Address = ctrl.config.mapAddress(endpoint.Address, clientIp)
			service.Endpoints = append(service.Endpoints, endpoint)
		} else if strings.HasPrefix(name, serviceDescNodeKey) {
			if err := json.Unmarshal(kv.Value, &service.ServiceDesc); err != nil {
				glog.Errorf("invalid desc(%s), unmarshal fail: %v", string(kv.Key), string(kv.Value))
				return nil, utils.NewSystemError("service-data damanged")
			}
		}
	}
	return &service, nil
}

func (ctrl *ServiceCtrl) makeAllService(clientIp net.IP, name string, kvs []*mvccpb.KeyValue) (map[string]*Service, error) {
	mkvs := make(map[string][]*mvccpb.KeyValue)
	for _, kv := range kvs {
		parts := strings.Split(string(kv.Key), "/")
		if len(parts) == 1 {
			continue
		}
		version := parts[len(parts)-2]
		mkvs[version] = append(mkvs[version], kv)
	}

	services := make(map[string]*Service)
	for version, subkvs := range mkvs {
		if service, err := ctrl.makeService(clientIp, name, version, subkvs); err == nil {
			services[version] = service
		} else {
			return nil, err
		}
	}
	return services, nil
}
