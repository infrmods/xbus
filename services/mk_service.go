package services

import (
	"encoding/json"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"strings"
)

func (ctrl *ServiceCtrl) makeService(kvs []*mvccpb.KeyValue) (*Service, error) {
	var service Service
	service.Endpoints = make([]ServiceEndpoint, 0, len(kvs))
	for _, kv := range kvs {
		key := string(kv.Key)
		if strings.HasPrefix(key, serviceKeyNodePrefix) {
			var endpoint ServiceEndpoint
			if err := json.Unmarshal(kv.Value, &endpoint); err != nil {
				glog.Errorf("unmarshal endpoint fail(%#v): %v", string(kv.Value), err)
				return nil, comm.NewError(comm.EcodeDamagedEndpointValue, "")
			}
			service.Endpoints = append(service.Endpoints, endpoint)
		} else if strings.HasPrefix(key, serviceDescNodeKey) {
			if err := json.Unmarshal(kv.Value, &service.Desc); err != nil {
				glog.Errorf("invalid desc(%s), unmarshal fail: %v", key, string(kv.Value))
				return nil, comm.NewError(comm.EcodeSystemError, "service-data damanged")
			}
		}
	}
	return &service, nil
}
