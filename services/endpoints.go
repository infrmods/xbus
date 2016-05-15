package services

import (
	"encoding/json"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
)

func (services *Services) makeEndpoints(kvs []*mvccpb.KeyValue) ([]comm.ServiceEndpoint, error) {
	endpoints := make([]comm.ServiceEndpoint, 0, len(kvs))
	for _, kv := range kvs {
		var endpoint comm.ServiceEndpoint
		if err := json.Unmarshal(kv.Value, &endpoint); err != nil {
			glog.Errorf("unmarshal endpoint fail(%#v): %v", string(kv.Value), err)
			return nil, comm.NewError(comm.EcodeDamagedEndpointValue, "")
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}
