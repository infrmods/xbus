package comm

import (
	"encoding/json"
	"github.com/golang/glog"
)

type ServiceDesc struct {
	Type  string `json:"type"`
	Proto string `json:"proto,omitempty"`
	Desc  string `json:"desc,omitempty"`
}

func (desc *ServiceDesc) Marshal() ([]byte, error) {
	if data, err := json.Marshal(desc); err == nil {
		return data, nil
	} else {
		glog.Errorf("marshal service-desc(%#v) fail: %v", desc, err)
		return nil, NewError(EcodeSystemError, "marshal service-desc fail")
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
		return nil, NewError(EcodeSystemError, "marshal endpoint fail")
	}
}
