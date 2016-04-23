package comm

import (
	"encoding/json"
	"github.com/golang/glog"
)

type ServiceEndpoint struct {
	Type    string
	Address string
	Config  string `json:",omitempty"`
	Proto   string `json:",omitempty"`
}

func (endpoint *ServiceEndpoint) Marshal() ([]byte, error) {
	if data, err := json.Marshal(endpoint); err == nil {
		return data, nil
	} else {
		glog.Errorf("marchal endpoint(%#v) fail: %v", endpoint, err)
		return nil, NewError(EcodeSystemError, "marshal endpoint fail")
	}
}
