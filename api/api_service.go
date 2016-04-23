package api

import (
	//"github.com/infrmods/xbus/service"
	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/comm"
	"github.com/labstack/echo"
	"golang.org/x/net/context"
	"time"
)

const (
	MinServiceTTL       = 10
	DefaultServiceTTL   = 60 // in seconds
	DefaultWatchTimeout = 60 // in seconds
)

type PlugResult struct {
	ServiceId string `json:"service_id"`
	KeepId    int64  `json:"keep_id"`
}

func (server *APIServer) Pulg(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < MinServiceTTL {
		return JsonErrorf(c, comm.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	var endpoint comm.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}
	if endpoint.Type == "" || endpoint.Address == "" {
		return JsonErrorf(c, comm.EcodeInvalidEndpoint, "")
	}

	if sid, kid, err := server.xbus.Plug(context.Background(),
		c.P(0), c.P(1), time.Duration(ttl)*time.Second, &endpoint); err == nil {
		return JsonResult(c, PlugResult{ServiceId: sid, KeepId: int64(kid)})
	} else {
		return JsonError(c, err)
	}
	return nil
}

func (server *APIServer) Unplug(c echo.Context) error {
	if err := server.xbus.Unplug(context.Background(), c.P(0), c.P(1), c.P(2)); err == nil {
		return JsonOk(c)
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) Update(c echo.Context) error {
	if c.Form("endpoint") != "" {
		var endpoint comm.ServiceEndpoint
		if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
			return err
		}
		if endpoint.Type == "" || endpoint.Address == "" {
			return JsonErrorf(c, comm.EcodeInvalidEndpoint, "")
		}
		if err := server.xbus.Update(context.Background(), c.P(0), c.P(1), c.P(2), &endpoint); err != nil {
			return JsonError(c, err)
		}
	} else {
		keepId, ok, err := IntFormParam(c, "keep_id")
		if !ok {
			return err
		}
		if err := server.xbus.KeepAlive(context.Background(),
			c.P(0), c.P(1), c.P(2), clientv3.LeaseID(keepId)); err != nil {
			return JsonError(c, err)
		}
	}
	return JsonOk(c)
}

type QueryResult struct {
	Endpoints []comm.ServiceEndpoint `json:"endpoints"`
	Revision  int64                  `json:"revision"`
}

func (server *APIServer) Query(c echo.Context) error {
	if c.Param("watch") == "true" {
		return server.Watch(c)
	}
	if endpoints, rev, err := server.xbus.Query(context.Background(),
		c.P(0), c.P(1)); err == nil {
		return JsonResult(c, QueryResult{Endpoints: endpoints, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) Watch(c echo.Context) error {
	revision, ok, err := IntQueryParam(c, "revision")
	if !ok {
		return err
	}
	var timeout time.Duration
	if c.Query("timeout") != "" {
		if timeout, err = time.ParseDuration(c.Query("timeout")); err != nil {
			return JsonErrorf(c, comm.EcodeInvalidParam, "invalid timeout")
		}
	} else {
		timeout = DefaultWatchTimeout * time.Second
	}

	if endpoints, rev, err := server.xbus.Watch(context.Background(),
		c.P(0), c.P(1), revision, timeout); err == nil {
		return JsonResult(c, QueryResult{Endpoints: endpoints, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}
