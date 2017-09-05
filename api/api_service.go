package api

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"golang.org/x/net/context"
	"time"
)

const (
	MinServiceTTL       = 10
	DefaultServiceTTL   = 60 // in seconds
	DefaultWatchTimeout = 60 // in seconds
)

type ServicePlugResult struct {
	LeaseID clientv3.LeaseID `json:"lease_id"`
	TTL     int64            `json:"ttl"`
}

func (server *APIServer) PlugService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < MinServiceTTL {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseId, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desc services.ServiceDesc
	if ok, err := JsonFormParam(c, "desc", &desc); !ok {
		return err
	}
	desc.Name, desc.Version = c.P(0), c.P(1)
	var endpoint services.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	if leaseId, err := server.services.Plug(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseId),
		&desc, &endpoint); err == nil {
		return JsonResult(c, ServicePlugResult{LeaseID: leaseId, TTL: ttl})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) PlugAllService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < MinServiceTTL {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseId, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desces []services.ServiceDesc
	if ok, err := JsonFormParam(c, "desces", &desces); !ok {
		return err
	}
	for _, desc := range desces {
		if ok, err := server.checkPerm(c, apps.PermTypeService, true, desc.Name); err == nil {
			if !ok {
				return JsonError(c, newNotPermittedErr(server.appName(c), desc.Name))
			}
		} else {
			return JsonError(c, err)
		}
	}

	var endpoint services.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	if leaseId, err := server.services.PlugAllService(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseId),
		desces, &endpoint); err == nil {
		return JsonResult(c, ServicePlugResult{LeaseID: leaseId, TTL: ttl})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) UnplugService(c echo.Context) error {
	if err := server.services.Unplug(context.Background(), c.P(0), c.P(1), c.P(2)); err == nil {
		return JsonOk(c)
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) UpdateService(c echo.Context) error {
	var endpoint services.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}
	if endpoint.Address == "" {
		endpoint.Address = c.P(2)
	} else if endpoint.Address != c.P(2) {
		return JsonErrorf(c, utils.EcodeInvalidParam, "can't modify address")
	}
	if err := server.services.Update(context.Background(), c.P(0), c.P(1), c.P(2), &endpoint); err != nil {
		return JsonError(c, err)
	}
	return JsonOk(c)
}

type ServiceQueryResult struct {
	Service  *services.Service `json:"service"`
	Revision int64             `json:"revision"`
}

func (server *APIServer) QueryService(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.WatchService(c)
	}
	if service, rev, err := server.services.Query(context.Background(),
		c.P(0), c.P(1)); err == nil {
		return JsonResult(c, ServiceQueryResult{Service: service, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

type AllServiceQueryResult struct {
	Services map[string]*services.Service `json:"services"`
	Revision int64                        `json:"revision"`
}

func (server *APIServer) QueryServiceAllVersions(c echo.Context) error {
	if services, rev, err := server.services.QueryAllVersions(context.Background(), c.P(0)); err == nil {
		return JsonResult(c, AllServiceQueryResult{Services: services, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) WatchService(c echo.Context) error {
	revision, ok, err := IntQueryParamD(c, "revision", 0)
	if !ok {
		return err
	}
	timeout, ok, err := IntQueryParamD(c, "timeout", DefaultWatchTimeout)
	if !ok {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancelFunc()

	if service, rev, err := server.services.Watch(ctx,
		c.P(0), c.P(1), revision); err == nil {
		return JsonResult(c, ServiceQueryResult{Service: service, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}
