package api

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
)

func (server *APIServer) v1PlugService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desc services.ServiceDescV1
	if ok, err := JsonFormParam(c, "desc", &desc); !ok {
		return err
	}
	if desc.Zone == "" {
		desc.Zone = services.DefaultZone
	}
	desc.Service = c.ParamValues()[0]
	var endpoint services.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	if leaseID, err := server.services.Plug(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		&desc, &endpoint); err == nil {
		return JsonResult(c, _ServicePlugResult{LeaseID: leaseID, TTL: ttl})
	}
	return JsonError(c, err)
}

func (server *APIServer) v1PlugAllService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desces []services.ServiceDescV1
	if ok, err := JsonFormParam(c, "desces", &desces); !ok {
		return err
	}
	notPermitted := make([]string, 0)
	for _, desc := range desces {
		if ok, err := server.checkPerm(c, apps.PermTypeService, true, desc.Service); err == nil {
			if !ok {
				notPermitted = append(notPermitted, desc.Service)
			}
		} else {
			return JsonError(c, err)
		}
		if desc.Zone == "" {
			desc.Zone = services.DefaultZone
		}
	}
	if len(notPermitted) > 0 {
		return server.newNotPermittedResp(c, notPermitted...)
	}

	var endpoint services.ServiceEndpoint
	if ok, err := JsonFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	if leaseID, err := server.services.PlugAllService(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		desces, &endpoint); err == nil {
		return JsonResult(c, _ServicePlugResult{LeaseID: leaseID, TTL: ttl})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) v1UnplugService(c echo.Context) error {
	params := c.ParamValues()
	if err := server.services.Unplug(context.Background(), params[0], params[1], params[2]); err == nil {
		return JsonOk(c)
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) v1SearchService(c echo.Context) error {
	skip, ok, err := IntQueryParamD(c, "skip", 0)
	if !ok {
		return err
	}
	limit, ok, err := IntQueryParamD(c, "limit", 200)
	if !ok {
		return err
	}
	if result, err := server.services.SearchService(c.QueryParam("q"), skip, limit); err == nil {
		return JsonResult(c, result)
	} else {
		return JsonError(c, err)
	}
}

type _ServiceQueryResultV1 struct {
	Service  *services.ServiceV1 `json:"service"`
	Revision int64               `json:"revision"`
}

func (server *APIServer) v1QueryService(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.v1WatchService(c)
	}
	if service, rev, err := server.services.Query(context.Background(), server.getRemoteIP(c), c.ParamValues()[0]); err == nil {
		return JsonResult(c, _ServiceQueryResultV1{Service: service, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) v1WatchService(c echo.Context) error {
	revision, ok, err := IntQueryParamD(c, "revision", 0)
	if !ok {
		return err
	}
	timeout, ok, err := IntQueryParamD(c, "timeout", _DefaultWatchTimeout)
	if !ok {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancelFunc()

	if service, rev, err := server.services.Watch(ctx, server.getRemoteIP(c), c.ParamValues()[0], revision); err == nil {
		return JsonResult(c, _ServiceQueryResultV1{Service: service, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) v1DeleteService(c echo.Context) error {
	zone := c.QueryParam("zone")
	if err := server.services.Delete(context.Background(), c.ParamValues()[0], zone); err != nil {
		return JsonError(c, err)
	}
	return JsonOk(c)
}
