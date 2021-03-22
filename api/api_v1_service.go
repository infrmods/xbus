package api

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

const (
	minServiceTTL       = 10 // in seconds
	defaultServiceTTL   = 60 // in seconds
	defaultWatchTimeout = 60 // in seconds
)

// ServicePlugResult service plug result
type ServicePlugResult struct {
	LeaseID clientv3.LeaseID `json:"lease_id"`
	TTL     int64            `json:"ttl"`
}

func (server *Server) v1PlugService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < minServiceTTL {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desc services.ServiceDescV1
	if ok, err := JSONFormParam(c, "desc", &desc); !ok {
		return err
	}
	if desc.Zone == "" {
		desc.Zone = services.DefaultZone
	}
	desc.Service = c.ParamValues()[0]
	var endpoint services.ServiceEndpoint
	if ok, err := JSONFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	if leaseID, err := server.services.PlugAll(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		[]services.ServiceDescV1{desc}, &endpoint); err == nil {
		return JSONResult(c, ServicePlugResult{LeaseID: leaseID, TTL: ttl})
	}
	return JSONError(c, err)
}

func (server *Server) v1PlugAllService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < minServiceTTL {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var descs []services.ServiceDescV1
	if c.FormValue("descs") != "" {
		if ok, err := JSONFormParam(c, "descs", &descs); !ok {
			return err
		}
	} else {
		if ok, err := JSONFormParam(c, "desces", &descs); !ok {
			return err
		}
	}
	notPermitted := make([]string, 0)
	for _, desc := range descs {
		if ok, err := server.checkPerm(c, apps.PermTypeService, true, desc.Service); err == nil {
			if !ok {
				notPermitted = append(notPermitted, desc.Service)
			}
		} else {
			return JSONError(c, err)
		}
		if desc.Zone == "" {
			desc.Zone = services.DefaultZone
		}
	}
	if len(notPermitted) > 0 {
		return server.newNotPermittedResp(c, notPermitted...)
	}

	var endpoint services.ServiceEndpoint
	if ok, err := JSONFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	newLeaseID, err := server.services.PlugAll(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		descs, &endpoint)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, ServicePlugResult{LeaseID: newLeaseID, TTL: ttl})
}

func (server *Server) v1UnplugService(c echo.Context) error {
	params := c.ParamValues()
	err := server.services.Unplug(context.Background(), params[0], params[1], params[2])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONOk(c)
}

func (server *Server) v1SearchService(c echo.Context) error {
	skip, ok, err := IntQueryParamD(c, "skip", 0)
	if !ok {
		return err
	}
	limit, ok, err := IntQueryParamD(c, "limit", 200)
	if !ok {
		return err
	}
	result, err := server.services.SearchService(c.QueryParam("q"), skip, limit)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, result)
}

type serviceQueryResultV1 struct {
	Service  *services.ServiceV1 `json:"service"`
	Revision int64               `json:"revision"`
}

type serviceQueryRawZoneResultV1 struct {
	Service  *services.ServiceWithRawZone `json:"service"`
	Revision int64                        `json:"revision"`
}

func (server *Server) v1QueryService(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.v1WatchService(c)
	}

	if c.QueryParam("only_zone") == "true" {
		service, rev, err := server.services.QueryZones(context.Background(), server.getRemoteIP(c), c.ParamValues()[0])
		if err != nil {
			return JSONError(c, err)
		}
		return JSONResult(c, serviceQueryRawZoneResultV1{Service: service, Revision: rev})
	}

	service, rev, err := server.services.Query(context.Background(), server.getRemoteIP(c), c.ParamValues()[0])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, serviceQueryResultV1{Service: service, Revision: rev})
}

func (server *Server) v1QueryServiceZone(c echo.Context) error {
	service, rev, err := server.services.QueryServiceZone(
		context.Background(),
		server.getRemoteIP(c),
		c.ParamValues()[0],
		c.ParamValues()[1],
	)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, serviceQueryResultV1{Service: service, Revision: rev})
}

func (server *Server) v1WatchService(c echo.Context) error {
	revision, ok, err := IntQueryParamD(c, "revision", 0)
	if !ok {
		return err
	}
	timeout, ok, err := IntQueryParamD(c, "timeout", defaultWatchTimeout)
	if !ok {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancelFunc()

	service, rev, err := server.services.Watch(ctx, server.getRemoteIP(c), c.ParamValues()[0], revision)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, serviceQueryResultV1{Service: service, Revision: rev})
}

func (server *Server) v1DeleteService(c echo.Context) error {
	zone := c.QueryParam("zone")
	if err := server.services.Delete(context.Background(), c.ParamValues()[0], zone); err != nil {
		return JSONError(c, err)
	}
	return JSONOk(c)
}

func (server *Server) v1WatchServiceDesc(c echo.Context) error {
	zone := c.QueryParam("zone")
	revision, ok, err := IntQueryParamD(c, "revision", 0)
	if !ok {
		return err
	}
	timeout, ok, err := IntQueryParamD(c, "timeout", defaultWatchTimeout)
	if !ok {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancelFunc()

	if !server.ProtoSwitch {
		result, err := server.services.WatchServiceDescBack(ctx, zone, revision)
		if err != nil {
			return JSONError(c, err)
		}
		return JSONResult(c, result)
	}
	result, err := server.services.WatchServiceDesc(ctx, zone, revision)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, result)
}
