package api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

const (
	_MinServiceTTL       = 10
	_DefaultServiceTTL   = 60 // in seconds
	_DefaultWatchTimeout = 60 // in seconds
)

type _ServiceDescV0 struct {
	Name        string `json:"name,omitempty"`
	Version     string `json:"version,omitempty"`
	Type        string `json:"type"`
	Proto       string `json:"proto,omitempty"`
	Description string `json:"description,omitempty"`
}

func newServiceDescV0(desc *services.ServiceDescV1) _ServiceDescV0 {
	parts := strings.Split(desc.Service, ":")
	return _ServiceDescV0{
		Name:        parts[0],
		Version:     parts[1],
		Type:        desc.Type,
		Proto:       desc.Proto,
		Description: desc.Description,
	}
}

func (desc *_ServiceDescV0) toV1() services.ServiceDescV1 {
	return services.ServiceDescV1{
		Service:     fmt.Sprintf("%s:%s", desc.Name, desc.Version),
		Zone:        services.DefaultZone,
		Type:        desc.Type,
		Proto:       desc.Proto,
		Description: desc.Description,
	}
}

type serviceV0 struct {
	Endpoints []services.ServiceEndpoint `json:"endpoints"`

	_ServiceDescV0
}

func newServiceV0(service *services.ServiceV1) *serviceV0 {
	serviceZone := service.Zones[services.DefaultZone]
	if serviceZone == nil {
		return nil
	}
	return &serviceV0{
		Endpoints:      serviceZone.Endpoints,
		_ServiceDescV0: newServiceDescV0(&serviceZone.ServiceDescV1),
	}
}

type _ServicePlugResult struct {
	LeaseID clientv3.LeaseID `json:"lease_id"`
	TTL     int64            `json:"ttl"`
}

func (server *Server) v0PlugService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desc _ServiceDescV0
	if ok, err := JSONFormParam(c, "desc", &desc); !ok {
		return err
	}
	desc.Name, desc.Version = c.ParamValues()[0], c.ParamValues()[1]
	var endpoint services.ServiceEndpoint
	if ok, err := JSONFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	descV1 := desc.toV1()
	newLeaseID, err := server.services.Plug(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		&descV1, &endpoint)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, _ServicePlugResult{LeaseID: newLeaseID, TTL: ttl})
}

func (server *Server) v0PlugAllService(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	leaseID, ok, err := IntFormParamD(c, "lease_id", 0)
	if !ok {
		return err
	}

	var desces []_ServiceDescV0
	if ok, err := JSONFormParam(c, "desces", &desces); !ok {
		return err
	}
	notPermitted := make([]string, 0)
	for _, desc := range desces {
		if ok, err := server.checkPerm(c, apps.PermTypeService, true, desc.Name); err == nil {
			if !ok {
				notPermitted = append(notPermitted, desc.Name)
			}
		} else {
			return JSONError(c, err)
		}
	}
	if len(notPermitted) > 0 {
		return server.newNotPermittedResp(c, notPermitted...)
	}

	var endpoint services.ServiceEndpoint
	if ok, err := JSONFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}

	var descesV1 []services.ServiceDescV1
	for _, desc := range desces {
		descesV1 = append(descesV1, desc.toV1())
	}
	newLeaseID, err := server.services.PlugAllService(context.Background(),
		time.Duration(ttl)*time.Second, clientv3.LeaseID(leaseID),
		descesV1, &endpoint)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, _ServicePlugResult{LeaseID: newLeaseID, TTL: ttl})
}

func (server *Server) v0UnplugService(c echo.Context) error {
	service := fmt.Sprintf("%s:%s", c.ParamValues()[0], c.ParamValues()[1])
	err := server.services.Unplug(context.Background(), service, services.DefaultZone, c.ParamValues()[2])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONOk(c)
}

func (server *Server) v0UpdateService(c echo.Context) error {
	var endpoint services.ServiceEndpoint
	if ok, err := JSONFormParam(c, "endpoint", &endpoint); !ok {
		return err
	}
	params := c.ParamValues()
	if endpoint.Address == "" {
		endpoint.Address = params[2]
	} else if endpoint.Address != params[2] {
		return JSONErrorf(c, utils.EcodeInvalidParam, "can't modify address")
	}
	service := fmt.Sprintf("%s:%s", params[0], params[1])
	if err := server.services.Update(context.Background(), service, services.DefaultZone, params[2], &endpoint); err != nil {
		return JSONError(c, err)
	}
	return JSONOk(c)
}

type _ServiceItemV0 struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Type    string `json:"type"`
}

type _SearchResultV0 struct {
	Services []_ServiceItemV0 `json:"services"`
	Total    int64            `json:"total"`
}

func (server *Server) v0SearchService(c echo.Context) error {
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
	v0Services := make([]_ServiceItemV0, 0, len(result.Services))
	for _, item := range result.Services {
		parts := strings.Split(item.Service, ":")
		v0Services = append(v0Services, _ServiceItemV0{
			Name: parts[0], Version: parts[1], Type: item.Type,
		})
	}
	return JSONResult(c, _SearchResultV0{Services: v0Services, Total: result.Total})
}

type serviceQueryResultV0 struct {
	Service  *serviceV0 `json:"service"`
	Revision int64      `json:"revision"`
}

func (server *Server) v0QueryService(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.v0WatchService(c)
	}
	params := c.ParamValues()
	serviceKey := fmt.Sprintf("%s:%s", params[0], params[1])
	service, rev, err := server.services.Query(context.Background(), server.getRemoteIP(c), serviceKey)
	if err != nil {
		return JSONError(c, err)
	}
	serviceV0 := newServiceV0(service)
	if serviceV0 == nil {
		return JSONError(c, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey))
	}
	return JSONResult(c, serviceQueryResultV0{Service: serviceV0, Revision: rev})
}

func (server *Server) v0WatchService(c echo.Context) error {
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

	params := c.ParamValues()
	serviceKey := fmt.Sprintf("%s:%s", params[0], params[1])
	service, rev, err := server.services.Watch(ctx, server.getRemoteIP(c), serviceKey, revision)
	if err != nil {
		return JSONError(c, err)
	}
	serviceV0 := newServiceV0(service)
	if serviceV0 == nil {
		return JSONError(c, utils.Errorf(utils.EcodeNotFound, "no such service: %s", serviceKey))
	}
	return JSONResult(c, serviceQueryResultV0{Service: serviceV0, Revision: rev})
}
