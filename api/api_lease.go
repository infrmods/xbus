package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

type leaseGrantResult struct {
	TTL        int64            `json:"ttl"`
	LeaseID    clientv3.LeaseID `json:"lease_id"`
	NewAppNode bool             `json:"new_app_node"`
}

func (server *Server) grantLease(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	app := server.app(c)
	var appNode *apps.AppNode
	if c.FormValue("app_node") != "" {
		var value apps.AppNode
		ok, err := JSONFormParam(c, "app_node", &value)
		if !ok {
			return err
		}
		appNode = &value
	}
	if appNode != nil && app == nil {
		return JSONErrorf(c, utils.EcodeInvalidParam, "missing app config")
	}
	ctx := context.Background()
	rep, err := server.etcdClient.Grant(ctx, ttl)
	if err != nil {
		return JSONError(c, utils.CleanErr(err, "grant lease fail", "grant lease(ttl: %d) fail: %v", ttl, err))
	}
	newAppNode := false
	if appNode != nil {
		if newAppNode, err = server.apps.PlugAppNode(ctx, app.Name, appNode, rep.ID); err != nil {
			return JSONError(c, err)
		}
	}
	return JSONResult(c, leaseGrantResult{
		TTL:        rep.TTL,
		LeaseID:    rep.ID,
		NewAppNode: newAppNode,
	})
}

func parseLeaseID(s string) (clientv3.LeaseID, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, echo.NewHTTPError(http.StatusNotFound)
	}
	return clientv3.LeaseID(n), nil
}

func (server *Server) keepAliveLease(c echo.Context) error {
	leaseID, err := parseLeaseID(c.ParamValues()[0])
	if err != nil {
		return err
	}
	_, err = server.etcdClient.KeepAliveOnce(context.Background(), leaseID)
	if err != nil {
		return JSONError(c, utils.CleanErr(err, "keepalive fail", "keepalive(%d) fail: %v", leaseID, err))
	}
	return JSONOk(c)
}

func (server *Server) revokeLease(c echo.Context) error {
	leaseID, err := parseLeaseID(c.ParamValues()[0])
	if err != nil {
		return err
	}
	nodeKey := c.QueryParam("rm_node_key")
	if nodeKey != "" {
		app := server.app(c)
		if app == nil {
			return JSONErrorf(c, utils.EcodeInvalidParam, "missing app config")
		}
		label := c.QueryParam("app_node_label")
		if label == "" {
			label = "default"
		}
		if err := server.apps.RemoveAppNode(context.Background(), app.Name, label, nodeKey); err != nil {
			return JSONError(c, err)
		}
	}
	_, err = server.etcdClient.Revoke(context.Background(), leaseID)
	if err != nil {
		return JSONError(c, utils.CleanErr(err, "revoke fail", "revoke(%d) fail: %v", leaseID, err))
	}
	return JSONOk(c)
}
