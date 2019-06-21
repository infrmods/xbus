package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

type LeaseGrantResult struct {
	TTL     int64            `json:"ttl"`
	LeaseId clientv3.LeaseID `json:"lease_id"`
}

func (server *APIServer) GrantLease(c echo.Context) error {
	ttl, ok, err := IntFormParamD(c, "ttl", 60)
	if !ok {
		return err
	}
	if ttl > 0 && ttl < _MinServiceTTL {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid ttl: %d", ttl)
	}
	if rep, err := server.etcdClient.Grant(context.Background(), ttl); err == nil {
		return JsonResult(c, LeaseGrantResult{
			TTL:     rep.TTL,
			LeaseId: rep.ID,
		})
	} else {
		return JsonError(c, utils.CleanErr(err, "grant lease fail", "grant lease(ttl: %d) fail: %v", ttl, err))
	}
}

func parseLeaseId(s string) (clientv3.LeaseID, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return clientv3.LeaseID(n), nil
	} else {
		return 0, echo.NewHTTPError(http.StatusNotFound)
	}
}

func (server *APIServer) KeepAliveLease(c echo.Context) error {
	leaseId, err := parseLeaseId(c.ParamValues()[0])
	if err != nil {
		return err
	}
	if _, err := server.etcdClient.KeepAliveOnce(context.Background(), leaseId); err == nil {
		return JsonOk(c)
	} else {
		return JsonError(c, utils.CleanErr(err, "keepalive fail", "keepalive(%d) fail: %v", leaseId, err))
	}
}

func (server *APIServer) RevokeLease(c echo.Context) error {
	leaseId, err := parseLeaseId(c.ParamValues()[0])
	if err != nil {
		return err
	}
	if _, err := server.etcdClient.Revoke(context.Background(), leaseId); err == nil {
		return JsonOk(c)
	} else {
		return JsonError(c, utils.CleanErr(err, "revoke fail", "revoke(%d) fail: %v", leaseId, err))
	}
}
