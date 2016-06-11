package api

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"golang.org/x/net/context"
	"time"
)

type RangeResult struct {
	Configs []configs.ConfigItem `json:"configs"`
	More    bool                 `json:"more"`
}

func (server *APIServer) RangeConfigs(c echo.Context) error {
	from := c.QueryParam("from")
	end := c.QueryParam("end")
	sortOption := clientv3.SortOption{Target: clientv3.SortByKey}
	switch c.QueryParam("order") {
	case "asc", "":
		sortOption.Order = clientv3.SortAscend
	case "desc":
		sortOption.Order = clientv3.SortDescend
	default:
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid order")
	}

	if cfgs, more, err := server.configs.Range(context.Background(), from, end, &sortOption); err == nil {
		return JsonResult(c, RangeResult{Configs: cfgs, More: more})
	} else {
		return JsonError(c, err)
	}
}

type ConfigQueryResult struct {
	Config   *configs.ConfigItem `json:"config"`
	Revision int64               `json:"revision"`
}

func (server *APIServer) GetConfig(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.Watch(c)
	}

	if cfg, rev, err := server.configs.Get(context.Background(), c.P(0)); err == nil {
		return JsonResult(c, ConfigQueryResult{Config: cfg, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

type ConfigPutResult struct {
	Revision int64 `json:"revision"`
}

func (server *APIServer) PutConfig(c echo.Context) error {
	value := c.FormValue("value")
	if value == "" {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid value")
	}
	version, ok, err := IntFormParamD(c, "version", 0)
	if !ok {
		return err
	}

	if rev, err := server.configs.Put(context.Background(), c.P(0), value, version); err == nil {
		return JsonResult(c, ConfigPutResult{Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) Watch(c echo.Context) error {
	revision, ok, err := IntQueryParamD(c, "version", 0)
	if !ok {
		return err
	}
	var timeout time.Duration
	if c.QueryParam("timeout") != "" {
		if timeout, err = time.ParseDuration(c.QueryParam("timeout")); err != nil {
			return JsonErrorf(c, utils.EcodeInvalidParam, "invalid timeout")
		}
	} else {
		timeout = DefaultWatchTimeout * time.Second
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	if cfg, rev, err := server.configs.Watch(ctx, c.P(0), revision); err == nil {
		return JsonResult(c, ConfigQueryResult{Config: cfg, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}
