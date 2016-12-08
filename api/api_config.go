package api

import (
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/infrmods/xbus/apps"
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
	node := c.Request().Header().Get("node")

	if cfg, rev, err := server.configs.Get(context.Background(), server.appId(c), node, c.P(0)); err == nil {
		return JsonResult(c, ConfigQueryResult{Config: cfg, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

type ConfigsQueryResult struct {
	Configs  []*configs.ConfigItem `json:"configs"`
	Revision int64                 `json:"revision"`
}

func (server *APIServer) GetAllConfigs(c echo.Context) error {
	ks := c.QueryParam("keys")
	if len(ks) == 0 {
		return JsonErrorf(c, utils.EcodeMissingParam, "missing keys")
	}
	var keys []string
	if err := json.Unmarshal([]byte(ks), &keys); err != nil {
		return JsonErrorf(c, utils.EcodeInvalidParam, "invalid keys: %v", err)
	}
	for _, key := range keys {
		if ok, err := server.checkPerm(c, apps.PermTypeConfig, false, key); err != nil {
			return JsonError(c, err)
		} else if !ok {
			return JsonError(c, ErrNotPermitted)
		}
	}

	node := c.Request().Header().Get("node")
	result := ConfigsQueryResult{Configs: make([]*configs.ConfigItem, 0, len(keys)), Revision: 0}
	for _, key := range keys {
		if cfg, rev, err := server.configs.Get(context.Background(), server.appId(c), node, key); err == nil {
			if result.Revision > 0 && rev < result.Revision {
				result.Revision = rev
			}
			result.Configs = append(result.Configs, cfg)
		} else {
			return JsonError(c, err)
		}
	}
	return JsonResult(c, result)
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

	if rev, err := server.configs.Put(context.Background(), c.P(0), server.appId(c), value, version); err == nil {
		return JsonResult(c, ConfigPutResult{Revision: rev})
	} else {
		return JsonError(c, err)
	}
}

func (server *APIServer) Watch(c echo.Context) error {
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
	node := c.Request().Header().Get("node")

	if cfg, rev, err := server.configs.Watch(ctx, server.appId(c), node, c.P(0), revision); err == nil {
		return JsonResult(c, ConfigQueryResult{Config: cfg, Revision: rev})
	} else {
		return JsonError(c, err)
	}
}
