package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

type listResult struct {
	Total   int64                `json:"total"`
	Configs []configs.ConfigInfo `json:"configs"`
	Skip    int                  `json:"skip"`
	Limit   int                  `json:"limit"`
}

func (server *Server) listConfig(c echo.Context) error {
	if c.QueryParam("keys") != "" {
		return server.getAllConfigs(c)
	}

	tag := c.QueryParam("tag")
	prefix := c.QueryParam("prefix")
	skip, ok, err := IntQueryParamD(c, "skip", 0)
	if !ok {
		return err
	}
	limit, ok, err := IntQueryParamD(c, "limit", 200)
	if !ok {
		return err
	}

	total, configs, err := server.configs.ListDBConfigs(context.Background(), tag, prefix, int(skip), int(limit))
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c,
		listResult{Total: total, Configs: configs, Skip: int(skip), Limit: int(limit)})
}

type configQueryResult struct {
	Config   *configs.ConfigItem `json:"config"`
	Revision int64               `json:"revision"`
}

func (server *Server) getConfig(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		return server.watch(c)
	}
	node := c.Request().Header.Get("node")

	cfg, rev, err := server.configs.Get(context.Background(), server.appID(c), node, c.ParamValues()[0])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, configQueryResult{Config: cfg, Revision: rev})
}

type configsQueryResult struct {
	Configs  []*configs.ConfigItem `json:"configs"`
	Revision int64                 `json:"revision"`
}

func (server *Server) getAllConfigs(c echo.Context) error {
	ks := c.QueryParam("keys")
	if len(ks) == 0 {
		return JSONErrorf(c, utils.EcodeMissingParam, "missing keys")
	}
	var keys []string
	if err := json.Unmarshal([]byte(ks), &keys); err != nil {
		return JSONErrorf(c, utils.EcodeInvalidParam, "invalid keys: %v", err)
	}
	notPermitted := make([]string, 0)
	for _, key := range keys {
		if ok, err := server.checkPerm(c, apps.PermTypeConfig, false, key); err != nil {
			return JSONError(c, err)
		} else if !ok {
			notPermitted = append(notPermitted, key)
		}
	}
	if len(notPermitted) != 0 {
		return server.newNotPermittedResp(c, notPermitted...)
	}

	node := c.Request().Header.Get("node")
	result := configsQueryResult{Configs: make([]*configs.ConfigItem, 0, len(keys)), Revision: 0}
	for _, key := range keys {
		if cfg, rev, err := server.configs.Get(context.Background(), server.appID(c), node, key); err == nil {
			if result.Revision > 0 && rev < result.Revision {
				result.Revision = rev
			}
			result.Configs = append(result.Configs, cfg)
		} else {
			return JSONError(c, err)
		}
	}
	return JSONResult(c, result)
}

func (server *Server) deleteConfig(c echo.Context) error {
	err := server.configs.Delete(context.Background(), c.ParamValues()[0])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONOk(c)
}

type configPutResult struct {
	Revision int64 `json:"revision"`
}

func (server *Server) putConfig(c echo.Context) error {
	tag := c.FormValue("tag")
	value := c.FormValue("value")
	if value == "" {
		return JSONErrorf(c, utils.EcodeInvalidValue, "invalid value")
	}
	version, ok, err := IntFormParamD(c, "version", 0)
	if !ok {
		return err
	}
	remark := c.FormValue("remark")

	rev, err := server.configs.Put(context.Background(), tag, c.ParamValues()[0], server.appID(c), remark, value, version)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, configPutResult{Revision: rev})
}

func (server *Server) watch(c echo.Context) error {
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
	node := c.Request().Header.Get("node")

	cfg, rev, err := server.configs.Watch(ctx, server.appID(c), node, c.ParamValues()[0], revision)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, configQueryResult{Config: cfg, Revision: rev})
}
