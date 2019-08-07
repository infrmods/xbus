package api

import (
	"context"
	"time"

	"github.com/golang/glog"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

func (server *Server) getAppCert(c echo.Context) error {
	app, err := server.apps.GetAppByName(c.ParamValues()[0])
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, app.Cert)
}

type listAppResult struct {
	Apps  []apps.App `json:"apps"`
	Skip  int        `json:"skip"`
	Limit int        `json:"limit"`
}

func (server *Server) listApp(c echo.Context) error {
	if ok, err := server.checkPerm(c, apps.PermTypeApp, false, ""); err == nil {
		if !ok {
			return server.newNotPermittedResp(c, "app perm")
		}
	} else {
		return err
	}
	skip, ok, err := IntQueryParamD(c, "skip", 0)
	if !ok {
		return err
	}
	limit, ok, err := IntQueryParamD(c, "limit", 100)
	if !ok {
		return err
	}

	apps, err := server.apps.ListApp(int(skip), int(limit))
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, listAppResult{Apps: apps, Skip: int(skip), Limit: int(limit)})
}

type newAppRequest struct {
	Name        string `json:"name" form:"name"`
	Description string `json:"description" form:"description"`
	KeyBits     int    `json:"key_bits" form:"key_bits"`
	Days        int    `json:"days" form:"days"`
}

func (server *Server) newApp(c echo.Context) error {
	if ok, err := server.checkPerm(c, apps.PermTypeApp, false, ""); err == nil {
		if !ok {
			return server.newNotPermittedResp(c, "app perm")
		}
	} else {
		return err
	}
	var req newAppRequest
	if err := c.Bind(&req); err != nil {
		return err
	}
	privKey, err := utils.NewPrivateKey("", req.KeyBits)
	if err != nil {
		glog.Errorf("generate private key fail: %v", err)
		return JSONErrorf(c, "SYSTEM_BUSY", "create private key fail")
	}
	app := apps.App{
		Status:      utils.StatusOk,
		Name:        req.Name,
		Description: req.Description}
	_, err = server.apps.NewApp(&app, privKey, nil, nil, req.Days)
	if err != nil {
		glog.Errorf("create app fail: %v", err)
		return JSONError(c, err)
	}
	return JSONResult(c, app)
}

func (server *Server) watchAppNodes(c echo.Context) error {
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

	appName := c.ParamValues()[0]
	label := c.QueryParam("label")
	if label == "" {
		label = "default"
	}

	nodes, err := server.apps.WatchAppNodes(ctx, appName, label, revision)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, nodes)
}

func (server *Server) isAppNodeOnline(c echo.Context) error {
	appName := c.ParamValues()[0]
	label := c.QueryParam("label")
	if label == "" {
		label = "default"
	}
	key := c.QueryParam("key")
	if key == "" {
		return JSONErrorf(c, utils.EcodeMissingParam, "missing key")
	}

	online, err := server.apps.IsAppNodeOnline(context.Background(), appName, label, key)
	if err != nil {
		return JSONError(c, err)
	}
	return JSONResult(c, online)
}
