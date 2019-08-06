package api

import (
	"context"

	"github.com/golang/glog"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

func (server *APIServer) GetAppCert(c echo.Context) error {
	app, err := server.apps.GetAppByName(c.ParamValues()[0])
	if err != nil {
		return JsonError(c, err)
	}
	return JsonResult(c, app.Cert)
}

type ListAppResult struct {
	Apps  []apps.App `json:"apps"`
	Skip  int        `json:"skip"`
	Limit int        `json:"limit"`
}

func (server *APIServer) ListApp(c echo.Context) error {
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

	if apps, err := server.apps.ListApp(int(skip), int(limit)); err == nil {
		return JsonResult(c, ListAppResult{Apps: apps, Skip: int(skip), Limit: int(limit)})
	} else {
		return JsonError(c, err)
	}
}

type NewAppRequest struct {
	Name        string `json:"name" form:"name"`
	Description string `json:"description" form:"description"`
	KeyBits     int    `json:"key_bits" form:"key_bits"`
	Days        int    `json:"days" form:"days"`
}

func (server *APIServer) NewApp(c echo.Context) error {
	if ok, err := server.checkPerm(c, apps.PermTypeApp, false, ""); err == nil {
		if !ok {
			return server.newNotPermittedResp(c, "app perm")
		}
	} else {
		return err
	}
	var req NewAppRequest
	if err := c.Bind(&req); err != nil {
		return err
	}
	privKey, err := utils.NewPrivateKey("", req.KeyBits)
	if err != nil {
		glog.Errorf("generate private key fail: %v", err)
		return JsonErrorf(c, "SYSTEM_BUSY", "create private key fail")
	}
	app := apps.App{
		Status:      utils.StatusOk,
		Name:        req.Name,
		Description: req.Description}
	if _, err := server.apps.NewApp(&app, privKey, nil, nil, req.Days); err == nil {
		return JsonResult(c, app)
	} else {
		glog.Errorf("create app fail: %v", err)
		return JsonError(c, err)
	}
}

func (server *APIServer) watchAppNodes(c echo.Context) error {
	revision, ok, err := IntQueryParamD(c, "revision", 0)
	if !ok {
		return err
	}
	appName := c.ParamValues()[0]
	label := c.QueryParam("label")
	if label == "" {
		label = "default"
	}
	if nodes, err := server.apps.WatchAppNodes(context.Background(), appName, label, revision); err == nil {
		return JsonResult(c, nodes)
	} else {
		return JsonError(c, err)
	}
}
