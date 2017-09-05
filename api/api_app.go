package api

import (
	"github.com/infrmods/xbus/apps"
	"github.com/labstack/echo"
)

func (server *APIServer) GetAppCert(c echo.Context) error {
	app, err := server.apps.GetAppByName(c.P(0))
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
			return JsonError(c, newNotPermittedErr(server.appName(c), "app perm"))
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
