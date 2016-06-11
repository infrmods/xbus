package api

import (
	"github.com/labstack/echo"
)

func (server *APIServer) GetAppCert(c echo.Context) error {
	app, err := server.apps.GetAppByName(c.P(0))
	if err != nil {
		// TODO: err in utils err
		return JsonError(c, err)
	}
	return JsonResult(c, app.Cert)
}
