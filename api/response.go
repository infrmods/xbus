package api

import (
	"github.com/infrmods/xbus/comm"
	"github.com/labstack/echo"
	"net/http"
)

type Response struct {
	Ok     bool        `json:"ok"`
	Result interface{} `json:"result,omitempty"`
}

var Ok = Response{Ok: true}

func JsonOk(c echo.Context) error {
	return c.JSON(http.StatusOK, Ok)
}

func JsonResult(c echo.Context, result interface{}) error {
	return c.JSON(http.StatusOK, Response{Ok: true, Result: result})
}

func JsonError(c echo.Context, err error) error {
	code := http.StatusOK
	if e, ok := err.(*comm.Error); ok {
		if e.Code == comm.EcodeSystemError {
			code = http.StatusServiceUnavailable
		}
	}
	return c.JSON(code, Response{Ok: false, Result: err})
}

func JsonErrorC(c echo.Context, code int, err error) error {
	return c.JSON(code, Response{Ok: false, Result: err})
}

func JsonErrorf(c echo.Context, errCode string, format string, args ...interface{}) error {
	err := comm.Errorf(errCode, format, args...)
	return JsonError(c, err)
}
