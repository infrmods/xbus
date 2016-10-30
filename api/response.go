package api

import (
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"net/http"
)

type Response struct {
	Ok     bool        `json:"ok"`
	Result interface{} `json:"result,omitempty"`
	Error  interface{} `json:"error,omitempty"`
}

var Ok = Response{Ok: true}

func JsonOk(c echo.Context) error {
	return c.JSON(http.StatusOK, Ok)
}

func JsonResult(c echo.Context, result interface{}) error {
	return c.JSON(http.StatusOK, Response{Ok: true, Result: result})
}

func formatError(err error) *utils.Error {
	if e, ok := err.(*utils.Error); ok {
		return e
	} else {
		return &utils.Error{Code: utils.EcodeSystemError, Message: err.Error()}
	}
}

func JsonError(c echo.Context, err error) error {
	code := http.StatusOK
	e := formatError(err)
	if e.Code == utils.EcodeSystemError {
		code = http.StatusServiceUnavailable
	}
	return c.JSON(code, Response{Ok: false, Error: err})
}

func JsonErrorC(c echo.Context, code int, err error) error {
	return c.JSON(code, Response{Ok: false, Error: formatError(err)})
}

func JsonErrorf(c echo.Context, errCode string, format string, args ...interface{}) error {
	err := utils.Errorf(errCode, format, args...)
	return JsonError(c, err)
}
