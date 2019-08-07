package api

import (
	"net/http"

	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

// Response simple response
type Response struct {
	Ok     bool        `json:"ok"`
	Result interface{} `json:"result,omitempty"`
	Error  interface{} `json:"error,omitempty"`
}

// Ok Response
var Ok = Response{Ok: true}

// JSONOk json ok result
func JSONOk(c echo.Context) error {
	return c.JSON(http.StatusOK, Ok)
}

// JSONResult json result
func JSONResult(c echo.Context, result interface{}) error {
	return c.JSON(http.StatusOK, Response{Ok: true, Result: result})
}

func formatError(err error) *utils.Error {
	e, ok := err.(*utils.Error)
	if ok {
		return e
	}
	return &utils.Error{Code: utils.EcodeSystemError, Message: err.Error()}
}

// JSONError json error
func JSONError(c echo.Context, err error) error {
	code := http.StatusOK
	e := formatError(err)
	if e.Code == utils.EcodeSystemError {
		code = http.StatusServiceUnavailable
	}
	return c.JSON(code, Response{Ok: false, Error: err})
}

// JSONErrorC json error with code
func JSONErrorC(c echo.Context, code int, err error) error {
	return c.JSON(code, Response{Ok: false, Error: formatError(err)})
}

// JSONErrorf json error format
func JSONErrorf(c echo.Context, errCode string, format string, args ...interface{}) error {
	err := utils.Errorf(errCode, format, args...)
	return JSONError(c, err)
}
