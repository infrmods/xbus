package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
)

func intParam(c echo.Context, name, value string) (int64, bool, error) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, JSONErrorC(c, http.StatusBadRequest,
			utils.Errorf(utils.EcodeInvalidParam, "invalid int (%s): %v", name, err))
	}
	return n, true, nil
}

// IntQueryParam int query param
func IntQueryParam(c echo.Context, name string) (int64, bool, error) {
	val := c.QueryParam(name)
	if val == "" {
		return 0, false, JSONErrorC(c, http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	return intParam(c, name, val)
}

// IntQueryParamD int query param with default
func IntQueryParamD(c echo.Context, name string, defval int64) (int64, bool, error) {
	val := c.QueryParam(name)
	if val == "" {
		return defval, true, nil
	}
	return intParam(c, name, val)
}

// IntFormParam int form param
func IntFormParam(c echo.Context, name string) (int64, bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return 0, false, JSONErrorC(c, http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	return intParam(c, name, val)
}

// IntFormParamD int form param with default
func IntFormParamD(c echo.Context, name string, defval int64) (int64, bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return defval, true, nil
	}
	return intParam(c, name, val)
}

// JSONFormParam json form param
func JSONFormParam(c echo.Context, name string, v interface{}) (bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return false, JSONErrorC(c, http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	err := json.Unmarshal([]byte(val), v)
	if err != nil {
		return false, JSONErrorC(c, http.StatusBadRequest,
			utils.Errorf(utils.EcodeInvalidParam, "invalid json (%s): %v", name, err))
	}
	return true, nil
}
