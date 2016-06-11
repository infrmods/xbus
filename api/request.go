package api

import (
	"encoding/json"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"net/http"
	"strconv"
)

func intParam(c echo.Context, name, value string) (int64, bool, error) {
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n, true, nil
	} else {
		return 0, false, c.JSON(http.StatusBadRequest,
			utils.Errorf(utils.EcodeInvalidParam, "invalid int (%s)", name))
	}
}

func IntQueryParam(c echo.Context, name string) (int64, bool, error) {
	val := c.QueryParam(name)
	if val == "" {
		return 0, false, c.JSON(http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	return intParam(c, name, val)
}

func IntQueryParamD(c echo.Context, name string, defval int64) (int64, bool, error) {
	val := c.QueryParam(name)
	if val == "" {
		return defval, true, nil
	}
	return intParam(c, name, val)
}

func IntFormParam(c echo.Context, name string) (int64, bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return 0, false, c.JSON(http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	return intParam(c, name, val)
}

func IntFormParamD(c echo.Context, name string, defval int64) (int64, bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return defval, true, nil
	}
	return intParam(c, name, val)
}

func JsonFormParam(c echo.Context, name string, v interface{}) (bool, error) {
	val := c.FormValue(name)
	if val == "" {
		return false, c.JSON(http.StatusBadRequest,
			utils.Errorf(utils.EcodeMissingParam, "missing %s", name))
	}
	if err := json.Unmarshal([]byte(val), v); err == nil {
		return true, nil
	} else {
		return false, c.JSON(http.StatusBadRequest,
			utils.Errorf(utils.EcodeInvalidParam, "invalid json (%s)", name))
	}
}
