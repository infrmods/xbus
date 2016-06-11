package api

import (
	"crypto/tls"
	"github.com/facebookgo/httpdown"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/labstack/echo/middleware"
	"net/http"
	"strings"
	"time"
)

const (
	ConfigPublicInfix = ".public."
)

type Config struct {
	Listen      string        `default:"127.0.0.1:4433"`
	CertFile    string        `default:"apicert.pem"`
	KeyFile     string        `default:"apikey.pem"`
	StopTimeout time.Duration `default:"5s" yaml:"stop_timeout"`
	KillTimeout time.Duration `default:"20s" yaml:"kill_timeout"`

	PermitPublicServiceQuery bool `default:"true"`
}

type APIServer struct {
	config Config
	tls    bool

	services *services.ServiceCtrl
	configs  *configs.ConfigCtrl
	apps     *apps.AppCtrl

	httpdown.Server
}

func NewAPIServer(config *Config, servs *services.ServiceCtrl,
	cfgs *configs.ConfigCtrl, apps *apps.AppCtrl) *APIServer {
	server := &APIServer{config: *config, tls: config.CertFile != "",
		services: servs, configs: cfgs, apps: apps}
	return server
}

func (server *APIServer) Start() error {
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(echo.MiddlewareFunc(server.verifyApp))
	server.registerServiceAPIs(e.Group("/api/services"))
	server.registerConfigAPIs(e.Group("/api/configs"))
	server.registerAppAPIs(e.Group("/api/apps"))
	var std *standard.Server
	addr := server.config.Listen
	if server.config.CertFile != "" {
		if !strings.Contains(addr, ":") {
			addr += ":https"
		}
		std = standard.WithTLS(addr, server.config.CertFile, server.config.KeyFile)
		cert, err := tls.LoadX509KeyPair(server.config.CertFile, server.config.KeyFile)
		if err != nil {
			return err
		}
		std.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    server.apps.GetAppCertPool(),
			ClientAuth:   tls.VerifyClientCertIfGiven}
		std.TLSConfig.BuildNameToCertificate()
	} else {
		if !strings.Contains(addr, ":") {
			addr += ":http"
		}
		std = standard.New(addr)
	}
	std.SetHandler(e)

	hd := &httpdown.HTTP{
		StopTimeout: server.config.StopTimeout,
		KillTimeout: server.config.KillTimeout}
	if ser, err := hd.ListenAndServe(std.Server); err == nil {
		glog.Infof("api server listening on: %s", server.config.Listen)
		server.Server = ser
	} else {
		return err
	}
	return nil
}

func (server *APIServer) verifyApp(h echo.HandlerFunc) echo.HandlerFunc {
	return echo.HandlerFunc(func(c echo.Context) error {
		req := c.Request().(*standard.Request).Request
		var app *apps.App
		var groupIds []int64
		if server.tls {
			if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
				cn := req.TLS.PeerCertificates[0].Subject.CommonName
				if cn != "" {
					var err error
					app, groupIds, err = server.apps.GetAppGroupByName(cn)
					if err != nil {
						glog.Errorf("get app(%s) fail: %v", cn, err)
						return JsonErrorC(c, http.StatusServiceUnavailable,
							utils.Errorf(utils.EcodeSystemError, "get app fail"))
					} else if app == nil {
						glog.V(1).Infof("no such app: %s", cn)
					}
				}
			}
		} else {
			// TODO: http request sign verify
		}
		c.Set("app", app)
		c.Set("groupIds", groupIds)
		return h(c)
	})
}

var ErrNotPermitted = utils.NewError(utils.EcodeNotPermitted, "not permitted")

func (server *APIServer) newPermChecker(permType int, needWrite bool) echo.MiddlewareFunc {
	return echo.MiddlewareFunc(func(h echo.HandlerFunc) echo.HandlerFunc {
		return echo.HandlerFunc(func(c echo.Context) error {
			name := c.P(0)
			app := c.Get("app").(*apps.App)
			if app == nil {
				if needWrite {
					return JsonError(c, ErrNotPermitted)
				}
				if has, err := server.apps.HasAnyPrefixPerm(permType, apps.PermPublicTargetId, nil, needWrite, name); err == nil {
					if !has {
						return JsonError(c, ErrNotPermitted)
					}
				} else {
					return JsonError(c, err)
				}
			} else if !strings.HasPrefix(name, app.Name+".") {
				groupIds := c.Get("groupIds").([]int64)
				if has, err := server.apps.HasAnyPrefixPerm(permType, app.Id, groupIds, needWrite, name); err == nil {
					if !has {
						return JsonError(c, ErrNotPermitted)
					}
				} else {
					return JsonError(c, err)
				}
			}
			return h(c)
		})
	})
}

func (server *APIServer) registerServiceAPIs(g *echo.Group) {
	g.Post("/:name/:version", echo.HandlerFunc(server.PulgService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Delete("/:name/:version/:id", echo.HandlerFunc(server.UnplugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Put("/:name/:version/:id", echo.HandlerFunc(server.UpdateService),
		server.newPermChecker(apps.PermTypeService, true))

	if server.config.PermitPublicServiceQuery {
		g.Get("/:name/:version", echo.HandlerFunc(server.QueryService))
	} else {
		g.Get("/:name/:version", echo.HandlerFunc(server.QueryService),
			server.newPermChecker(apps.PermTypeService, false))
	}
}

func (server *APIServer) registerConfigAPIs(g *echo.Group) {
	// g.Get("", echo.HandlerFunc(server.RangeConfigs))
	g.Get("/:name", echo.HandlerFunc(server.GetConfig),
		server.newPermChecker(apps.PermTypeConfig, false))
	g.Put("/:name", echo.HandlerFunc(server.PutConfig),
		server.newPermChecker(apps.PermTypeConfig, true))
}

func (server *APIServer) registerAppAPIs(g *echo.Group) {
	g.Get("/:name/cert", echo.HandlerFunc(server.GetAppCert))
}
