package api

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/facebookgo/httpdown"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/labstack/echo/middleware"
)

type IPNet net.IPNet

type Config struct {
	Listen      string        `default:"127.0.0.1:4433"`
	CertFile    string        `default:"apicert.pem"`
	KeyFile     string        `default:"apikey.pem"`
	StopTimeout time.Duration `default:"5s" yaml:"stop_timeout"`
	KillTimeout time.Duration `default:"20s" yaml:"kill_timeout"`

	PermitPublicServiceQuery bool `default:"true"`
	DevNets                  []IPNet
}

func (ipnet *IPNet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return fmt.Errorf("empty ipnet")
	}

	if _, result, err := net.ParseCIDR(str); err == nil {
		ipnet.IP = result.IP
		ipnet.Mask = result.Mask
		return nil
	} else {
		return err
	}
}

type APIServer struct {
	config Config
	tls    bool

	etcdClient *clientv3.Client
	services   *services.ServiceCtrl
	configs    *configs.ConfigCtrl
	apps       *apps.AppCtrl

	httpdown.Server
}

func NewAPIServer(config *Config, etcdClient *clientv3.Client,
	servs *services.ServiceCtrl, cfgs *configs.ConfigCtrl, apps *apps.AppCtrl) *APIServer {
	server := &APIServer{config: *config, tls: config.CertFile != "",
		etcdClient: etcdClient,
		services:   servs, configs: cfgs, apps: apps}
	return server
}

func (server *APIServer) Start() error {
	e := echo.New()
	e.Use(middleware.Recover())
	if glog.V(1) {
		e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Format: "method=${method}, uri=${uri}, status=${status}\n",
		}))
	}
	e.Use(echo.MiddlewareFunc(server.verifyApp))
	server.registerV0ServiceAPIs(e.Group("/api/services"))
	server.registerV1ServiceAPIs(e.Group("/api/v1/services"))
	server.registerConfigAPIs(e.Group("/api/configs"))
	server.registerAppAPIs(e.Group("/api/apps"))
	server.registerLeaseAPIs(e.Group("/api/leases"))
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

func (server *APIServer) getRemoteIp(c echo.Context) net.IP {
	req := c.Request().(*standard.Request).Request
	if host, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		return net.ParseIP(host)
	}
	return nil
}

func (server *APIServer) verifyApp(h echo.HandlerFunc) echo.HandlerFunc {
	return echo.HandlerFunc(func(c echo.Context) error {
		var appName string
		req := c.Request().(*standard.Request).Request
		if server.tls {
			if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
				appName = req.TLS.PeerCertificates[0].Subject.CommonName
				c.Set("tlsAppName", appName)
			} else if server.config.DevNets != nil {
				if devApp := req.Header.Get("Dev-App"); devApp != "" {
					if ip := server.getRemoteIp(c); ip != nil {
						for _, ipnet := range server.config.DevNets {
							if (*net.IPNet)(&ipnet).Contains(ip) {
								appName = devApp
								break
							}
						}
					} else {
						glog.Warningf("invalid remote addr: %v", req.RemoteAddr)
					}
				}
			}
		} else {
			// TODO: http request sign verify
		}

		c.Set("app", (*apps.App)(nil))
		c.Set("groupIds", []int64{})
		if appName != "" {
			if app, groupIds, err := server.apps.GetAppGroupByName(appName); err != nil {
				glog.Errorf("get app(%s) fail: %v", appName, err)
				return JsonErrorC(c, http.StatusServiceUnavailable,
					utils.Errorf(utils.EcodeSystemError, "get app fail"))
			} else if app == nil {
				glog.V(1).Infof("no such app: %s", appName)
			} else {
				c.Set("app", app)
				c.Set("groupIds", groupIds)
			}
		}
		return h(c)
	})
}

func (server *APIServer) appId(c echo.Context) int64 {
	if x := c.Get("app").(*apps.App); x != nil {
		return x.Id
	}
	return 0
}

func (server *APIServer) appName(c echo.Context) string {
	if x := c.Get("app").(*apps.App); x != nil {
		return x.Name
	}
	return "null"
}

func (server *APIServer) newNotPermittedResp(c echo.Context, keys ...string) error {
	msg := fmt.Sprintf("not permitted: [%s] %s", server.appName(c), strings.Join(keys, ", "))
	return JsonError(c, utils.NewNotPermittedError(msg, keys))
}

func (server *APIServer) checkPerm(c echo.Context, permType int, needWrite bool, name string) (bool, error) {
	app := c.Get("app").(*apps.App)
	if app == nil {
		if needWrite {
			return false, nil
		}
		if has, err := server.apps.HasAnyPrefixPerm(permType, apps.PermPublicTargetId, nil, needWrite, name); err == nil {
			if !has {
				return false, nil
			}
		} else {
			return false, err
		}
	} else if !strings.HasPrefix(name, app.Name+".") {
		groupIds := c.Get("groupIds").([]int64)
		if has, err := server.apps.HasAnyPrefixPerm(permType, app.Id, groupIds, needWrite, name); err == nil {
			if !has {
				return false, nil
			}
		} else {
			return false, err
		}
	}

	return true, nil
}

func (server *APIServer) newPermChecker(permType int, needWrite bool) echo.MiddlewareFunc {
	return echo.MiddlewareFunc(func(h echo.HandlerFunc) echo.HandlerFunc {
		return echo.HandlerFunc(func(c echo.Context) error {
			if ok, err := server.checkPerm(c, permType, needWrite, c.P(0)); err == nil {
				if !ok {
					return server.newNotPermittedResp(c, c.P(0))
				}
			} else {
				return JsonError(c, err)
			}
			return h(c)
		})
	})
}

func (server *APIServer) registerV0ServiceAPIs(g *echo.Group) {
	g.Post("/:name/:version", echo.HandlerFunc(server.v0PlugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Delete("/:name/:version/:id", echo.HandlerFunc(server.v0UnplugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Post("", echo.HandlerFunc(server.v0PlugAllService))
	g.Put("/:name/:version/:id", echo.HandlerFunc(server.v0UpdateService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Get("", echo.HandlerFunc(server.v0SearchService))

	if server.config.PermitPublicServiceQuery {
		g.Get("/:name/:version", echo.HandlerFunc(server.v0QueryService))
	} else {
		g.Get("/:name/:version", echo.HandlerFunc(server.v0QueryService),
			server.newPermChecker(apps.PermTypeService, false))
	}
}

func (server *APIServer) registerV1ServiceAPIs(g *echo.Group) {
	g.Post("/:service", echo.HandlerFunc(server.v1PlugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Delete("/:service/:zone/:addr", echo.HandlerFunc(server.v1UnplugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.Post("", echo.HandlerFunc(server.v1PlugAllService))
	g.Get("", echo.HandlerFunc(server.v1SearchService))

	if server.config.PermitPublicServiceQuery {
		g.Get("/:service", echo.HandlerFunc(server.v1QueryService))
	} else {
		g.Get("/:service", echo.HandlerFunc(server.v1QueryService),
			server.newPermChecker(apps.PermTypeService, false))
	}
}

func (server *APIServer) registerLeaseAPIs(g *echo.Group) {
	g.Post("", echo.HandlerFunc(server.GrantLease))
	g.Post("/:id", echo.HandlerFunc(server.KeepAliveLease))
	g.Delete("/:id", echo.HandlerFunc(server.RevokeLease))
}

func (server *APIServer) registerConfigAPIs(g *echo.Group) {
	g.Get("/:name", echo.HandlerFunc(server.GetConfig),
		server.newPermChecker(apps.PermTypeConfig, false))
	g.Get("", echo.HandlerFunc(server.ListConfig))
	g.Put("/:name", echo.HandlerFunc(server.PutConfig),
		server.newPermChecker(apps.PermTypeConfig, true))
	g.Delete("/:name", echo.HandlerFunc(server.DeleteConfig),
		server.newPermChecker(apps.PermTypeConfig, true))
}

func (server *APIServer) registerAppAPIs(g *echo.Group) {
	g.Get("/:name/cert", echo.HandlerFunc(server.GetAppCert))
	g.Get("", echo.HandlerFunc(server.ListApp))
	g.Put("", echo.HandlerFunc(server.NewApp))
}
