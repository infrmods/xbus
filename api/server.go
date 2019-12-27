package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/apps"
	"github.com/infrmods/xbus/configs"
	"github.com/infrmods/xbus/services"
	"github.com/infrmods/xbus/utils"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// IPNet ipnet
type IPNet net.IPNet

// Config api server config
type Config struct {
	Listen      string        `default:"127.0.0.1:4433"`
	CertFile    string        `default:"apicert.pem"`
	KeyFile     string        `default:"apikey.pem"`
	StopTimeout time.Duration `default:"60s" yaml:"stop_timeout"`

	PermitPublicServiceQuery bool `default:"true"`
	DevNets                  []IPNet
}

// UnmarshalYAML unmarshal yaml
func (ipnet *IPNet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return fmt.Errorf("empty ipnet")
	}

	_, result, err := net.ParseCIDR(str)
	if err != nil {
		return err
	}
	ipnet.IP = result.IP
	ipnet.Mask = result.Mask
	return nil
}

// Server api server
type Server struct {
	config Config
	tls    bool

	etcdClient *clientv3.Client
	services   *services.ServiceCtrl
	configs    *configs.ConfigCtrl
	apps       *apps.AppCtrl

	e *echo.Echo
}

// NewServer new api server
func NewServer(config *Config, etcdClient *clientv3.Client,
	servs *services.ServiceCtrl, cfgs *configs.ConfigCtrl, apps *apps.AppCtrl) *Server {
	server := &Server{config: *config, tls: config.CertFile != "",
		etcdClient: etcdClient,
		services:   servs, configs: cfgs, apps: apps, e: echo.New()}
	server.prepare()
	return server
}

func (server *Server) prepare() {
	server.e.Use(middleware.Recover())
	if glog.V(1) {
		server.e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Format: "[${time_rfc3339}] ${remote_ip} ${user_agent} ${protocol} ${method} ${uri}: ${status} ${latency_human}\n",
		}))
	}
	server.e.GET("/api/ok", func(c echo.Context) error {
		return c.JSON(200, map[string]bool{"ok": true})
	})
	server.e.Use(echo.MiddlewareFunc(server.verifyApp))
	server.registerV1ServiceAPIs(server.e.Group("/api/v1/services"))
	server.e.GET("/api/v1/service-extensions/:extension", server.v1WatchExt)
	server.registerConfigAPIs(server.e.Group("/api/configs"))
	server.registerAppAPIs(server.e.Group("/api/apps"))
	server.registerLeaseAPIs(server.e.Group("/api/leases"))
}

// Run run server
func (server *Server) Run() error {
	useTLS := server.config.CertFile != ""
	addr := server.config.Listen
	if !strings.Contains(addr, ":") {
		if useTLS {
			addr += ":https"
		} else {
			addr += ":http"
		}
	}
	go func() {
		if err := server.start(); err == http.ErrServerClosed {
			glog.Info("shutting down the server")
		} else if err != nil {
			glog.Fatal(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), server.config.StopTimeout)
	defer cancel()
	return server.e.Shutdown(ctx)
}

func (server *Server) start() (err error) {
	useTLS := server.config.CertFile != ""
	var s *http.Server
	if useTLS {
		s = server.e.TLSServer
		s.TLSConfig = new(tls.Config)
		s.TLSConfig.ClientCAs = server.apps.GetAppCertPool()
		s.TLSConfig.Certificates = make([]tls.Certificate, 1)
		s.TLSConfig.ClientAuth = tls.VerifyClientCertIfGiven
		s.TLSConfig.Certificates[0], err = tls.LoadX509KeyPair(server.config.CertFile, server.config.KeyFile)
		if err != nil {
			return
		}
		if !server.e.DisableHTTP2 {
			s.TLSConfig.NextProtos = append(s.TLSConfig.NextProtos, "h2")
		}
	} else {
		s = server.e.Server
	}
	s.Addr = server.config.Listen
	if !strings.Contains(s.Addr, ":") {
		if useTLS {
			s.Addr += ":https"
		} else {
			s.Addr += ":http"
		}
	}
	return server.e.StartServer(s)
}

func (server *Server) getRemoteIP(c echo.Context) net.IP {
	if host, _, err := net.SplitHostPort(c.Request().RemoteAddr); err == nil {
		return net.ParseIP(host)
	}
	return nil
}

func (server *Server) verifyApp(h echo.HandlerFunc) echo.HandlerFunc {
	return echo.HandlerFunc(func(c echo.Context) error {
		var appName string
		req := c.Request()
		if server.tls {
			if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
				appName = req.TLS.PeerCertificates[0].Subject.CommonName
				c.Set("tlsAppName", appName)
			} else if server.config.DevNets != nil {
				if devApp := req.Header.Get("Dev-App"); devApp != "" {
					if ip := server.getRemoteIP(c); ip != nil {
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
				return JSONErrorC(c, http.StatusServiceUnavailable,
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

func (server *Server) appID(c echo.Context) int64 {
	if x := c.Get("app").(*apps.App); x != nil {
		return x.ID
	}
	return 0
}

func (server *Server) appName(c echo.Context) string {
	if x := c.Get("app").(*apps.App); x != nil {
		return x.Name
	}
	return "null"
}

func (server *Server) app(c echo.Context) *apps.App {
	return c.Get("app").(*apps.App)
}

func (server *Server) newNotPermittedResp(c echo.Context, keys ...string) error {
	msg := fmt.Sprintf("not permitted: [%s] %s", server.appName(c), strings.Join(keys, ", "))
	return JSONError(c, utils.NewNotPermittedError(msg, keys))
}

func (server *Server) checkPerm(c echo.Context, permType int, needWrite bool, name string) (bool, error) {
	app := c.Get("app").(*apps.App)
	if app == nil {
		if needWrite {
			return false, nil
		}
		if has, err := server.apps.HasAnyPrefixPerm(permType, apps.PermPublicTargetID, nil, needWrite, name); err == nil {
			if !has {
				return false, nil
			}
		} else {
			return false, err
		}
	} else if !strings.HasPrefix(name, app.Name+".") {
		groupIds := c.Get("groupIds").([]int64)
		if has, err := server.apps.HasAnyPrefixPerm(permType, app.ID, groupIds, needWrite, name); err == nil {
			if !has {
				return false, nil
			}
		} else {
			return false, err
		}
	}

	return true, nil
}

func (server *Server) newPermChecker(permType int, needWrite bool) echo.MiddlewareFunc {
	return echo.MiddlewareFunc(func(h echo.HandlerFunc) echo.HandlerFunc {
		return echo.HandlerFunc(func(c echo.Context) error {
			if ok, err := server.checkPerm(c, permType, needWrite, c.ParamValues()[0]); err == nil {
				if !ok {
					return server.newNotPermittedResp(c, c.ParamValues()[0])
				}
			} else {
				return JSONError(c, err)
			}
			return h(c)
		})
	})
}

func (server *Server) registerV1ServiceAPIs(g *echo.Group) {
	g.POST("/:service", echo.HandlerFunc(server.v1PlugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.DELETE("/:service", echo.HandlerFunc(server.v1DeleteService),
		server.newPermChecker(apps.PermTypeService, true))
	g.DELETE("/:service/:zone/:addr", echo.HandlerFunc(server.v1UnplugService),
		server.newPermChecker(apps.PermTypeService, true))
	g.POST("", echo.HandlerFunc(server.v1PlugAllService))
	g.GET("", echo.HandlerFunc(server.v1SearchService))

	if server.config.PermitPublicServiceQuery {
		g.GET("/:service", echo.HandlerFunc(server.v1QueryService))
	} else {
		g.GET("/:service", echo.HandlerFunc(server.v1QueryService),
			server.newPermChecker(apps.PermTypeService, false))
	}
}

func (server *Server) registerLeaseAPIs(g *echo.Group) {
	g.POST("", echo.HandlerFunc(server.grantLease))
	g.POST("/:id", echo.HandlerFunc(server.keepAliveLease))
	g.DELETE("/:id", echo.HandlerFunc(server.revokeLease))
}

func (server *Server) registerConfigAPIs(g *echo.Group) {
	g.GET("/:name", echo.HandlerFunc(server.getConfig),
		server.newPermChecker(apps.PermTypeConfig, false))
	g.GET("", echo.HandlerFunc(server.listConfig))
	g.PUT("/:name", echo.HandlerFunc(server.putConfig),
		server.newPermChecker(apps.PermTypeConfig, true))
	g.DELETE("/:name", echo.HandlerFunc(server.deleteConfig),
		server.newPermChecker(apps.PermTypeConfig, true))
}

func (server *Server) registerAppAPIs(g *echo.Group) {
	g.GET("/:name/cert", echo.HandlerFunc(server.getAppCert))
	g.GET("/:name/nodes", echo.HandlerFunc(server.watchAppNodes))
	g.GET("/:name/online", echo.HandlerFunc(server.isAppNodeOnline))
	g.GET("", echo.HandlerFunc(server.listApp))
	g.PUT("", echo.HandlerFunc(server.newApp))
}
