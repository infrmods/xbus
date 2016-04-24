package api

import (
	"github.com/facebookgo/httpdown"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/services"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/labstack/echo/middleware"
	"time"
)

type Config struct {
	Listen      string        `default:"127.0.0.1:7000"`
	StopTimeout time.Duration `default:"5s" yaml:"stop_timeout"`
	KillTimeout time.Duration `default:"20s" yaml:"kill_timeout"`
}

type APIServer struct {
	config   Config
	services *services.Services
	httpdown.Server
}

func NewAPIServer(config *Config, xbus *services.Services) *APIServer {
	server := &APIServer{config: *config, services: xbus}
	return server
}

func (server *APIServer) Start() error {
	e := echo.New()
	e.Use(middleware.Recover())
	server.registerServiceAPIs(e.Group("/services"))
	std := standard.New(server.config.Listen)
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

func (server *APIServer) registerServiceAPIs(g *echo.Group) {
	g.Post("/:name/:version", echo.HandlerFunc(server.PulgService))
	g.Delete("/:name/:version/:id", echo.HandlerFunc(server.UnplugService))
	g.Put("/:name/:version/:id", echo.HandlerFunc(server.UpdateService))
	g.Get("/:name/:version", echo.HandlerFunc(server.QueryService))
}
