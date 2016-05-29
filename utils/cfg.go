package utils

import (
	"crypto/tls"
	"time"
)

type ETCDConfig struct {
	Endpoints []string      `default:"[\"127.0.0.1:2379\"]"`
	Timeout   time.Duration `default:"5s"`
	TLS       *tls.Config
}
