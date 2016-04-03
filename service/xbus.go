package service

type Config struct {
	EtcdEndpoints []string `default:"[\"127.0.0.1:2378\"]"`
}

type XBus struct {
	config Config
}

func NewXBus(config *Config) *XBus {
	xbus := &XBus{config: *config}
	return xbus
}
