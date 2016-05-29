package apps

import (
	"database/sql"
	"fmt"
)

type Config struct {
	DB struct {
		Driver string
		Source string
	}
}

type AppCtrl struct {
	config *Config
	db     *sql.DB
}

func NewAppCtrl(config *Config) (*AppCtrl, error) {
	db, err := sql.Open(config.DB.Driver, config.DB.Source)
	if err != nil {
		return nil, fmt.Errorf("open db fail: %v", err)
	}
	return &AppCtrl{config: config, db: db}, nil
}
